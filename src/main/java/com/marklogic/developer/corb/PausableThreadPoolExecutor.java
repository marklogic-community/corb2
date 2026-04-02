/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
  * *
  * * Licensed under the Apache License, Version 2.0 (the "License");
  * * you may not use this file except in compliance with the License.
  * * You may obtain a copy of the License at
  * *
  * * http://www.apache.org/licenses/LICENSE-2.0
  * *
  * * Unless required by applicable law or agreed to in writing, software
  * * distributed under the License is distributed on an "AS IS" BASIS,
  * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * * See the License for the specific language governing permissions and
  * * limitations under the License.
  * *
  * * The use of the Apache License does not indicate that this project is
  * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import org.jetbrains.annotations.NotNull;

import static com.marklogic.developer.corb.TransformOptions.FAILED_URI_TOKEN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread pool executor with pause/resume capabilities and URI execution tracking.
 * <p>
 * PausableThreadPoolExecutor extends {@link ThreadPoolExecutor} to provide:
 * </p>
 * <ul>
 * <li>Ability to pause and resume execution of tasks</li>
 * <li>Tracking of execution time for individual URIs</li>
 * <li>Identification of the longest-running URIs</li>
 * <li>Counts of successful and failed task executions</li>
 * <li>Collection of failed URI samples for error reporting</li>
 * </ul>
 * <p>
 * When paused, running tasks continue to completion, but new tasks are not started
 * until the executor is resumed. This allows for graceful throttling of CoRB jobs
 * without interrupting in-flight operations.
 * </p>
 * <p>
 * The executor maintains a configurable list of the longest-running URIs, which is
 * useful for identifying performance bottlenecks. It also tracks failed URIs up to
 * a configurable limit for error reporting and analysis.
 * </p>
 * <p>
 * Thread safety: This class is thread-safe. Pause/resume operations use a
 * {@link ReentrantLock} to ensure proper synchronization. URI statistics collection
 * is synchronized to prevent race conditions.
 * </p>
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @see ThreadPoolExecutor
 * @see Manager
 * @see Monitor
 */
public class PausableThreadPoolExecutor extends ThreadPoolExecutor {

    /**
     * Lock object used for synchronizing access to shared statistics (counters and failed URIs list).
     */
    private final Object lock = new Object();

    /**
     * Logger for this class.
     * Used to log task execution details and any errors during statistics collection.
     */
    private static final Logger LOG = Logger.getLogger(PausableThreadPoolExecutor.class.getName());

    /**
     * Flag indicating whether the executor is currently paused.
     * When true, new tasks will not begin execution until {@link #resume()} is called.
     */
    private boolean isPaused;

    /**
     * Lock used to coordinate pause/resume operations.
     * Protects the {@link #isPaused} flag and controls thread waiting.
     */
    private final ReentrantLock pauseLock = new ReentrantLock();

    /**
     * Condition variable for signaling threads waiting due to pause.
     * Associated with {@link #pauseLock}.
     */
    private final Condition unpaused = pauseLock.newCondition();

    /**
     * Maintains the list of longest-running URIs.
     * Tracks the top N URIs by execution time for performance analysis.
     */
    protected TopUriList topUriList;

    /**
     * List of failed URIs captured for error reporting.
     * Size is limited by {@link #numFailedUrisToCapture}.
     */
    private List<String> failedUris;

    /**
     * ThreadLocal storing the start time (in nanoseconds) for each task execution.
     * Used to calculate task duration in {@link #afterExecute(Runnable, Throwable)}.
     */
    private final ThreadLocal<Long> startTime = new ThreadLocal<>();

    /**
     * ThreadLocal storing the original thread name before task execution.
     * Restored after task completion in {@link #afterExecute(Runnable, Throwable)}.
     */
    private final ThreadLocal<String> threadName = new ThreadLocal<>();

    /**
     * Maximum number of failed URIs to capture for reporting.
     * Configured via {@link TransformOptions#getNumberOfFailedUris()}.
     */
    private int numFailedUrisToCapture = 0;

    /**
     * Total count of failed task executions.
     * Incremented in {@link #afterExecute(Runnable, Throwable)} when a task fails.
     */
    private long numFailedUris = 0;

    /**
     * Total count of successful task executions.
     * Incremented in {@link #afterExecute(Runnable, Throwable)} when a task succeeds.
     */
    private long numSucceededUris = 0;

    /**
     * Constructs a PausableThreadPoolExecutor with default transform options.
     *
     * @param corePoolSize the number of threads to keep in the pool
     * @param maximumPoolSize the maximum number of threads allowed in the pool
     * @param keepAliveTime time that excess idle threads wait for new tasks
     * @param unit the time unit for the keepAliveTime argument
     * @param workQueue the queue to use for holding tasks before execution
     * @param handler the handler to use when execution is blocked
     */
    public PausableThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler, defaultTransformOptions());
    }

    /**
     * Creates default transform options with reasonable defaults.
     *
     * @return TransformOptions with default settings
     */
    private static TransformOptions defaultTransformOptions() {
        TransformOptions options = new TransformOptions();
        options.setNumberOfLongRunningUris(5);
        return options;
    }

    /**
     * Constructs a PausableThreadPoolExecutor with specified transform options.
     *
     * @param corePoolSize the number of threads to keep in the pool
     * @param maximumPoolSize the maximum number of threads allowed in the pool
     * @param keepAliveTime time that excess idle threads wait for new tasks
     * @param unit the time unit for the keepAliveTime argument
     * @param workQueue the queue to use for holding tasks before execution
     * @param handler the handler to use when execution is blocked
     * @param options transform options for configuring URI tracking limits
     */
    public PausableThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler,
            TransformOptions options) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, Executors.defaultThreadFactory(), handler);
        topUriList = new TopUriList(options.getNumberOfLongRunningUris());
        failedUris = new ArrayList<>();
        numFailedUrisToCapture = options.getNumberOfFailedUris();
    }

    /**
     * Invoked before executing a task.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Saves the current thread name</li>
     * <li>Waits if the executor is paused</li>
     * <li>Records the task start time</li>
     * </ol>
     * <p>
     * If the executor is paused, the thread will block until {@link #resume()}
     * is called or the thread is interrupted.
     * </p>
     *
     * @param t the thread that will run task r
     * @param r the task that will be executed
     */
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        threadName.set(Thread.currentThread().getName());
        pauseLock.lock();
        try {
            while (isPaused && !t.isInterrupted()) {
                unpaused.await();
            }
        } catch (InterruptedException ie) {
            t.interrupt();
        } finally {
            pauseLock.unlock();
            startTime.set(System.nanoTime());
        }
    }

    /**
     * Invoked after a task completes execution.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Extracts the execution result from the thread name</li>
     * <li>Determines if the task failed or succeeded</li>
     * <li>Updates failure/success counters</li>
     * <li>Records failed URIs (up to the configured limit)</li>
     * <li>Calculates execution time and updates the top URIs list</li>
     * </ol>
     * <p>
     * Failed tasks are identified by a result string starting with {@link TransformOptions#FAILED_URI_TOKEN}.
     * The failed URI is extracted and added to the failed URIs list if space is available.
     * </p>
     *
     * @param r the task that has completed
     * @param t the exception that caused termination, or null if execution completed normally
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        try {
            String result = Thread.currentThread().getName();
            Thread.currentThread().setName(threadName.get());
            if (result != null) {
                boolean failed = result.toUpperCase().startsWith(FAILED_URI_TOKEN);
                if (failed) {
                    String[] tokens = result.split(FAILED_URI_TOKEN);
                    synchronized (lock) {
                        if (tokens.length > 1 && !tokens[1].isEmpty() && failedUris.size() < numFailedUrisToCapture) {
                            failedUris.add(tokens[1]);
                        }
                        numFailedUris++;
                    }
                } else {
                    synchronized (lock) {
                        numSucceededUris++;
                    }
                    long endTime = System.nanoTime();
                    long taskTime = endTime - startTime.get();
                    long durationInMs = TimeUnit.MILLISECONDS.convert(taskTime, TimeUnit.NANOSECONDS);
                    topUriList.add(result, durationInMs);
                }
            }
        } catch (Exception e) {
            //Ignore
            LOG.log(Level.FINE, "Encountered an issue collecting result status", e);
        }
        LOG.log(Level.FINE, String.format("succeeded: %s failed: %s ",numSucceededUris, numFailedUris));
    }

    /**
     * Returns a map of the longest-running URIs and their execution times.
     *
     * @return map where keys are URIs and values are execution times in milliseconds
     */
    public Map<String, Long> getTopUris() {
        return topUriList.getData();
    }

    /**
     * Checks if the executor is currently running (not paused).
     *
     * @return true if running, false if paused
     */
    public boolean isRunning() {
        return !isPaused;
    }

    /**
     * Checks if the executor is currently paused.
     *
     * @return true if paused, false if running
     */
    public boolean isPaused() {
        return isPaused;
    }

    /**
     * Pauses task execution.
     * <p>
     * Currently executing tasks will continue to completion, but no new tasks
     * will begin execution until {@link #resume()} is called.
     * </p>
     */
    public void pause() {
        pauseLock.lock();
        try {
            isPaused = true;
        } finally {
            pauseLock.unlock();
        }
    }

    /**
     * Resumes task execution after being paused.
     * <p>
     * All threads waiting to execute tasks will be signaled to continue.
     * </p>
     */
    public void resume() {
        pauseLock.lock();
        try {
            isPaused = false;
            unpaused.signalAll();
        } finally {
            pauseLock.unlock();
        }
    }

    /**
     * Gets the list of failed URIs that have been captured.
     * <p>
     * The size of this list is limited by the configured maximum (see
     * {@link TransformOptions#getNumberOfFailedUris()}).
     * </p>
     *
     * @return list of failed URIs
     */
    public List<String> getFailedUris() {
        return failedUris;
    }

    /**
     * Gets the total count of failed task executions.
     *
     * @return the total number of failed URIs
     */
    public long getNumFailedUris() {
        return numFailedUris;
    }

    /**
     * Gets the total count of successful task executions.
     *
     * @return the total number of succeeded URIs
     */
    public long getNumSucceededUris() {
        return numSucceededUris;
    }

    /**
     * Maintains a sorted list of the longest-running URIs.
     * <p>
     * This class uses a {@link TreeSet} to maintain URIs sorted by execution time.
     * The list is bounded to a maximum size, keeping only the top N longest-running URIs.
     * </p>
     * <p>
     * Thread safety: The {@link #add(String, Long)} method is synchronized to ensure
     * thread-safe updates to the list.
     * </p>
     */
    protected class TopUriList {

        /**
         * TreeSet maintaining URIs sorted by execution time (ascending).
         */
        private TreeSet<UriObject> list;
        /**
         * Maximum number of URIs to maintain in the list.
         */
        private int size = 0;

        /**
         * Constructs a TopUriList with the specified maximum size.
         *
         * @param size maximum number of URIs to track
         */
        public TopUriList(int size) {
            this.size = size;
            list = new TreeSet<UriObject>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String toString() {
                    StringBuilder strBuff = new StringBuilder();
                    for (UriObject o : this) {
                        strBuff.append(o.toString());
                    }
                    return strBuff.toString();
                }
            };
        }

        /**
         * Returns the URI data as a map.
         *
         * @return map where keys are URIs and values are execution times in milliseconds
         */
        Map<String, Long> getData() {
            Map<String, Long> map = new HashMap<>();
            synchronized (lock) {
                for (UriObject obj : list) {
                    map.put(obj.uri, obj.timeTaken);
                }
            }
            return map;
        }

        /**
         * Adds a URI and its execution time to the list.
         * <p>
         * The URI is only added if:
         * </p>
         * <ul>
         * <li>The list is not yet full, OR</li>
         * <li>The execution time is longer than the shortest time currently in the list</li>
         * </ul>
         * <p>
         * If the list is full, the shortest-running URI(s) will be removed to make room.
         * This method is synchronized to ensure thread safety.
         * </p>
         *
         * @param uri the URI that was executed
         * @param timeTaken the execution time in milliseconds
         */
        void add(String uri, Long timeTaken) {
            UriObject newObj = new UriObject(uri, timeTaken);
            if (list.size() < this.size || (!list.isEmpty() && list.last().compareTo(newObj) < 1)) {
                synchronized (lock) {
                    if (list.size() >= this.size) {
                        for (int i = 0; i <= list.size() - this.size; i++) {
                            if (!list.isEmpty()) {
                                list.remove(list.first());
                            }
                        }
                    }
                    list.add(newObj);
                }
            }
        }

        /**
         * Gets the maximum size of the list.
         *
         * @return the maximum number of URIs tracked
         */
        protected int getSize() {
            return size;
        }

        /**
         * Sets the maximum size of the list.
         *
         * @param size the maximum number of URIs to track
         */
        protected void setSize(int size) {
            this.size = size;
        }

        /**
         * Represents a URI and its execution time.
         * <p>
         * Instances are compared based solely on execution time, allowing
         * the TreeSet to maintain URIs sorted by duration.
         * </p>
         */
        private class UriObject implements Comparable<UriObject> {

            /**
             * The URI that was executed.
             */
            String uri;
            /**
             * Execution time in milliseconds.
             */
            Long timeTaken;

            /**
             * Constructs a UriObject with the specified URI and execution time.
             *
             * @param uri the URI
             * @param timeTaken the execution time in milliseconds
             */
            public UriObject(String uri, Long timeTaken) {
                super();
                this.uri = uri;
                this.timeTaken = timeTaken;
            }

            /**
             * Returns a string representation of this UriObject.
             *
             * @return string containing URI and execution time
             */
            @Override
            public String toString() {
                return "UriObject [uri=" + uri + ", timeTaken=" + timeTaken + "]";
            }

            /**
             * Checks equality based on execution time only.
             * <p>
             * Two UriObjects are equal if they have the same execution time,
             * regardless of the URI value.
             * </p>
             *
             * @param obj the object to compare with
             * @return true if execution times are equal, false otherwise
             */
            @Override
            public boolean equals(Object obj) {
                if (obj instanceof UriObject) {
                    UriObject o = (UriObject) obj;
                    if (this.timeTaken != null && o.timeTaken != null) {
                        return this.timeTaken.compareTo(o.timeTaken) == 0;
                    } else {
                        return false;
                    }
                } else {
                    return super.equals(obj);
                }
            }

            /**
             * Generates a hash code based on URI and execution time.
             *
             * @return the hash code
             */
            @Override
            public int hashCode() {
                int hash = 5;
                hash = 53 * hash + Objects.hashCode(this.uri);
                hash = 53 * hash + Objects.hashCode(this.timeTaken);
                return hash;
            }

            /**
             * Compares this UriObject to another based on execution time.
             *
             * @param o the UriObject to compare with
             * @return negative if this time is less, zero if equal, positive if greater
             */
            @Override
            public int compareTo(@NotNull UriObject o) {
                if (this.timeTaken != null && o.timeTaken != null) {
                    return this.timeTaken.compareTo(o.timeTaken);
                } else {
                    return 1;//should never get here
                }
            }
        }
    }
}
