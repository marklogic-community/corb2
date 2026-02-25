/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.COMMAND_FILE;
import java.text.MessageFormat;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;

/**
 * Monitors task completion and progress for CoRB jobs.
 * <p>
 * The Monitor runs in a dedicated thread and is responsible for:
 * </p>
 * <ul>
 * <li>Tracking completion of tasks submitted to the thread pool</li>
 * <li>Calculating and displaying progress metrics (TPS, ETC, completion count)</li>
 * <li>Detecting and logging low memory conditions</li>
 * <li>Notifying when jobs are paused</li>
 * <li>Coordinating shutdown when all tasks complete</li>
 * </ul>
 * <p>
 * Progress updates are logged at regular intervals (defined by
 * {@link TransformOptions#PROGRESS_INTERVAL_MS}). The Monitor uses a
 * {@link CompletionService} to poll for completed tasks without blocking.
 * </p>
 * <p>
 * The Monitor continues running until either:
 * </p>
 * <ul>
 * <li>All tasks have completed successfully</li>
 * <li>A fatal error occurs (propagated to the Manager)</li>
 * <li>The Monitor is interrupted or shutdown</li>
 * </ul>
 *
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see BaseMonitor
 * @see Manager
 * @see PausableThreadPoolExecutor
 */
public class Monitor extends BaseMonitor implements Runnable {

    /**
     * Logger instance for the Monitor class.
     * Used to log progress updates, warnings, errors, and completion status during task monitoring.
     */
    protected static final Logger LOG = Logger.getLogger(Monitor.class.getName());

    /**
     * Flag indicating whether the monitor should shutdown immediately.
     * When set to {@code true}, the monitoring loop exits on the next iteration.
     * Set via {@link #shutdownNow()} to terminate monitoring before all tasks complete.
     */
    protected boolean shutdownNow;

    /**
     * The number of tasks that have completed successfully.
     * Incremented each time a task is retrieved from the completion service.
     * Used to track progress and determine when all tasks have finished.
     */
    protected long completed = 0L;

    /**
     * The pausable thread pool executor being monitored.
     * Provides access to thread pool statistics such as active thread count,
     * completed task count, and pause/resume state. Also tracks failed URI count.
     */
    protected PausableThreadPoolExecutor threadPoolExecutor;

    /**
     * The completion service for retrieving completed tasks.
     * Polled at regular intervals to detect task completion without blocking.
     * Returns {@link Future} objects representing completed tasks.
     */
    protected final CompletionService<String[]> cs;
    /**
     * Constructs a Monitor for the specified thread pool and completion service.
     *
     * @param threadPoolExecutor the thread pool executor to monitor
     * @param cs the completion service for retrieving completed tasks
     * @param manager the Manager coordinating the job
     */
    public Monitor(PausableThreadPoolExecutor threadPoolExecutor, CompletionService<String[]> cs, Manager manager) {
        super(manager);
        this.threadPoolExecutor = threadPoolExecutor;
        this.cs = cs;
    }

    /**
     * Main monitoring loop that tracks task completion and displays progress.
     * <p>
     * This method runs in a dedicated thread and continuously polls the completion
     * service for finished tasks. It updates progress metrics, logs status messages,
     * and detects completion or error conditions.
     * </p>
     * <p>
     * If a fatal execution error occurs, it notifies the Manager to stop the job.
     * The loop terminates when all tasks complete or the monitor is shutdown.
     * </p>
     */
    @Override
    public void run() {
        startMillis = System.currentTimeMillis();

        try {
            Thread.yield();
            monitorResults();
        } catch (ExecutionException e) {
            // tell the main thread to quit
            manager.stop(e);
        } catch (InterruptedException e) {
            // reset interrupt status and exit
            Thread.interrupted();
            LOG.log(SEVERE, "interrupted: exiting", e);
        }
    }

    /**
     * Monitors task results by polling the completion service.
     * <p>
     * This method:
     * <ol>
     * <li>Polls the completion service for completed tasks</li>
     * <li>Updates the completion count</li>
     * <li>Displays progress at regular intervals</li>
     * <li>Detects when all tasks have completed</li>
     * <li>Waits for thread pool termination</li>
     * </ol>
     * </p>
     * <p>
     * The method includes special handling for edge cases where the completion
     * count and thread pool statistics may temporarily diverge.
     * </p>
     *
     * @throws InterruptedException if the monitor thread is interrupted
     * @throws ExecutionException if a task execution fails
     */
    private void monitorResults() throws InterruptedException, ExecutionException {
        // fast-fail as soon as we see any exceptions
        LOG.log(INFO, () -> MessageFormat.format("monitoring {0} tasks", taskCount));
        Future<String[]> future;
        while (!shutdownNow) {
            // try to avoid thread starvation
            Thread.yield();

            future = cs.poll(TransformOptions.PROGRESS_INTERVAL_MS, TimeUnit.MILLISECONDS);
            if (null != future) {
                // record result, or throw exception
                String[] lastUris = future.get();
                completed += lastUris.length;
            }

            showProgress();

            if (completed >= taskCount) {
                try {
                    Thread.sleep(100); //sleep a little for the pool to align
                } catch(InterruptedException ex) {
                    LOG.log(WARNING, "Interrupted!", ex);
                    Thread.currentThread().interrupt();
                }
                if (threadPoolExecutor.getActiveCount() > 0 || (threadPoolExecutor.getTaskCount() - threadPoolExecutor.getCompletedTaskCount()) > 0) {
                    LOG.log(WARNING, "Thread pool is still active with all the tasks completed and received. We shouldn't see this message.");
                    //wait for the ThreadPool numbers to align
                } else {
                    //everyone agrees; all tasks are completed and the threadPool reports all tasks are complete.
                    break;
                }
            } else if (future == null && threadPoolExecutor.getActiveCount() == 0) {
                LOG.log(WARNING, () -> MessageFormat.format("No active tasks found with {0,number} tasks remains to be completed", taskCount - completed));
            }
        }
        LOG.info("waiting for pool to terminate");
        threadPoolExecutor.awaitTermination(1, TimeUnit.SECONDS);
        LOG.log(INFO, () -> MessageFormat.format("completed all tasks {0,number}/{1,number}", completed, taskCount));
    }

    /**
     * Displays progress information at regular intervals.
     * <p>
     * Progress updates include:
     * <ul>
     * <li>Number of completed tasks</li>
     * <li>Current and average TPS (transactions per second)</li>
     * <li>Estimated time of completion</li>
     * <li>Active thread count</li>
     * <li>Failed task count</li>
     * <li>Pause status</li>
     * <li>Low memory warnings</li>
     * </ul>
     * </p>
     * <p>
     * Progress is only displayed if the configured interval has elapsed since
     * the last update.
     * </p>
     *
     * @return the timestamp of the last progress update
     */
    private long showProgress() {
        long current = System.currentTimeMillis();
        if (current - lastProgress > TransformOptions.PROGRESS_INTERVAL_MS) {
            if (threadPoolExecutor.isPaused()) {
                LOG.log(INFO, "CoRB2 has been paused. Resume execution by changing the " + Options.COMMAND + " option in the command file {0} to RESUME", manager.getOption(COMMAND_FILE));
            }
            LOG.log(INFO, () -> MessageFormat.format("completed {0}", getProgressMessage(completed)));
            lastProgress = current;

            // check for low memory
            long freeMemory = Runtime.getRuntime().freeMemory();
            if (freeMemory < (16 * 1024 * 1024)) {
                LOG.log(WARNING, () -> MessageFormat.format("free memory: {0} MiB", freeMemory / 1024 * 1024));
            }
        }
        return lastProgress;
    }

    /**
     * Generates a formatted progress message with current job statistics.
     *
     * @param completed the number of completed tasks
     * @return formatted progress message string
     */
    protected String getProgressMessage(long completed) {
        populateTps(completed);
        return getProgressMessage(completed, taskCount, avgTps, currentTps, estimatedTimeOfCompletion, threadPoolExecutor.getActiveCount(), threadPoolExecutor.getNumFailedUris());
    }

    /**
     * Sets the total number of tasks to monitor.
     *
     * @param count the total task count
     */
    public void setTaskCount(long count) {
        taskCount = count;
    }

    /**
     * Gets the total number of tasks to monitor.
     *
     * @return the total task count
     */
    public long getTaskCount() {
        return taskCount;
    }

    /**
     * Gets the number of tasks that have completed.
     *
     * @return the completed task count
     */
    public long getCompletedCount() {
        return this.completed;
    }

    /**
     * Gets the thread pool executor being monitored.
     *
     * @return the thread pool executor
     */
    public PausableThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

    /**
     * Signals the monitor to shutdown immediately.
     * <p>
     * This causes the monitoring loop to exit on the next iteration.
     * </p>
     */
    public void shutdownNow() {
        shutdownNow = true;
    }

}
