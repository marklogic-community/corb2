/*
  * * Copyright (c) 2004-2022 MarkLogic Corporation
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
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class PausableThreadPoolExecutor extends ThreadPoolExecutor {

    private final Object lock = new Object();
    private static final Logger LOG = Logger.getLogger(PausableThreadPoolExecutor.class.getName());
    private boolean isPaused;
    private final ReentrantLock pauseLock = new ReentrantLock();
    private final Condition unpaused = pauseLock.newCondition();
    protected TopUriList topUriList;
    private List<String> failedUris;

    private final ThreadLocal<Long> startTime = new ThreadLocal<>();
    private final ThreadLocal<String> threadName = new ThreadLocal<>();
    private int numFailedUrisToCapture = 0;
    private long numFailedUris = 0;
    private long numSucceededUris = 0;

    public PausableThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler, defaultTransformOptions());
    }

    private static TransformOptions defaultTransformOptions() {
        TransformOptions options = new TransformOptions();
        options.setNumberOfLongRunningUris(5);
        return options;
    }

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

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        threadName.set(Thread.currentThread().getName());
        pauseLock.lock();
        try {
            while (isPaused) {
                unpaused.await();
            }
        } catch (InterruptedException ie) {
            t.interrupt();
        } finally {
            pauseLock.unlock();
            startTime.set(System.nanoTime());
        }
    }

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

    public Map<String, Long> getTopUris() {
        return topUriList.getData();
    }

    public boolean isRunning() {
        return !isPaused;
    }

    public boolean isPaused() {
        return isPaused;
    }

    public void pause() {
        pauseLock.lock();
        try {
            isPaused = true;
        } finally {
            pauseLock.unlock();
        }
    }

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
     * @return the failedUris
     */
    public List<String> getFailedUris() {
        return failedUris;
    }

    /**
     * @return the numFailedUris
     */
    public long getNumFailedUris() {
        return numFailedUris;
    }

    /**
     * @return the numSucceededUris
     */
    public long getNumSucceededUris() {
        return numSucceededUris;
    }

    protected class TopUriList {

        private TreeSet<UriObject> list;
        private int size = 0;

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

        Map<String, Long> getData() {
            Map<String, Long> map = new HashMap<>();
            for (UriObject obj : list) {
                map.put(obj.uri, obj.timeTaken);
            }
            return map;
        }

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
         * @return the size
         */
        protected int getSize() {
            return size;
        }

        /**
         * @param size the size to set
         */
        protected void setSize(int size) {
            this.size = size;
        }

        private class UriObject implements Comparable<UriObject> {

            String uri;
            Long timeTaken;

            public UriObject(String uri, Long timeTaken) {
                super();
                this.uri = uri;
                this.timeTaken = timeTaken;
            }

            @Override
            public String toString() {
                return "UriObject [uri=" + uri + ", timeTaken=" + timeTaken + "]";
            }

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

            @Override
            public int hashCode() {
                int hash = 5;
                hash = 53 * hash + Objects.hashCode(this.uri);
                hash = 53 * hash + Objects.hashCode(this.timeTaken);
                return hash;
            }

            @Override
            public int compareTo(UriObject o) {
                if (this.timeTaken != null && o.timeTaken != null) {
                    return this.timeTaken.compareTo(o.timeTaken);
                } else {
                    return 1;//should never get here
                }
            }
        }
    }
}
