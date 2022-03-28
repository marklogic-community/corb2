/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public class Monitor extends BaseMonitor implements Runnable {

    protected static final Logger LOG = Logger.getLogger(Monitor.class.getName());
    protected boolean shutdownNow;
    protected long completed = 0L;

    protected PausableThreadPoolExecutor threadPoolExecutor;
    protected final CompletionService<String[]> cs;
    /**
     * @param threadPoolExecutor
     * @param cs
     * @param manager
     */
    public Monitor(PausableThreadPoolExecutor threadPoolExecutor, CompletionService<String[]> cs, Manager manager) {
        super(manager);
        this.threadPoolExecutor = threadPoolExecutor;
        this.cs = cs;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
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

    protected String getProgressMessage(long completed) {
        populateTps(completed);
        return getProgressMessage(completed, taskCount, avgTps, currentTps, estimatedTimeOfCompletion, threadPoolExecutor.getActiveCount(), threadPoolExecutor.getNumFailedUris());
    }

    /**
     * @param count
     */
    public void setTaskCount(long count) {
        taskCount = count;
    }

    public long getTaskCount() {
        return taskCount;
    }

    public long getCompletedCount() {
        return this.completed;
    }

    public PausableThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

     /**
     *
     */
    public void shutdownNow() {
        shutdownNow = true;
    }

}
