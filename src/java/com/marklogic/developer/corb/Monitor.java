/*
 * Copyright (c)2005-2007 Mark Logic Corporation
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.developer.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Monitor implements Runnable {

    private static SimpleLogger logger;

    private ThreadPoolExecutor pool;

    private Task[] tasks;

    private long lastProgress = 0;

    private long startMillis;

    private Manager manager;

    /**
     * @param batchId
     * @param pool
     * @param manager
     */
    public Monitor(ThreadPoolExecutor pool, Manager manager) {
        this.pool = pool;
        this.manager = manager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        startMillis = System.currentTimeMillis();

        try {
            // block until at least one task has been queued
            // this will support faster fast-fail
            while (tasks == null || tasks[0] == null) {
                logger.finest("no transforms yet - napping");
                Thread.sleep(TransformOptions.SLEEP_TIME_MS);
            }

            monitorResults();
        } catch (InterruptedException e) {
            logger.logException("interrupted: exiting", e);
            return;
        }
    }

    private void monitorResults() throws InterruptedException {
        String uri;
        Task currentTask = null;
        // fast-fail as soon as we see any exceptions
        logger.info("monitoring " + tasks.length + " tasks");
        try {
            for (int i = 0; i < tasks.length; i++) {
                showProgress();

                // check the results
                try {
                    currentTask = tasks[i];
                    uri = currentTask.get();
                    logger.fine("output uri: " + uri);
                } catch (InterruptedException e) {
                    logger.logException("interrupted " + i, e);
                }
            }
            logger.info("waiting for pool to terminate");
            pool.awaitTermination(1, TimeUnit.SECONDS);
            logger
                    .info("completed all tasks "
                            + " status: "
                            + getProgressMessage(startMillis, pool,
                                    tasks.length));
        } catch (ExecutionException e) {
            // tell the main thread to quit
            manager.stop(e, currentTask.getTransform());
            return;
        }
    }

    private long showProgress() {
        long current = System.currentTimeMillis();
        if (current - lastProgress > TransformOptions.PROGRESS_INTERVAL_MS) {
            logger
                    .info("status: "
                            + getProgressMessage(startMillis, pool,
                                    tasks.length));
            lastProgress = current;
        }
        return lastProgress;
    }

    /**
     * @param tasks
     */
    public void setTasks(Task[] tasks) {
        this.tasks = tasks;
    }

    private String getProgressMessage(long start,
            ThreadPoolExecutor pool, long total) {
        long completed = pool.getCompletedTaskCount();
        int tps = (int) ((double) completed * (double) 1000 / (System
                .currentTimeMillis() - start));
        return completed + "/" + total + ", " + tps + " tps, "
                + pool.getActiveCount() + " active threads";
    }

    /**
     * @param pool
     */
    public void setPool(ThreadPoolExecutor pool) {
        this.pool = pool;
    }

    /**
     * @param _logger
     */
    public static void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

}
