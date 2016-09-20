/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
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
public class Monitor implements Runnable {

    protected static final Logger LOG = Logger.getLogger(Monitor.class.getName());

    protected static final int DEFAULT_NUM_TPS_FOR_ETC = 10;

    private final CompletionService<String[]> cs;
    private long lastProgress = 0;
    private long startMillis;
    private final Manager manager;
    private String[] lastUris;
    private long taskCount;
    private final PausableThreadPoolExecutor pool;
    private boolean shutdownNow;
    protected long completed = 0;
    private long prevCompleted = 0;
    private long prevMillis = 0;

    private final List<Double> tpsForETCList;
    private final int numTpsForEtc;

    /**
     * @param pool
     * @param cs
     * @param manager
     */
    public Monitor(PausableThreadPoolExecutor pool, CompletionService<String[]> cs, Manager manager) {
        this.pool = pool;
        this.cs = cs;
        this.manager = manager;

        this.numTpsForEtc = manager.getOptions() != null ? manager.getOptions().getNumTpsForETC() : DEFAULT_NUM_TPS_FOR_ETC;
        this.tpsForETCList = new ArrayList<>(this.numTpsForEtc);
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
        } catch (CorbException e) {
            LOG.log(SEVERE, "Unexpected error", e);
        }
    }

    private void monitorResults() throws InterruptedException, ExecutionException, CorbException {
        // fast-fail as soon as we see any exceptions
        LOG.log(INFO, "monitoring {0} tasks", taskCount);
        Future<String[]> future;
        while (!shutdownNow) {
            // try to avoid thread starvation
            Thread.yield();

            future = cs.poll(TransformOptions.PROGRESS_INTERVAL_MS, TimeUnit.MILLISECONDS);
            if (null != future) {
                // record result, or throw exception
                lastUris = future.get();
                completed += lastUris.length;
                //LOG.log(Level.FINE, "completed uris: {0}", Arrays.toString(lastUris));
            }

            showProgress();

            if (completed >= taskCount) {
                if (pool.getActiveCount() > 0 || (pool.getTaskCount() - pool.getCompletedTaskCount()) > 0) {
                    LOG.log(SEVERE, "Thread pool is still active with all the tasks completed and received. We shouldn't see this message.");
                }
                break;
            } else if (future == null && pool.getActiveCount() == 0) {
                LOG.log(WARNING, "No active tasks found with {0} tasks remains to be completed", (taskCount - completed));
            }
        }
        LOG.info("waiting for pool to terminate");
        pool.awaitTermination(1, TimeUnit.SECONDS);
        LOG.log(INFO, "completed all tasks {0}/{1}", new Object[]{completed, taskCount});
    }

    private long showProgress() throws InterruptedException {
        long current = System.currentTimeMillis();
        if (current - lastProgress > TransformOptions.PROGRESS_INTERVAL_MS) {
            if (pool.isPaused()) {
                LOG.log(INFO, "CoRB2 has been paused. Resume execution by changing the " + Options.COMMAND + " option in the command file {0} to RESUME", manager.getOption(COMMAND_FILE));
            }
            LOG.log(INFO, "completed {0}", getProgressMessage(completed));
            lastProgress = current;

            // check for low memory
            long freeMemory = Runtime.getRuntime().freeMemory();
            if (freeMemory < (16 * 1024 * 1024)) {
                LOG.log(WARNING, "free memory: {0} MiB", freeMemory / 1024 * 1024);
            }
        }
        return lastProgress;
    }

    /**
     * @param count
     */
    public void setTaskCount(long count) {
        taskCount = count;
    }

    public long getTaskCount() {
        return this.taskCount;
    }

    public long getCompletedCount() {
        return this.completed;
    }

    private String getProgressMessage(long completed) {
        long curMillis = System.currentTimeMillis();
        double tps = calculateTransactionsPerSecond(completed, curMillis, startMillis);
        double curTps = tps;
        if (prevMillis > 0) {
            curTps = calculateTransactionsPerSecond(completed, prevCompleted, curMillis, prevMillis);
        }
        prevCompleted = completed;
        prevMillis = curMillis;

        boolean isPaused = manager.isPaused();
        double tpsForETC = calculateTpsForETC(curTps, isPaused);
        return getProgressMessage(completed, taskCount, tps, curTps, tpsForETC, pool.getActiveCount(), isPaused);
    }

    protected double calculateTpsForETC(double curTps, boolean isPaused) {
        if (curTps == 0 && isPaused) {
            this.tpsForETCList.clear();
        } else {
            if (this.tpsForETCList.size() >= this.numTpsForEtc) {
                this.tpsForETCList.remove(0);
            }
            this.tpsForETCList.add(curTps);
        }

        double tpsForETC = 0;
        double sum = 0;
        for (Double next : this.tpsForETCList) {
            sum += next;
        }
        if (!this.tpsForETCList.isEmpty()) {
            tpsForETC = sum / this.tpsForETCList.size();
        }
        return tpsForETC;
    }

    static protected double calculateTransactionsPerSecond(long amountCompleted, long currentMillis, long previousMillis) {
        return calculateTransactionsPerSecond(amountCompleted, 0, currentMillis, previousMillis);
    }

    static protected double calculateTransactionsPerSecond(long amountCompleted, long previouslyCompleted, long currentMillis, long previousMillis) {
        return (amountCompleted - previouslyCompleted) * 1000d / (currentMillis - previousMillis);
    }

    static protected String getProgressMessage(long completed, long taskCount, double tps, double curTps, double tpsForETC, int threads, boolean isPaused) {
        String etc = getEstimatedTimeCompletion(taskCount, completed, tpsForETC, isPaused);
        return completed + "/" + taskCount + ", "
                + formatTransactionsPerSecond(tps) + " tps(avg), "
                + formatTransactionsPerSecond(curTps) + " tps(cur), "
                + "ETC " + etc + ", "
                + threads + " active threads.";
    }

    static protected String getEstimatedTimeCompletion(double taskCount, double completed, double tpsForETC, boolean isPaused) {
        double ets = (tpsForETC != 0) ? (taskCount - completed) / tpsForETC : -1;
        int hours = (int) ets / 3600;
        int minutes = (int) (ets % 3600) / 60;
        int seconds = (int) ets % 60;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds)
                + (isPaused ? " (paused)" : "");
    }

    /**
     * Returns a string representation of the number. Returns a decimal number
     * to two places if the value is less than 1.
     *
     * @param n
     * @return
     */
    static protected String formatTransactionsPerSecond(Number n) {
        NumberFormat format = DecimalFormat.getInstance();
        format.setRoundingMode(RoundingMode.HALF_UP);
        format.setMinimumFractionDigits(0);
        format.setMaximumFractionDigits(2);
        return n.intValue() >= 1 ? format.format(n.intValue()) : format.format(n);
    }

    /**
     *
     */
    public void shutdownNow() {
        shutdownNow = true;
    }

}
