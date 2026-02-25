/*
 * * Copyright (c) 2004-2023 MarkLogic Corporation
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

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Base monitoring class for tracking CoRB job progress and performance metrics.
 * Provides functionality for calculating throughput (transactions per second),
 * estimating time to completion, and generating progress messages.
 *
 * <p>Key metrics tracked:</p>
 * <ul>
 *   <li>Average transactions per second (TPS) since job start</li>
 *   <li>Current transactions per second (calculated from recent interval)</li>
 *   <li>Estimated time of completion (ETC) based on rolling average TPS</li>
 *   <li>Number of active threads</li>
 *   <li>Failed task count</li>
 * </ul>
 *
 * <p>The ETC calculation uses a rolling average of recent TPS values to provide
 * a more stable estimate that adapts to changing throughput.</p>
 */
public class BaseMonitor {

	/**
	 * Default number of TPS samples to use for estimated time of completion calculation.
	 */
	protected static final int DEFAULT_NUM_TPS_FOR_ETC = 10;

	/**
	 * Timestamp of the last progress update in milliseconds.
	 */
    protected long lastProgress;

    /**
     * The CoRB manager instance being monitored.
     */
    protected final Manager manager;

    /**
     * Total number of tasks to be processed in the job.
     */
    protected long taskCount;

    /**
     * Number of tasks completed at the previous measurement interval.
     * Used to calculate current TPS over the most recent time period.
     */
    protected long prevCompleted = 0;

    /**
     * Timestamp in milliseconds of the previous measurement.
     * Used to calculate current TPS over the most recent time period.
     */
    protected long prevMillis = 0;

    /**
     * Timestamp in milliseconds when monitoring started.
     * Used as the baseline for calculating average TPS.
     */
    protected long startMillis;

    /**
     * Rolling list of recent TPS values used for calculating a smoothed ETC.
     * Size is limited by {@link #numTpsForEtc}.
     */
    protected final List<Double> tpsForETCList;

    /**
     * Maximum number of TPS samples to maintain in the rolling average.
     * Configured via manager options or defaults to {@link #DEFAULT_NUM_TPS_FOR_ETC}.
     */
    protected final int numTpsForEtc;

	/**
	 * Average transactions per second calculated from job start to current time.
	 */
	protected Double avgTps = 0d;

	/**
	 * Current transactions per second calculated from the most recent time interval.
	 */
    protected Double currentTps = 0d;

	/**
	 * Formatted string representing the estimated time of completion (HH:MM:SS).
	 * May include "(paused)" suffix if the job is paused.
	 */
	protected String estimatedTimeOfCompletion = "";

    /**
     * Constructs a BaseMonitor for the specified manager.
     * Initializes start time and TPS tracking list.
     *
     * @param manager the CoRB manager to monitor
     */
    public BaseMonitor(Manager manager) {
        this.manager = manager;
        startMillis = System.currentTimeMillis();
        numTpsForEtc = manager !=null && manager.getOptions() != null ? manager.getOptions().getNumTpsForETC() : DEFAULT_NUM_TPS_FOR_ETC;
        tpsForETCList = new ArrayList<>(this.numTpsForEtc);
    }

    /**
     * Updates TPS metrics and estimated time of completion.
     * Calculates both average TPS (since start) and current TPS (recent interval).
     * Maintains a rolling average for ETC calculation.
     *
     * @param completed the number of tasks completed so far
     */
    protected void populateTps(long completed){
    	long curMillis = System.currentTimeMillis();
        avgTps = calculateTransactionsPerSecond(completed, curMillis, startMillis);
        currentTps = avgTps;
        if (prevMillis > 0) {
            currentTps = calculateTransactionsPerSecond(completed, prevCompleted, curMillis, prevMillis);
        }
        prevCompleted = completed;
        prevMillis = curMillis;

        boolean isPaused = manager.isPaused();
        double tpsForETC = calculateTpsForETC(currentTps, isPaused);
        estimatedTimeOfCompletion = getEstimatedTimeCompletion(taskCount, completed, tpsForETC, isPaused);
    }

    /**
     * Generates a progress message with all metrics.
     * Calculates ETC from TPS and delegates to overloaded method.
     *
     * @param completed number of tasks completed
     * @param taskCount total number of tasks
     * @param tps average transactions per second
     * @param curTps current transactions per second
     * @param tpsForETC transactions per second to use for ETC calculation
     * @param threads number of active threads
     * @param isPaused whether the job is currently paused
     * @return formatted progress message
     */
    protected static String getProgressMessage(long completed, long taskCount, double tps, double curTps, double tpsForETC, int threads, boolean isPaused) {
        String etc = getEstimatedTimeCompletion(taskCount, completed, tpsForETC, isPaused);
    	return getProgressMessage(completed, taskCount, tps, curTps, etc, threads);
    }

    /**
     * Generates a progress message with pre-calculated ETC.
     * Assumes no failed tasks (calls overloaded method with failedTasks=0).
     *
     * @param completed number of tasks completed
     * @param taskCount total number of tasks
     * @param tps average transactions per second
     * @param curTps current transactions per second
     * @param etc formatted estimated time of completion string
     * @param threads number of active threads
     * @return formatted progress message
     */
    protected static String getProgressMessage(long completed, long taskCount, double tps, double curTps, String etc, int threads) {
        return  getProgressMessage( completed,  taskCount,  tps,  curTps,  etc,  threads,0);
    }

    /**
     * Generates a comprehensive progress message.
     * Format: "completed/total, [X tasks failed, ]tps(avg), tps(cur), ETC HH:MM:SS, N active threads."
     *
     * @param completed number of tasks completed
     * @param taskCount total number of tasks
     * @param tps average transactions per second
     * @param curTps current transactions per second
     * @param etc formatted estimated time of completion string
     * @param threads number of active threads
     * @param failedTasks number of failed tasks (0 to omit from message)
     * @return formatted progress message
     */
    protected static String getProgressMessage(long completed, long taskCount, double tps, double curTps, String etc, int threads, long failedTasks) {
        String failedTaskMessage = failedTasks > 0 ? failedTasks + " tasks failed, " : "";
    	return completed + "/" + taskCount + ", "
        		+ failedTaskMessage
                + formatTransactionsPerSecond(tps) + " tps(avg), "
                + formatTransactionsPerSecond(curTps) + " tps(cur), "
                + "ETC " + etc + ", "
                + threads + " active threads.";
    }

    /**
     * Calculates the transactions per second to use for ETC estimation.
     * Maintains a rolling average of recent TPS values (sliding window).
     * Clears the history if TPS is zero and the job is paused.
     *
     * @param currentTransactionsPerSecond the current TPS value
     * @param isPaused whether the job is currently paused
     * @return the average TPS from the rolling window
     */
    protected double calculateTpsForETC(double currentTransactionsPerSecond, boolean isPaused) {
        if (isZero(currentTransactionsPerSecond) && isPaused) {
            tpsForETCList.clear();
        } else {
            if (tpsForETCList.size() >= numTpsForEtc) {
                tpsForETCList.remove(0);
            }
            tpsForETCList.add(currentTransactionsPerSecond);
        }

        double transactionsPerSecondForETC = 0;
        double sum = 0;
        for (Double next : tpsForETCList) {
            sum += next;
        }
        if (!tpsForETCList.isEmpty()) {
            transactionsPerSecondForETC = sum / tpsForETCList.size();
        }
        return transactionsPerSecondForETC;
    }

    /**
     * Determines if the given double value is equal to zero.
     * Uses Double.compare for proper floating-point comparison.
     *
     * @param value the value to check
     * @return true if the value equals 0.0, false otherwise
     */
    protected static boolean isZero(double value) {
        return Double.compare(value, 0.0) == 0;
    }

    /**
     * Calculates transactions per second from the start of monitoring.
     * Delegates to the overloaded method with previouslyCompleted=0.
     *
     * @param amountCompleted number of tasks completed
     * @param currentMillis current timestamp in milliseconds
     * @param previousMillis start timestamp in milliseconds
     * @return transactions per second
     */
    protected static double calculateTransactionsPerSecond(long amountCompleted, long currentMillis, long previousMillis) {
        return calculateTransactionsPerSecond(amountCompleted, 0, currentMillis, previousMillis);
    }

    /**
     * Calculates transactions per second for a specific time interval.
     * Formula: (amountCompleted - previouslyCompleted) * 1000 / (currentMillis - previousMillis)
     *
     * @param amountCompleted number of tasks completed at current time
     * @param previouslyCompleted number of tasks completed at previous time
     * @param currentMillis current timestamp in milliseconds
     * @param previousMillis previous timestamp in milliseconds
     * @return transactions per second for the interval
     */
    protected static double calculateTransactionsPerSecond(long amountCompleted, long previouslyCompleted, long currentMillis, long previousMillis) {
        return (amountCompleted - previouslyCompleted) * 1000d / (currentMillis - previousMillis);
    }

    /**
     * Calculates and formats the estimated time to completion.
     * Returns time in HH:MM:SS format with "(paused)" suffix if applicable.
     * Returns -1 for ETS calculation if TPS is zero.
     *
     * @param taskCount total number of tasks
     * @param completed number of tasks completed
     * @param tpsForETC transactions per second to use for calculation
     * @param isPaused whether the job is currently paused
     * @return formatted time string (e.g., "01:23:45" or "01:23:45 (paused)")
     */
    protected static String getEstimatedTimeCompletion(double taskCount, double completed, double tpsForETC, boolean isPaused) {
        double ets = !isZero(tpsForETC) ? (taskCount - completed) / tpsForETC : -1;
        int hours = (int) ets / 3600;
        int minutes = (int) (ets % 3600) / 60;
        int seconds = (int) ets % 60;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds)
                + (isPaused ? " (paused)" : "");
    }

    /**
     * Formats a number as transactions per second.
     * Returns an integer format for values >= 1, or up to 2 decimal places for values &lt; 1.
     * Uses HALF_UP rounding mode.
     *
     * @param n the number to format
     * @param groupingUsed whether to use thousands separators (e.g., "1,000")
     * @return formatted string representation
     */
    protected static String formatTransactionsPerSecond(Number n, boolean groupingUsed) {
        NumberFormat format = DecimalFormat.getInstance();
        format.setGroupingUsed(groupingUsed);
        format.setRoundingMode(RoundingMode.HALF_UP);
        format.setMinimumFractionDigits(0);
        format.setMaximumFractionDigits(2);
        return n.intValue() >= 1 ? format.format(n.intValue()) : format.format(n);
    }

    /**
     * Formats a number as transactions per second with thousands separators.
     * Convenience method that calls {@link #formatTransactionsPerSecond(Number, boolean)} with groupingUsed=true.
     *
     * @param n the number to format
     * @return formatted string representation with grouping
     */
    protected static String formatTransactionsPerSecond(Number n) {
        return formatTransactionsPerSecond(n, true);
    }
}
