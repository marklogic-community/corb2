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

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

public class BaseMonitor {

	protected static final int DEFAULT_NUM_TPS_FOR_ETC = 10;

    protected long lastProgress;
    protected final Manager manager;
    protected long taskCount;

    protected long prevCompleted = 0;
    protected long prevMillis = 0;
    protected long startMillis;

    protected final List<Double> tpsForETCList;
    protected final int numTpsForEtc;
	protected Double avgTps = 0d;
    protected Double currentTps = 0d;
	protected String estimatedTimeOfCompletion = "";

    public BaseMonitor(Manager manager) {
        this.manager = manager;
        startMillis = System.currentTimeMillis();
        numTpsForEtc = manager !=null && manager.getOptions() != null ? manager.getOptions().getNumTpsForETC() : DEFAULT_NUM_TPS_FOR_ETC;
        tpsForETCList = new ArrayList<>(this.numTpsForEtc);
    }

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

    protected static String getProgressMessage(long completed, long taskCount, double tps, double curTps, double tpsForETC, int threads, boolean isPaused) {
        String etc = getEstimatedTimeCompletion(taskCount, completed, tpsForETC, isPaused);
    	return getProgressMessage(completed, taskCount, tps, curTps, etc, threads);
    }

    protected static String getProgressMessage(long completed, long taskCount, double tps, double curTps, String etc, int threads) {
        return  getProgressMessage( completed,  taskCount,  tps,  curTps,  etc,  threads,0);
    }

    protected static String getProgressMessage(long completed, long taskCount, double tps, double curTps, String etc, int threads, long failedTasks) {
        String failedTaskMessage = failedTasks > 0 ? failedTasks + " tasks failed, " : "";
    	return completed + "/" + taskCount + ", "
        		+ failedTaskMessage
                + formatTransactionsPerSecond(tps) + " tps(avg), "
                + formatTransactionsPerSecond(curTps) + " tps(cur), "
                + "ETC " + etc + ", "
                + threads + " active threads.";
    }

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
     * Determine if the given double value is equal to zero
     * @param value
     * @return
     */
    protected static boolean isZero(double value) {
        return Double.compare(value, 0.0) == 0;
    }

    protected static double calculateTransactionsPerSecond(long amountCompleted, long currentMillis, long previousMillis) {
        return calculateTransactionsPerSecond(amountCompleted, 0, currentMillis, previousMillis);
    }

    protected static double calculateTransactionsPerSecond(long amountCompleted, long previouslyCompleted, long currentMillis, long previousMillis) {
        return (amountCompleted - previouslyCompleted) * 1000d / (currentMillis - previousMillis);
    }

    protected static String getEstimatedTimeCompletion(double taskCount, double completed, double tpsForETC, boolean isPaused) {
        double ets = !isZero(tpsForETC) ? (taskCount - completed) / tpsForETC : -1;
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
    protected static String formatTransactionsPerSecond(Number n, boolean groupingUsed) {
        NumberFormat format = DecimalFormat.getInstance();
        format.setGroupingUsed(groupingUsed);
        format.setRoundingMode(RoundingMode.HALF_UP);
        format.setMinimumFractionDigits(0);
        format.setMaximumFractionDigits(2);
        return n.intValue() >= 1 ? format.format(n.intValue()) : format.format(n);
    }

    protected static String formatTransactionsPerSecond(Number n) {
        return formatTransactionsPerSecond(n, true);
    }
}
