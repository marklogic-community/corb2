/*
 * Copyright (c)2005-2009 Mark Logic Corporation
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

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.marklogic.developer.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public class Monitor implements Runnable {

    protected static final int SLEEP_MILLIS = 500;

    private SimpleLogger logger;

    private CompletionService<String[]> cs;

    private long lastProgress = 0;

    private long startMillis;

    private Manager manager;

    private String[] lastUris;

    private long taskCount;

    private ThreadPoolExecutor pool;

    private boolean shutdownNow = false;

    /**
     * @param _pool
     * @param _cs
     * @param _manager
     * @param _logger
     */
    public Monitor(ThreadPoolExecutor _pool, CompletionService<String[]> _cs, Manager _manager, SimpleLogger _logger) {
        pool = _pool;
        cs = _cs;
        manager = _manager;
        logger = _logger;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
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
            logger.logException("interrupted: exiting", e);
        } catch (CorbException e){
			logger.logException("Unexpected error", e);
        }
    }

    private void monitorResults() throws InterruptedException,
            ExecutionException, CorbException {
        // fast-fail as soon as we see any exceptions
        logger.info("monitoring " + taskCount + " tasks");
        long completed = 0;
        Future<String[]> future = null;
        while (!shutdownNow) {
            // try to avoid thread starvation
            Thread.yield();

            future = cs.poll(TransformOptions.PROGRESS_INTERVAL_MS,TimeUnit.MILLISECONDS);
            if (null != future) {
                // record result, or throw exception
                lastUris = future.get();
                completed = completed+lastUris.length;
                //logger.fine("completed uris: " + Arrays.toString(lastUris));
            }
            
            long active = pool.getActiveCount();
            
            showProgress(completed);

            if (completed >= taskCount) {
                break;
            }else if(active == 0){
            	logger.warning("No active tasks found with "+(taskCount-completed)+" tasks remains to be completed");
            	if(pool.isTerminated()) break;
            }
        }
        logger.info("waiting for pool to terminate");
        pool.awaitTermination(1, TimeUnit.SECONDS);
        logger.info("completed all tasks " + completed);
        runPostBatchTaskIfExists(); //post batch tasks
		logger.info("Done");
    }

    private long showProgress(long completed) {
        long current = System.currentTimeMillis();
        if (current - lastProgress > TransformOptions.PROGRESS_INTERVAL_MS) {
            logger.info("completed " + getProgressMessage(completed));
            lastProgress = current;

            // check for low memory
            long freeMemory = Runtime.getRuntime().freeMemory();
            if (freeMemory < (16 * 1024 * 1024)) {
                logger.warning("free memory: "
                               + (freeMemory / (1024 * 1024))
                               + " MiB");
            }
        }
        return lastProgress;
    }

    /**
     * @param _count
     */
    public void setTaskCount(long _count) {
        taskCount = _count;
    }
    
    private long prevCompleted=0;
    private long prevMillis=0;
    private String getProgressMessage(long completed) {
        long curMillis = System.currentTimeMillis();
        int tps = (int) ((double) completed * (double) 1000 / (curMillis - startMillis));
        int curTps = tps;
        if(prevMillis > 0){
        	curTps = (int) ((double) (completed-prevCompleted) * (double) 1000 / (curMillis - prevMillis));
        }
        prevCompleted=completed;
        prevMillis = curMillis;
        long ets = (tps != 0) ? (taskCount-completed)/tps : -1;
        String etc = pad0((ets/3600))+":"+pad0((ets%3600)/60)+":"+pad0(ets%60);
        return completed + "/" + taskCount + ", " + tps + " tps(avg), "+ curTps + " tps(cur), ETC "+ etc + ", "+ pool.getActiveCount() + " active threads.";
    }
    
    private String pad0(long time){
    	return time < 10 ? "0"+time : time+"";
    }

    /**
     *
     */
    public void shutdownNow() {
        shutdownNow = true;
    }
    
    private void runPostBatchTaskIfExists() throws CorbException{
    	TaskFactory tf = new TaskFactory(manager);
		try{
    		Task postTask = tf.newPostBatchTask();
    		if(postTask != null){
    			logger.info("Running post batch Task");
    			postTask.call();
    		}
		}catch(Exception exc){
			throw new CorbException("Error invoking post batch task",exc);
		}
    }

}
