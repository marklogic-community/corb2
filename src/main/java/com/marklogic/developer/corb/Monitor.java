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

import java.util.Arrays;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public class Monitor implements Runnable {

	protected static final int SLEEP_MILLIS = 500;
	protected static final Logger LOG = Manager.getLogger();
	
	private CompletionService<String[]> cs;
	private long lastProgress = 0;
	private long startMillis;
	private Manager manager;
	private String[] lastUris;
	private long taskCount;
	private ThreadPoolExecutor pool;
	private boolean shutdownNow = false;
	protected long completed = 0;

	/**
	 * @param pool
	 * @param cs
	 * @param manager
	 */
	public Monitor(ThreadPoolExecutor pool, CompletionService<String[]> cs, Manager manager) {
		this.pool = pool;
		this.cs = cs;
		this.manager = manager;
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
			LOG.log(Level.SEVERE,"interrupted: exiting", e);
		} catch (CorbException e) {
			LOG.log(Level.SEVERE,"Unexpected error", e);
		}
	}

	private void monitorResults() throws InterruptedException, ExecutionException, CorbException {
		// fast-fail as soon as we see any exceptions
		LOG.log(Level.INFO, "monitoring {0} tasks", taskCount);
		Future<String[]> future = null;
		while (!shutdownNow) {
			// try to avoid thread starvation
			Thread.yield();

			future = cs.poll(TransformOptions.PROGRESS_INTERVAL_MS, TimeUnit.MILLISECONDS);
			if (null != future) {
				// record result, or throw exception
				lastUris = future.get();
				completed = completed + lastUris.length;
				//LOG.log(Level.FINE, "completed uris: {0}", Arrays.toString(lastUris));
			}

			showProgress();

			if (completed >= taskCount) {
				if(pool.getActiveCount() > 0 || (pool.getTaskCount() - pool.getCompletedTaskCount()) > 0){
					LOG.log(Level.SEVERE, "Thread pool is still active with all the tasks completed and received. We shouldn't see this message.");
				}
				break;
			} else if (pool.getActiveCount() == 0) {
				LOG.log(Level.WARNING, "No active tasks found with {0} tasks remains to be completed", (taskCount - completed));
				if (pool.isTerminated()){
					break;
				}
			}
		}
		LOG.info("waiting for pool to terminate");
		pool.awaitTermination(1, TimeUnit.SECONDS);
		LOG.log(Level.INFO, "completed all tasks {0}/{1}", new Object[]{completed, taskCount});		
	}

	private long showProgress() {
		long current = System.currentTimeMillis();
		if (current - lastProgress > TransformOptions.PROGRESS_INTERVAL_MS) {
			LOG.log(Level.INFO, "completed {0}", getProgressMessage(completed));
			lastProgress = current;

      // check for low memory
      long freeMemory = Runtime.getRuntime().freeMemory();
      if (freeMemory < (16 * 1024 * 1024)) {
          LOG.log(Level.WARNING, "free memory: {0} MiB", (freeMemory / (1024 * 1024)));
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
	
	public long getTaskCount(){
		return this.taskCount;
	}

	public long getCompletedCount() {
		return this.completed;
	}

	private long prevCompleted = 0;
	private long prevMillis = 0;

	private String getProgressMessage(long completed) {
		long curMillis = System.currentTimeMillis();
		int tps = (int) ((double) completed * (double) 1000 / (curMillis - startMillis));
		int curTps = tps;
		if (prevMillis > 0) {
			curTps = (int) ((double) (completed - prevCompleted) * (double) 1000 / (curMillis - prevMillis));
		}
		prevCompleted = completed;
		prevMillis = curMillis;
		long ets = (tps != 0) ? (taskCount - completed) / tps : -1;
		String etc = pad0((ets / 3600)) + ":" + pad0((ets % 3600) / 60) + ":" + pad0(ets % 60);
		return completed + "/" + taskCount + ", " + tps + " tps(avg), " + curTps + " tps(cur), ETC " + etc + ", "+ pool.getActiveCount() + " active threads.";
	}

	private String pad0(long time) {
		return time < 10 ? "0" + time : time + "";
	}

    /**
     *
     */
	public void shutdownNow() {
		shutdownNow = true;
	}

}
