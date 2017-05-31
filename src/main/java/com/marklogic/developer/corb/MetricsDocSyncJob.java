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

import static java.util.logging.Level.SEVERE;

import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public class MetricsDocSyncJob implements Runnable {

	protected static final Logger LOG = Logger.getLogger(Monitor.class.getName());

	protected static final int DEFAULT_NUM_TPS_FOR_ETC = 10;

	private int syncFrequencyInMillis = -1;
	private Manager manager = null;
	private boolean shutdownNow;
	private boolean paused;

	/**
	 * @param pool
	 * @param cs
	 * @param manager
	 */
	public MetricsDocSyncJob(Manager manager, int syncFrequencyInMillis) {
		this.manager = manager;
		this.syncFrequencyInMillis = syncFrequencyInMillis;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while (!shutdownNow && syncFrequencyInMillis >0) {
			if (!paused) {

				try {
					Thread.yield();
					Thread.sleep(syncFrequencyInMillis);
					syncDoc();
				} catch (InterruptedException e) {
					// reset interrupt status and exit
					Thread.interrupted();
					LOG.log(SEVERE, "interrupted: exiting", e);
				} catch (Exception e) {
					LOG.log(SEVERE, "Unexpected error", e);
				}
			}
		}
	}

	private void syncDoc() {
		manager.logJobStatsToServerDocument();
	}

	/**
	 *
	 */
	public void shutdownNow() {
		shutdownNow = true;
	}

	/**
	 * @return the paused
	 */
	protected boolean isPaused() {
		return paused;
	}

	/**
	 * @param paused
	 *            the paused to set
	 */
	protected void setPaused(boolean paused) {
		this.paused = paused;
	}

}
