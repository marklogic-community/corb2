/*
 * Copyright (c) 2008-2009 Mark Logic Corporation. All rights reserved.
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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.Configuration;

import com.marklogic.developer.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class UriQueue extends Thread {

    protected static final long SLEEP_MILLIS = 125;

    public static final int MONITOR_UPDATE_COUNT = 10000;

    protected Configuration configuration;

    protected volatile BlockingQueue<String> queue;

    protected TaskFactory factory;

    protected CompletionService<String> completionService;

    protected boolean shutdown = false;

    protected boolean halt = false;

    protected ThreadPoolExecutor pool;

    protected SimpleLogger logger;

    protected Monitor monitor;

    protected long expectedCount;

    private long count;

    /**
     * @param _cs
     * @param _pool
     * @param _factory
     * @param _monitor
     * @param _queue
     * @param _logger
     */
    public UriQueue(CompletionService<String> _cs,
            ThreadPoolExecutor _pool, TaskFactory _factory,
            Monitor _monitor, BlockingQueue<String> _queue,
            SimpleLogger _logger) {
        pool = _pool;
        factory = _factory;
        monitor = _monitor;
        queue = _queue;
        completionService = _cs;
        logger = _logger;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Thread#run()
     */
    public void run() {
        count = 0;

        if (null == factory) {
            throw new NullPointerException("null factory");
        }
        if (null == completionService) {
            throw new NullPointerException("null completion service");
        }
        
        while (!halt) {
            String uri = null;
            try {
                uri = queue.poll(SLEEP_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // reset interrupt status and continue
                Thread.interrupted();
                logger.logException("interrupted", e);
                if (null == uri) {
                    continue;
                }
            }
            if (null == uri) {
                if (!halt) {
                    continue;
                }
                // queue is empty
                break;
            }
            if (0 == count) {
                logger.fine("took first uri: " + uri);
            }
            logger.fine(count + ": uri = " + uri);

            completionService.submit(factory.newTask(uri));
            count++;

            if (count >= expectedCount) {
                break;
            }
        }

        pool.shutdown();

        logger.fine("finished queuing " + count + " uris");

        if (expectedCount != count) {
            logger
                    .warning("expected " + expectedCount + ", got "
                            + count);
            logger.warning("check your uri module!");
            return;
        }
    }

    public void shutdown() {
        // graceful shutdown, draining the queue
        logger.fine("closing queue " + count + "/" + expectedCount);
        shutdown = true;
    }

    /**
     * 
     */
    public void halt() {
        // something bad happened - make sure we exit the loop
        logger.warning("halting queue");
        queue = null;
        halt = true;
        pool.shutdownNow();
        interrupt();
    }

    /**
     * @param _uri
     */
    public void add(String _uri) {
        if (shutdown || halt) {
            throw new UnsupportedOperationException(
                    "queue has been halted or shut down");
        }
        queue.add(_uri);
    }

    /**
     * @param _count
     */
    public void setExpected(long _count) {
        expectedCount = _count;
    }

}
