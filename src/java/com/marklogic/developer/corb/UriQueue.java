/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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

    protected boolean active;

    protected ThreadPoolExecutor pool;

    protected SimpleLogger logger;

    protected Monitor monitor;

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
        long count = 0;

        active = true;

        if (null == factory) {
            throw new NullPointerException("null factory");
        }
        if (null == completionService) {
            throw new NullPointerException("null completion service");
        }

        while (null != queue) {
            String uri = null;
            try {
                uri = queue.poll(SLEEP_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.logException("interrupted", e);
                if (null == uri) {
                    continue;
                }
            }
            if (null == uri) {
                if (active) {
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
        }

        pool.shutdown();

        logger.fine("finished queuing " + count + " uris");
    }

    public void shutdown() {
        // graceful shutdown, draining the queue
        logger.info("closing queue");
        active = false;
        pool.shutdown();
    }

    /**
     * 
     */
    public void halt() {
        // something bad happened - make sure we exit the loop
        logger.info("halting queue");
        queue = null;
        active = false;
        pool.shutdownNow();
        interrupt();
    }

    /**
     * @param _uri
     */
    public void add(String _uri) {
        queue.add(_uri);
    }

    /**
     * @param _factory
     * @throws InterruptedException
     */
    public void setFactory(TaskFactory _factory)
            throws InterruptedException {
        while (queue.size() > 0) {
            Thread.sleep(SLEEP_MILLIS);
        }
        factory = _factory;
    }

}
