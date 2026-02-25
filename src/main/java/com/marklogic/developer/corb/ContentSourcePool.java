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

import java.io.Closeable;
import java.util.Properties;

import com.marklogic.xcc.ContentSource;
/**
 * Interface for managing a pool of XCC ContentSource connections to MarkLogic.
 * Provides connection pooling with support for multiple connection strings,
 * load balancing, retry logic, SSL configuration, and failover capabilities.
 *
 * <p>Implementations of this interface manage the lifecycle of ContentSource instances,
 * including creation, distribution, health monitoring, and cleanup. The pool supports
 * multiple MarkLogic hosts for high availability and load distribution.</p>
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Multiple connection support for load balancing and failover</li>
 *   <li>Configurable connection selection strategies (round-robin, random)</li>
 *   <li>Automatic retry with configurable intervals</li>
 *   <li>SSL/TLS support through SSLConfig</li>
 *   <li>Connection health tracking and removal of failed connections</li>
 *   <li>Resource cleanup through Closeable interface</li>
 * </ul>
 *
 * @see DefaultContentSourcePool
 * @since 2.4.0
 */
public interface ContentSourcePool extends Closeable{
    /**
     * Initializes the ContentSourcePool with configuration and connection strings.
     * This method should only be called once during CoRB initialization by AbstractManager.
     * Creates ContentSource instances for each connection string provided.
     *
     * @param properties configuration properties for connection management
     * @param sslConfig SSL configuration for secure connections, may be null for non-SSL
     * @param connectionStrings one or more XCC connection URI strings (e.g., "xcc://user:password@host:port/database")
     * @throws IllegalStateException if already initialized
     */
    void init(Properties properties, SSLConfig sslConfig, String... connectionStrings);

    /**
     * Returns the SSLConfig used by the content source pool.
     *
     * @return the SSLConfig instance, or null if SSL is not configured
     */
    SSLConfig sslConfig();

    /**
     * Returns the next available ContentSource from the pool.
     * The selection strategy (round-robin or random) is implementation-specific.
     * If a connection has recently failed, the implementation may wait for a configured
     * retry interval ({@link Options#XCC_CONNECTION_RETRY_INTERVAL}) before returning it.
     *
     * <p>DefaultContentSourcePool uses round-robin by default and tracks failed connections
     * to implement retry delays.</p>
     *
     * @return a ContentSource instance from the pool
     * @throws CorbException if no content sources are available or all have failed
     */
    ContentSource get() throws CorbException;

    /**
     * Removes a ContentSource from the pool.
     * This method should be called when a connection is determined to be permanently
     * unavailable or experiencing repeated failures. The connection will no longer
     * be returned by {@link #get()}.
     *
     * @param contentSource the ContentSource to remove from the pool
     */
    void remove(ContentSource contentSource);

    /**
     * Checks if there is at least one ContentSource available in the pool.
     * This can be used to verify pool initialization and availability before
     * attempting to retrieve connections.
     *
     * @return true if at least one ContentSource is available, false if the pool is empty
     */
    boolean available();

    /**
     * Returns all ContentSource instances managed by this pool.
     * This includes both healthy and potentially failed connections.
     * The array returned may be used for monitoring or administrative purposes.
     *
     * @return an array of all ContentSource instances in the pool, never null
     */
    ContentSource[] getAllContentSources();
}
