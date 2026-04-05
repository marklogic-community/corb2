/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.spi.ConnectionProvider;
import com.marklogic.xcc.spi.SingleHostAddress;
import com.marklogic.xcc.types.XdmVariable;

/**
 * Default implementation of ContentSourcePool with load balancing, failover, and automatic retry capabilities.
 * Manages a pool of XCC ContentSource connections to MarkLogic with support for multiple connection
 * strategies, health monitoring, and dynamic IP address resolution.
 *
 * <p><strong>Connection Policies:</strong></p>
 * <ul>
 *   <li><strong>ROUND-ROBIN</strong> (default): Distributes connections evenly across all available hosts in circular order</li>
 *   <li><strong>RANDOM</strong>: Selects a random host for each connection request</li>
 *   <li><strong>LOAD</strong>: Selects the host with the fewest active connections</li>
 * </ul>
 *
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li>Automatic failover when connections fail</li>
 *   <li>Configurable retry logic with exponential backoff</li>
 *   <li>Health tracking and automatic removal of failed hosts</li>
 *   <li>Dynamic IP address resolution for DNS round-robin and load balancer support</li>
 *   <li>Connection renewal to detect IP changes</li>
 *   <li>Transparent retry through dynamic proxies</li>
 *   <li>Thread-safe operations</li>
 * </ul>
 *
 * <p>Uses dynamic proxies to intercept ContentSource and Session method calls, enabling
 * transparent retry logic and connection health tracking without modifying client code.</p>
 *
 * @see ContentSourcePool
 * @see AbstractContentSourcePool
 * @since 2.4.0
 */
public class DefaultContentSourcePool extends AbstractContentSourcePool {
    /**
     * Connection policy constant for round-robin selection.
     * Distributes connections evenly across all available hosts in circular order.
     * This is the default policy if none is specified.
     */
    protected static final String CONNECTION_POLICY_ROUND_ROBIN = "ROUND-ROBIN";

    /**
     * Connection policy constant for random selection.
     * Selects a random host for each connection request.
     * Provides basic load distribution without state tracking.
     */
    protected static final String CONNECTION_POLICY_RANDOM = "RANDOM";

    /**
     * Connection policy constant for load-based selection.
     * Selects the host with the fewest active connections.
     * Requires tracking connection counts for each ContentSource.
     */
    protected static final String CONNECTION_POLICY_LOAD = "LOAD";

    /**
     * The active connection policy being used (ROUND-ROBIN, RANDOM, or LOAD).
     * Defaults to ROUND-ROBIN if not explicitly configured.
     * Set during initialization from the {@link Options#CONNECTION_POLICY} property.
     */
    protected String connectionPolicy = CONNECTION_POLICY_ROUND_ROBIN;

    /**
     * Maps each ContentSource to its original XCC connection string.
     * Used for renewing ContentSources to detect IP address changes.
     * The connection string is stored during initialization and used to create
     * fresh ContentSource instances when DNS resolution may have changed.
     */
    protected final Map<ContentSource, String> connectionStringForContentSource = new HashMap<>();

    /**
     * List of all ContentSource instances currently in the pool.
     * ContentSources are added during initialization and when new IP addresses are discovered.
     * ContentSources are removed when they exceed the error retry limit or are explicitly removed.
     * Thread-safe access is ensured through synchronized methods.
     */
    protected final List<ContentSource> contentSources = new ArrayList<>();

    /**
     * Tracks the number of active connections for each ContentSource.
     * Used by the LOAD policy to select the host with the fewest connections.
     * Incremented by {@link #hold(ContentSource)} when a connection is allocated.
     * Decremented by {@link #release(ContentSource)} when a connection is released.
     */
    protected final Map<ContentSource, Long> connectionCountForContentSource = new HashMap<>();

    /**
     * Tracks the number of consecutive errors for each ContentSource.
     * Incremented by {@link #error(ContentSource, long)} when a connection fails.
     * Cleared by {@link #success(ContentSource)} when a connection succeeds.
     * When the error count exceeds {@link #hostRetryLimit}, the ContentSource is removed from the pool.
     */
    protected final Map<ContentSource, Long> errorCountForContentSource = new HashMap<>();

    /**
     * Tracks the timestamp (in milliseconds) of the last error for each ContentSource.
     * Used to implement retry interval delays - ContentSources are not selected for
     * {@link #retryInterval} seconds after their last error.
     * Entries are removed when the retry interval has elapsed or when a connection succeeds.
     */
    protected final Map<ContentSource, Long> errorTimeForContentSource = new HashMap<>();

    /**
     * Maps hostname:port to the set of IP addresses that have been seen for that host.
     * Used for DNS change detection - when a ContentSource resolves to a new IP address,
     * it indicates that DNS has changed or a load balancer has rotated IPs.
     * Enables dynamic adaptation to infrastructure changes without restarting the job.
     */
    protected final Map<String, Set<String>> ipAddressByHostAndPort = new HashMap<>();

    /**
     * Tracks the last renewal check timestamp (in milliseconds) for each ContentSource.
     * Used to implement periodic renewal intervals - ContentSources are checked for IP changes
     * every {@link #renewContentSourceInterval} seconds.
     * Updated in {@link #renewContentSource(ContentSource)} and when ContentSources are added.
     */
    protected final Map<ContentSource, Long> renewalTimeForContentSource = new HashMap<>();


    /**
     * Maximum number of consecutive errors before a host is removed from the pool.
     * Value of 0 means no limit (hosts are never removed due to error count).
     * Initialized from {@link Options#XCC_CONNECTION_HOST_RETRY_LIMIT} property.
     */
    protected int hostRetryLimit = 0;

    /**
     * Number of seconds to wait between retry attempts after a connection error.
     * After a ContentSource fails, it will not be selected again until this interval has passed.
     * Initialized from {@link Options#XCC_CONNECTION_RETRY_INTERVAL} property.
     */
    protected int retryInterval = 0;

    /**
     * Maximum number of retry attempts for a failed request.
     * When a request fails with a ServerConnectionException, the proxy will automatically
     * retry the request on a different ContentSource up to this many times.
     * Initialized from {@link Options#XCC_CONNECTION_RETRY_LIMIT} property.
     */
    protected int retryLimit = 0;

    /**
     * Current index for round-robin selection.
     * Points to the next ContentSource to be returned in round-robin order.
     * Value of -1 indicates uninitialized state; will be reset to 0 on first use.
     * Wraps back to 0 when it reaches the end of the contentSources list.
     */
    protected int roundRobinIndex =  -1;

    /**
     * Cached flag indicating whether the LOAD connection policy is active.
     * Set once during initialization to avoid repeated string comparisons.
     * When true, connection counts are tracked to enable load-based selection.
     */
    protected boolean isLoadPolicy = false;

    /**
     * Cached flag indicating whether the RANDOM connection policy is active.
     * Set once during initialization to avoid repeated string comparisons.
     * When true, ContentSources are selected randomly for each request.
     */
    protected boolean isRandomPolicy = false;

    /**
     * Flag indicating whether ContentSources should be periodically renewed to detect IP changes.
     * Initialized from {@link Options#CONTENT_SOURCE_RENEW} property (defaults to true).
     * When true, ContentSources are periodically re-resolved to detect DNS changes,
     * load balancer IP rotations, and other infrastructure changes.
     */
    protected boolean shouldRenewContentSource;

    /**
     * Interval in seconds between ContentSource renewal checks.
     * Value of 0 disables automatic renewal.
     * Initialized from {@link Options#CONTENT_SOURCE_RENEW_INTERVAL} property.
     * Used to implement periodic IP address change detection.
     */
    protected int renewContentSourceInterval;

    /**
     * Logger instance for logging pool operations, connection errors, policy selection,
     * IP address changes, and diagnostic information.
     */
    private static final Logger LOG = Logger.getLogger(DefaultContentSourcePool.class.getName());

    /**
     * Random number generator for RANDOM connection policy.
     * Used by {@link #nextContentSource()} to select ContentSources randomly.
     * Thread-safe because nextContentSource() is synchronized.
     */
    private final Random random = new Random();

    /**
     * Constructs a new DefaultContentSourcePool.
     */
    public DefaultContentSourcePool() {
        super();
    }

    /**
     * Initializes the content source pool with configuration and connection strings.
     * Creates ContentSource instances for each connection string and configures the connection policy.
     *
     * @param properties configuration properties
     * @param sslConfig SSL configuration for secure connections
     * @param connectionStrings one or more XCC connection URI strings
     * @throws NullPointerException if connectionStrings is null or empty
     */
    @Override
    public void init(Properties properties, SSLConfig sslConfig, String... connectionStrings) {
        super.init(properties, sslConfig);
        if (connectionStrings == null || connectionStrings.length == 0) {
            throw new NullPointerException("XCC connection strings cannot be null or empty");
        }
        synchronized (this) {
            shouldRenewContentSource = shouldRenewContentSource();
            renewContentSourceInterval = getRenewContentSourceInterval();
            retryInterval = getConnectRetryInterval();
            retryLimit = getConnectRetryLimit();
            hostRetryLimit = getConnectHostRetryLimit();
        }
        String policy = getProperty(CONNECTION_POLICY);
        if (CONNECTION_POLICY_RANDOM.equals(policy) || CONNECTION_POLICY_LOAD.equals(policy)) {
            this.connectionPolicy = policy;
        }
        LOG.log(INFO, "Using the connection policy {0}", connectionPolicy);

        for (String connectionString : connectionStrings) {
            initContentSource(connectionString);
        }

        //for better performance avoid too many string equals later for every get and submit
        synchronized (this) {
            isRandomPolicy = CONNECTION_POLICY_RANDOM.equals(connectionPolicy);
            isLoadPolicy = CONNECTION_POLICY_LOAD.equals(connectionPolicy);
        }
    }

    /**
     * Initializes a single ContentSource from a connection string.
     * Creates the ContentSource and adds it to the pool if successful.
     *
     * @param connectionString the XCC connection URI string
     */
    protected void initContentSource(String connectionString) {
        ContentSource contentSource = super.createContentSource(connectionString);
        if (contentSource != null) {
            addContentSource(contentSource, connectionString);
            LOG.log(INFO, "Initialized ContentSource {0}", asString(contentSource));
        }
    }

    /**
     * Adds a ContentSource to the pool.
     * Registers the ContentSource in all tracking maps and initializes its renewal timestamp.
     * Records the IP address for DNS change detection.
     *
     * @param contentSource the ContentSource to add
     * @param connectionString the original connection string
     */
    protected void addContentSource(ContentSource contentSource, String connectionString) {
        LOG.log(INFO, String.format("Adding new ContentSource for %s with IP %s", asString(contentSource), getIPAddress(contentSource)));
        contentSources.add(contentSource);
        connectionStringForContentSource.put(contentSource, connectionString);
        renewalTimeForContentSource.put(contentSource, System.currentTimeMillis());
        ipAddressByHostAndPort.computeIfAbsent(asString(contentSource), key -> new HashSet<>())
            .add(getIPAddress(contentSource));
    }

    /**
     * Gets the next available ContentSource from the pool.
     * Implements retry delay if the selected ContentSource has recently failed.
     * Returns a proxy that enables transparent retry logic.
     *
     * <p><strong>Note:</strong> This method is intentionally not synchronized to avoid
     * performance degradation from Thread.sleep() calls.</p>
     *
     * @return a proxied ContentSource instance
     * @throws CorbException if no ContentSource is available
     */
    @Override
    public ContentSource get() throws CorbException {
        ContentSource contentSource = nextContentSource();
        if (contentSource == null) {
            throw new CorbException("ContentSource not available.");
        }

        //if the nextContentSource() returns the connection with existing errors, then it means it could not find
        //any clean connections, so we need to wait.
        //even if errored, but wait expired, then no need to wait.
        Long failedCount = errorCountForContentSource.get(contentSource);
        if (failedCount != null && failedCount > 0 && errorTimeForContentSource.containsKey(contentSource)) {
            LOG.log(WARNING, "Connection failed for ContentSource {0}. Waiting for {1} seconds before retry attempt {2}",
                    new Object[]{asString(contentSource), retryInterval, failedCount + 1});
            try {
                Thread.sleep(retryInterval * 1000L);
            } catch (InterruptedException ex) {
                LOG.log(WARNING, "Interrupted!", ex);
                Thread.currentThread().interrupt();
            }
        }

        return createContentSourceProxy(contentSource);
    }

    /**
     * Selects the next ContentSource based on the configured connection policy.
     * Implements ROUND-ROBIN, RANDOM, or LOAD selection strategy.
     * Periodically checks for IP address changes if renewal is enabled.
     *
     * @return the selected ContentSource, or null if none available
     */
    protected synchronized ContentSource nextContentSource() {
        List<ContentSource> availableList = getAvailableContentSources();
        if (availableList.isEmpty()) {
            return null;
        }

        ContentSource contentSource = null;
        if (availableList.size() == 1) {
            contentSource = availableList.get(0);
        } else if (isRandomPolicy) {
            contentSource = availableList.get(this.random.nextInt(availableList.size()));
        } else if (isLoadPolicy) {
            for (ContentSource next: availableList) {
                long connectionCount = connectionCountForContentSource.getOrDefault(next, 0L);
                if (connectionCount == 0) {
                    contentSource = next;
                    break;
                } else if (contentSource == null || connectionCount < connectionCountForContentSource.get(contentSource)) {
                    contentSource = next;
                }
            }
        } else {
            roundRobinIndex++;
            if (roundRobinIndex >= availableList.size()) {
                roundRobinIndex = 0;
            }
            contentSource = availableList.get(roundRobinIndex);
        }
        /*
        Periodically check to see if the ContentSource will resolve different/additional IP addresses and add them to the pool.
        This can help spread the load across a pool of multiple IP addresses returned from DNS and dynamically adjust to any changes.
         */
        if (renewContentSourceInterval > 0 &&
            (System.currentTimeMillis() - renewalTimeForContentSource.getOrDefault(contentSource, System.currentTimeMillis())) >= (renewContentSourceInterval * 1000L)) {
            renewContentSource(contentSource);
        }

        return contentSource;
    }

    /**
     * Gets the list of ContentSources that are available for use.
     * Filters out ContentSources that are within their retry wait period.
     * Returns all ContentSources if none are available outside the retry period.
     *
     * @return list of available ContentSource instances
     */
    protected synchronized List<ContentSource> getAvailableContentSources() {
        //check if any errored connections are eligible for retries without further wait
        if (!errorTimeForContentSource.isEmpty()) {
            long current = System.currentTimeMillis();
            errorTimeForContentSource.entrySet().removeIf(next -> (current - next.getValue()) >= (retryInterval * 1000L));
        }
        if (!errorTimeForContentSource.isEmpty()) {
            List<ContentSource> availableList = new ArrayList<>(contentSources.size());
            for (ContentSource contentSource: contentSources) {
                if (!errorTimeForContentSource.containsKey(contentSource)) {
                    availableList.add(contentSource);
                }
            }
            //if nothing available, then return the whole list as we have to wait anyway
            return !availableList.isEmpty() ? availableList : contentSources;
        } else {
            return contentSources;
        }
    }

    /**
     * Removes a ContentSource from the pool.
     * Extracts the actual ContentSource from the proxy if necessary and removes it
     * from all tracking data structures.
     *
     * @param proxiedContentSource the ContentSource (possibly a proxy) to remove
     */
    @Override
    public synchronized void remove(ContentSource proxiedContentSource) {
        ContentSource contentSource = getContentSourceFromProxy(proxiedContentSource);
        if (contentSources.contains(contentSource)) {
            LOG.log(WARNING, "Removing the ContentSource {0} with IP {1} from the content source pool.", new Object[]{asString(contentSource), getIPAddress(contentSource)});
            contentSources.remove(contentSource);
            removeContentSourceFromStatusTrackers(contentSource);
        }
    }

    /**
     * Checks if any ContentSources are available in the pool.
     *
     * @return true if at least one ContentSource is available, false otherwise
     */
    @Override
    public boolean available() {
        return !contentSources.isEmpty();
    }

    /**
     * Gets all ContentSources currently in the pool.
     *
     * @return array of all ContentSource instances in the pool
     */
    @Override
    public ContentSource[] getAllContentSources() {
        return contentSources.toArray(new ContentSource[0]);
    }

    /**
     * Closes the pool and clears all tracking data structures.
     * This method does not close the individual ContentSource connections.
     */
    @Override
    public void close() {
        contentSources.clear();
        connectionCountForContentSource.clear();
        connectionStringForContentSource.clear();
        errorCountForContentSource.clear();
        errorTimeForContentSource.clear();
        ipAddressByHostAndPort.clear();
        renewalTimeForContentSource.clear();
    }

    /**
     * Checks if the RANDOM connection policy is active.
     *
     * @return true if the RANDOM policy is configured, false otherwise
     */
    protected boolean isRandomPolicy() {
        return this.isRandomPolicy;
    }

    /**
     * Checks if the LOAD connection policy is active.
     *
     * @return true if the LOAD policy is configured, false otherwise
     */
    protected boolean isLoadPolicy() {
        return this.isLoadPolicy;
    }

    /**
     * Increments the active connection count for a ContentSource.
     * Used by the LOAD policy to track connection distribution.
     *
     * @param contentSource the ContentSource whose connection count should be incremented
     */
    protected synchronized void hold(ContentSource contentSource) {
        if (contentSources.contains(contentSource)) {
	        long count = connectionCountForContentSource.getOrDefault(contentSource, 0L) + 1;
	        connectionCountForContentSource.put(contentSource, count);
        }
    }

    /**
     * Decrements the active connection count for a ContentSource.
     * Used by the LOAD policy to track connection distribution.
     *
     * @param contentSource the ContentSource whose connection count should be decremented
     */
    protected synchronized void release(ContentSource contentSource) {
        long count = connectionCountForContentSource.getOrDefault(contentSource, 0L);
        if (count > 0) {
            connectionCountForContentSource.put(contentSource, count - 1);
        }
    }

    /**
     * Marks a successful connection for a ContentSource.
     * Clears error count and error timestamp tracking.
     *
     * @param contentSource the ContentSource that successfully completed a connection
     */
    protected synchronized void success(ContentSource contentSource) {
		errorCountForContentSource.remove(contentSource);
		errorTimeForContentSource.remove(contentSource);
	}

    /**
     * Records an error for a ContentSource without allocation time tracking.
     * Delegates to {@link #error(ContentSource, long)} with allocTime = -1.
     *
     * @param contentSource the ContentSource that experienced an error
     */
    protected void error(ContentSource contentSource) {
        error(contentSource,-1);
    }

    /**
     * Records an error for a ContentSource with allocation time tracking.
     * Increments the error count and updates the error timestamp if the error occurred
     * after the last recorded error. Removes the ContentSource if it exceeds the host retry limit.
     * Attempts to renew the ContentSource to detect IP address changes.
     *
     * @param contentSource the ContentSource that experienced an error
     * @param allocTime the timestamp when the ContentSource was allocated, or -1 to ignore timing
     */
    protected synchronized void error(ContentSource contentSource, long allocTime) {
        if (contentSources.contains(contentSource)) {
            Long lastErrorTime = errorTimeForContentSource.get(contentSource);
            if (lastErrorTime == null || allocTime <= 0 || allocTime > lastErrorTime) {
		        long errorCount = errorCount(contentSource) + 1;
                errorCountForContentSource.put(contentSource, errorCount);
                errorTimeForContentSource.put(contentSource, System.currentTimeMillis());

                LOG.log(WARNING, "Connection error count for ContentSource {0} is {1}. Max limit is {2}.", new Object[]{asString(contentSource), errorCount, hostRetryLimit});
                if (errorCount > hostRetryLimit) {
                    remove(contentSource);
                }
                //re-bind and obtain IP, adding new ones to the ContentSource pool, which can help with proxies and load balancers with dynamic IP
                renewContentSource(contentSource);

            } else {
                LOG.log(WARNING, "Connection error for ContentSource {0} is not counted towards the limit as it was allocated before last error.", new Object[]{asString(contentSource)});
            }
        }
	}
    /**
     * Attempts to renew a ContentSource to detect IP address changes.
     * Creates a new ContentSource from the same connection string and adds it to the pool
     * if it resolves to a different IP address. This enables dynamic adaptation to
     * DNS changes, load balancers, and IP address rotations.
     *
     * @param contentSource the ContentSource to renew
     */
    protected synchronized void renewContentSource(ContentSource contentSource) {
        if (shouldRenewContentSource) {
            String xccConnectionString = connectionStringForContentSource.get(contentSource);
            renewalTimeForContentSource.put(contentSource, System.currentTimeMillis());
            ContentSource freshContentSource = super.createContentSource(xccConnectionString);
            if (freshContentSource != null && !ipAddressByHostAndPort.getOrDefault(asString(freshContentSource), Collections.emptySet())
                    .contains(getIPAddress(freshContentSource)) &&
                haveDifferentIP(contentSource, freshContentSource)) {
                addContentSource(freshContentSource, xccConnectionString);
            }
        }
    }

    /**
     * Checks if two ContentSources have different IP addresses.
     * Useful for detecting when DNS resolution returns a new IP address.
     *
     * @param contentSourceA the first ContentSource
     * @param contentSourceB the second ContentSource
     * @return true if the ContentSources have different IP addresses, false otherwise
     */
    protected boolean haveDifferentIP(ContentSource contentSourceA, ContentSource contentSourceB) {
        boolean result = false;
        if (contentSourceB != null) {
            String currentIP = getIPAddress(contentSourceA);
            String freshIP = getIPAddress(contentSourceB);
            if (currentIP != null && !currentIP.equals(freshIP)) {
                LOG.log(Level.FINE, () -> String.format("%s IP changed from: %s to: %s", contentSourceA.getConnectionProvider().getHostName(), currentIP, freshIP));
                result = true;
            }
        }
        return result;
    }

    /**
     * Extracts the IP address from a ContentSource.
     * Only works for ContentSources using SingleHostAddress connection providers.
     *
     * @param contentSource the ContentSource to extract the IP address from
     * @return the IP address as a string, or null if not available
     */
    protected String getIPAddress(ContentSource contentSource) {
        String ip = null;
        ConnectionProvider connectionProvider = contentSource.getConnectionProvider();
        if (connectionProvider instanceof SingleHostAddress) {
            SingleHostAddress currentProvider = (SingleHostAddress) connectionProvider;
            InetSocketAddress address = currentProvider.getAddress();
            if (address != null && address.getAddress() != null) {
                ip = address.getAddress().getHostAddress();
            }
        }
        return ip;
    }

    /**
     * Gets the current error count for a ContentSource.
     *
     * @param contentSource the ContentSource to check
     * @return the number of errors recorded for this ContentSource
     */
    protected long errorCount(ContentSource contentSource) {
        return errorCountForContentSource.getOrDefault(contentSource, 0L);
    }

    /**
     * Removes a ContentSource from all internal tracking data structures.
     * Cleans up connection counts, error tracking, IP addresses, and renewal timestamps.
     *
     * @param contentSource the ContentSource to remove from tracking
     */
    private synchronized void removeContentSourceFromStatusTrackers(ContentSource contentSource) {
        connectionCountForContentSource.remove(contentSource);
        connectionStringForContentSource.remove(contentSource);
        errorCountForContentSource.remove(contentSource);
        errorTimeForContentSource.remove(contentSource);
        ipAddressByHostAndPort.getOrDefault(asString(contentSource), Collections.emptySet())
            .remove(getIPAddress(contentSource));
        renewalTimeForContentSource.remove(contentSource);
    }

    /**
     * Creates a safe string representation of a ContentSource.
     * Returns only hostname and port to avoid exposing credentials that may be
     * present in the connection string's toString() method.
     *
     * @param contentSource the ContentSource to represent as string
     * @return safe string in format "hostname:port", or "null" if contentSource is null
     */
    protected String asString(ContentSource contentSource) {
        if (contentSource == null) {
            return "null";
        }
        ConnectionProvider provider = contentSource.getConnectionProvider();
        return String.format("%s:%d", provider.getHostName(), provider.getPort());
    }

    /**
     * Creates a dynamic proxy for a ContentSource.
     * The proxy intercepts method calls to enable transparent retry logic and
     * connection health tracking.
     *
     * @param contentSource the ContentSource to wrap in a proxy
     * @return a proxied ContentSource instance
     */
    protected ContentSource createContentSourceProxy(ContentSource contentSource) {
        ClassLoader classLoader = DefaultContentSourcePool.class.getClassLoader() == null ? ClassLoader.getSystemClassLoader() : DefaultContentSourcePool.class.getClassLoader();
        if (classLoader == null) {
            return null;
        }
        return (ContentSource) Proxy.newProxyInstance(
            classLoader,
            new Class[] { ContentSource.class },
            new ContentSourceInvocationHandler(this, contentSource));
    }

    /**
     * Extracts the underlying ContentSource from a proxy.
     * If the provided ContentSource is not a proxy, returns it unchanged.
     *
     * @param contentSourceProxy the ContentSource that may be a proxy
     * @return the underlying ContentSource, or the original if not a proxy
     */
    public static ContentSource getContentSourceFromProxy(ContentSource contentSourceProxy) {
		ContentSource target = contentSourceProxy;
		if (contentSourceProxy != null && Proxy.isProxyClass(contentSourceProxy.getClass())) {
			InvocationHandler handler = Proxy.getInvocationHandler(contentSourceProxy);
			if (handler instanceof ContentSourceInvocationHandler) {
				target = ((ContentSourceInvocationHandler)handler).target;
			}
		}
		return target;
	}

    /**
     * Extracts the underlying Session from a proxy.
     * If the provided Session is not a proxy, returns it unchanged.
     *
     * @param sessionProxy the Session that may be a proxy
     * @return the underlying Session, or the original if not a proxy
     */
    public static Session getSessionFromProxy(Session sessionProxy) {
		Session target = sessionProxy;
		if (sessionProxy != null && Proxy.isProxyClass(sessionProxy.getClass())) {
			InvocationHandler handler = Proxy.getInvocationHandler(sessionProxy);
			if (handler instanceof SessionInvocationHandler) {
				target = ((SessionInvocationHandler)handler).target;
			}
		}
		return target;
	}

    /**
     * InvocationHandler for ContentSource proxies.
     * Intercepts method calls to ContentSource instances to enable transparent functionality:
     * <ul>
     *   <li>Wraps returned Session objects in SessionInvocationHandler proxies</li>
     *   <li>Tracks allocation time for error handling</li>
     *   <li>Delegates all method calls to the underlying ContentSource</li>
     * </ul>
     * This allows sessions created from the ContentSource to inherit retry logic and
     * connection health tracking without modifying client code.
     */
    protected static class ContentSourceInvocationHandler implements InvocationHandler {
        /**
         * Method name constant for newSession() method.
         * Used to detect when a Session is being created so it can be wrapped in a proxy.
         */
        static final String NEW_SESSION = "newSession";

        /**
         * Reference to the DefaultContentSourcePool that owns this proxy.
         * Used to pass connection pool context to SessionInvocationHandler proxies.
         */
        DefaultContentSourcePool contentSourcePool;

        /**
         * The underlying ContentSource instance being proxied.
         * All method calls are delegated to this target after interception logic.
         */
        ContentSource target;

        /**
         * Timestamp (in milliseconds) when this ContentSource was allocated from the pool.
         * Used for error tracking - errors are only counted if they occur after this time,
         * preventing stale connections from being penalized multiple times.
         */
        long allocTime;

        /**
         * Constructs a ContentSourceInvocationHandler for proxying a ContentSource.
         *
         * @param contentSourcePool the pool that owns this ContentSource
         * @param target the ContentSource to proxy
         */
        protected ContentSourceInvocationHandler(DefaultContentSourcePool contentSourcePool, ContentSource target) {
            this.contentSourcePool = contentSourcePool;
            this.target = target;
            this.allocTime = System.currentTimeMillis();
        }

        /**
         * Intercepts method calls to the ContentSource.
         * Wraps Session objects returned by newSession() in SessionInvocationHandler proxies
         * to enable transparent retry logic.
         *
         * @param proxy the proxy instance
         * @param method the method being invoked
         * @param args the method arguments
         * @return the method result (possibly a proxied Session)
         * @throws Throwable if the underlying method throws an exception
         */
        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Object obj = method.invoke(target, args);
			if (obj != null && isNewSession(method) && obj instanceof Session) {
				obj = createSessionProxy((Session)obj);
			}
			return obj;
		}

        /**
         * Creates a dynamic proxy for a Session.
         * The proxy enables transparent retry logic and connection health tracking.
         *
         * @param session the Session to wrap in a proxy
         * @return a proxied Session instance
         */
        protected Session createSessionProxy(Session session) {
            ClassLoader classLoader = DefaultContentSourcePool.class.getClassLoader() == null ? ClassLoader.getSystemClassLoader() : DefaultContentSourcePool.class.getClassLoader();
            if (classLoader == null) {
                return null;
            }
            return (Session)Proxy.newProxyInstance(
                classLoader,
                new Class[] { Session.class },
                new SessionInvocationHandler(contentSourcePool, target, session, allocTime));
        }

        /**
         * Checks if the method being invoked is newSession().
         *
         * @param method the method to check
         * @return true if the method is newSession(), false otherwise
         */
        private boolean isNewSession(Method method) {
        		return NEW_SESSION.equals(method.getName());
        }
    }

    /**
     * InvocationHandler for Session proxies.
     * Intercepts method calls to Session instances to provide:
     * <ul>
     *   <li>Automatic retry on ServerConnectionException</li>
     *   <li>Connection health tracking (success/error reporting)</li>
     *   <li>Load policy connection counting</li>
     *   <li>Transparent failover to alternative ContentSources</li>
     * </ul>
     *
     * <p><strong>Limitations:</strong></p>
     * <ul>
     *   <li>Does not support explicit commit() or rollback() operations (throws UnsupportedOperationException)</li>
     *   <li>Only supports AdhocQuery and ModuleInvoke request types</li>
     * </ul>
     *
     * <p>When a request fails with ServerConnectionException, this handler automatically:
     * <ol>
     *   <li>Records the error in the pool</li>
     *   <li>Obtains a new Session from the pool (potentially different host)</li>
     *   <li>Recreates the request with the same parameters</li>
     *   <li>Submits the request again</li>
     * </ol>
     * This process repeats up to {@link DefaultContentSourcePool#retryLimit} times.
     */
    protected static class SessionInvocationHandler implements InvocationHandler {
        /**
         * Method name constant for submitRequest() method.
         * Used to detect request submission for retry logic and connection tracking.
         */
        static final String SUBMIT_REQUEST = "submitRequest";

        /**
         * Method name constant for insertContent() method.
         * Used to detect content insertion for retry logic and connection tracking.
         */
        static final String INSERT_CONTENT = "insertContent";

        /**
         * Method name constant for commit() method.
         * Used to detect unsupported operations.
         */
        static final String COMMIT = "commit";

        /**
         * Method name constant for rollback() method.
         * Used to detect unsupported operations.
         */
        static final String ROLLBACK = "rollback";

        /**
         * Method name constant for close() method.
         * Used to ensure retry sessions are also closed.
         */
        static final String CLOSE = "close";

        /**
         * Empty sequence literal for creating placeholder requests.
         * Used when creating RequestException instances for insertContent errors.
         */
        static final String EMPTY_SEQ = "()";

        /**
         * Reference to the DefaultContentSourcePool that owns this proxy.
         * Used for connection health tracking and obtaining retry sessions.
         */
        private final DefaultContentSourcePool contentSourcePool;

        /**
         * The ContentSource that created this Session.
         * Used for connection health tracking (recording success/error).
         */
        private final ContentSource contentSource;

        /**
         * The underlying Session instance being proxied.
         * All method calls are delegated to this target after interception logic.
         */
        private final Session target;

        /**
         * Timestamp (in milliseconds) when the ContentSource was allocated from the pool.
         * Used for error tracking to prevent stale connections from being penalized.
         */
        private final long allocTime;

        /**
         * Number of request submission attempts (including retries) for the current operation.
         * Incremented each time submitRequest() or insertContent() is called.
         * Compared against {@link DefaultContentSourcePool#retryLimit} to determine if more retries are allowed.
         * Propagated to retry sessions to maintain attempt count across failovers.
         */
        private int attempts = 0;

        /**
         * Proxy session created for retry operations.
         * When a request fails and needs to be retried on a different host,
         * a new Session is obtained from the pool and stored here.
         * This session is closed when the original session is closed.
         */
        private Session retryProxy;

		/**
		 * Constructs a SessionInvocationHandler for proxying a Session.
		 *
		 * @param contentSourcePool the pool that owns this Session's ContentSource
		 * @param contentSource the ContentSource that created this Session
		 * @param target the Session to proxy
		 * @param allocTime timestamp when the ContentSource was allocated
		 */
		protected SessionInvocationHandler(DefaultContentSourcePool contentSourcePool, ContentSource contentSource, Session target, long allocTime) {
			this.contentSourcePool = contentSourcePool;
			this.contentSource = contentSource;
			this.target = target;
			this.allocTime = allocTime;
		}

        /**
         * Intercepts method calls to the Session.
         * Implements retry logic for submitRequest() and insertContent() methods.
         * Tracks connection health (success/error) and connection counts for load policy.
         * Ensures retry sessions are closed when the original session is closed.
         *
         * @param proxy the proxy instance
         * @param method the method being invoked
         * @param args the method arguments
         * @return the method result
         * @throws Throwable if the method throws an exception or retry limit is exceeded
         */
        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            checkUnsupported(method);

			//NOTE: We only need to track connection counts for LOAD policy
			if (isSubmitRequest(method) || isInsertContent(method)) {
				if (isSubmitRequest(method)) {
                    validRequest(args);
                }

				if (contentSourcePool.isLoadPolicy()) {
                    contentSourcePool.hold(contentSource);
                }
				attempts++;
			}
			try {
				if (retryProxy != null && isClose(method)) {
					retryProxy.close(); //Use proxy only as there can be multiple retry attempts in a chain.
				}
				Object obj = method.invoke(target, args);
				//TODO: connection is held longer for streaming result sequence even after request is submitted.
				//We are ok now as we only use streaming results for query uris loader.
				if (isSubmitRequest(method) || isInsertContent(method)) {
					contentSourcePool.success(contentSource);
					if (contentSourcePool.isLoadPolicy()) {
                        contentSourcePool.release(contentSource);
                    }
				}
				return obj;
			} catch (Exception exc) {
				return handleInvokeException(exc, method, args);
			}
		}

        /**
         * Handles exceptions thrown during Session method invocation.
         * Implements retry logic for ServerConnectionException:
         * <ul>
         *   <li>Records the error in the pool</li>
         *   <li>Releases connection count for LOAD policy</li>
         *   <li>Retries on a new Session if retry limit not exceeded</li>
         *   <li>Unwraps InvocationTargetException to expose the real cause</li>
         * </ul>
         *
         * @param exc the exception that was thrown
         * @param method the method that threw the exception
         * @param args the method arguments
         * @return the result of the retry operation
         * @throws Throwable if retry limit is exceeded or exception is not retryable
         */
        protected Object handleInvokeException(Exception exc, Method method, Object[] args) throws Throwable {
            if (contentSourcePool != null && contentSourcePool.isLoadPolicy() && (isSubmitRequest(method) || isInsertContent(method))) {
                contentSourcePool.release(contentSource); //we should do this before the recursion. not finally.
            }
            if (exc instanceof InvocationTargetException) {
                if (exc.getCause() instanceof ServerConnectionException) {
                    if (contentSourcePool != null) {
                        contentSourcePool.error(contentSource, allocTime); //we should do this before the recursion. not finally.
                    }
                    String name = exc.getCause().getClass().getSimpleName();
                    if (isSubmitRequest(method) && attempts <= contentSourcePool.retryLimit) {
                        LOG.log(WARNING, "Submit request failed {0} times with {1}. Max Limit is {2}. Retrying..", new Object[]{attempts, name, contentSourcePool.retryLimit});
                        return submitAsNewRequest(args);
                    } else if (isInsertContent(method) && attempts <= contentSourcePool.retryLimit) {
                        LOG.log(WARNING, "Insert content failed {0} times {1}. Max Limit is {2}. Retrying..", new Object[]{attempts, name, contentSourcePool.retryLimit});
                        return insertAsNewRequest(args);
                    } else {
                        Throwable cause = exc.getCause();
                        if (cause != null) {
                            throw exc.getCause();
                        } else {
                            throw exc;
                        }
                    }
                } else {
                    Throwable cause = exc.getCause();
                    if (cause != null) {
                        throw exc.getCause();
                    } else {
                        throw exc;
                    }
                }
            } else {
                throw exc;
            }
        }

		/**
		 * Validates that the request is a supported type.
		 * Only AdhocQuery and ModuleInvoke requests are supported by CoRB.
		 *
		 * @param args the method arguments (first argument must be a Request)
		 * @throws IllegalArgumentException if the request type is not supported
		 */
		protected void validRequest(Object... args) {
			Request request = (Request)args[0];
			if (!(request instanceof AdhocQuery || request instanceof ModuleInvoke)) {
				throw new IllegalArgumentException("Only moduleInvoke or adhocQuery requests are supported by CoRB");
			}
		}

		/**
		 * Retries a failed request on a new Session from the pool.
		 * Creates a new request with the same query/module, options, and variables.
		 * Propagates the attempt count to the new session proxy.
		 *
		 * @param args the original method arguments (first argument is the Request)
		 * @return the ResultSequence from the retried request
		 * @throws RequestException if the retry fails or no ContentSource is available
		 */
		protected Object submitAsNewRequest(Object... args) throws RequestException {
			Request request = (Request)args[0];
			try {
                ContentSource contentSource = contentSourcePool.get();
                if (contentSource == null) {
                    throw new RequestException("No ContentSource available for retrying the request.", request);
                }
				retryProxy = contentSource.newSession();
				setAttemptsToNewSession(retryProxy);
				Request newRequest;
				if (request instanceof AdhocQuery) {
					newRequest = retryProxy.newAdhocQuery(((AdhocQuery)request).getQuery());
				} else {
					newRequest = retryProxy.newModuleInvoke(((ModuleInvoke)request).getModuleUri());
				}
				newRequest.setOptions(request.getOptions());
                for (XdmVariable xdmVariable: request.getVariables()) {
                    newRequest.setVariable(xdmVariable);
                }
				return retryProxy.submitRequest(newRequest);
			} catch (CorbException exc) {
				throw new RequestException(exc.getMessage(),request,exc);
			}
		}

		/**
		 * Retries a failed insertContent operation on a new Session from the pool.
		 * Propagates the attempt count to the new session proxy.
		 *
		 * @param args the original method arguments (Content or Content[] to insert)
		 * @return null (insertContent returns void)
		 * @throws RequestException if the retry fails or no ContentSource is available
		 */
		protected Object insertAsNewRequest(Object... args) throws RequestException {
			try {
                ContentSource contentSource = contentSourcePool.get();
                if (contentSource == null) {
                    throw new RequestException("No ContentSource available for retrying the insertContent operation.", target.newAdhocQuery(EMPTY_SEQ));
                }
				retryProxy = contentSource.newSession();
				setAttemptsToNewSession(retryProxy);
				if (args[0] instanceof Content) {
					retryProxy.insertContent((Content)args[0]);
				} else if (args[0] instanceof Content[]) {
					retryProxy.insertContent((Content[])args[0]);
				}
				return null;
			} catch (CorbException exc) {
				throw new RequestException(exc.getMessage(), target.newAdhocQuery(EMPTY_SEQ), exc);
			}
		}

		/**
		 * Checks if the method being invoked is submitRequest().
		 *
		 * @param method the method to check
		 * @return true if the method is submitRequest(), false otherwise
		 */
		private boolean isSubmitRequest(Method method) {
			return SUBMIT_REQUEST.equals(method.getName());
		}

		/**
		 * Checks if the method being invoked is insertContent().
		 *
		 * @param method the method to check
		 * @return true if the method is insertContent(), false otherwise
		 */
		private boolean isInsertContent(Method method) {
			return INSERT_CONTENT.equals(method.getName());
		}

		/**
		 * Checks if the method being invoked is close().
		 *
		 * @param method the method to check
		 * @return true if the method is close(), false otherwise
		 */
		private boolean isClose(Method method) {
			return CLOSE.equals(method.getName());
		}

		/**
		 * Checks if the method being invoked is unsupported.
		 * Throws UnsupportedOperationException for commit() and rollback().
		 *
		 * @param method the method to check
		 * @throws UnsupportedOperationException if the method is commit() or rollback()
		 */
		private void checkUnsupported(Method method) {
			if (COMMIT.equals(method.getName()) || ROLLBACK.equals(method.getName())) {
				throw new UnsupportedOperationException(method.getName() + " is not supported by " + getClass().getName());
			}
		}

		/**
		 * Propagates the attempt count to a retry session proxy.
		 * Ensures that retry operations maintain the same attempt count
		 * across failovers to different hosts.
		 *
		 * @param newProxy the new Session proxy to propagate attempts to
		 */
		protected void setAttemptsToNewSession(Session newProxy) {
			if (Proxy.isProxyClass(newProxy.getClass())) {
				InvocationHandler handler = Proxy.getInvocationHandler(newProxy);
				if (handler instanceof SessionInvocationHandler) {
					((SessionInvocationHandler)handler).attempts = this.attempts;
				}
			}
		}
	}
}
