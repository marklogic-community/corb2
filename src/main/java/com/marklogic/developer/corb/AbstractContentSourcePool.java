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

import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_HOST_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.CONTENT_SOURCE_RENEW;
import static com.marklogic.developer.corb.Options.CONTENT_SOURCE_RENEW_INTERVAL;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.Logger;

import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * Abstract base class for content source pool implementations.
 * Provides common functionality for managing XCC ContentSource connections to MarkLogic,
 * including connection retry logic, SSL configuration, and content source renewal.
 * Subclasses must implement the specific pooling strategy.
 *
 * @since 2.4.0
 */
public abstract class AbstractContentSourcePool implements ContentSourcePool {
    /**
     * Default interval in seconds to wait between connection retry attempts.
     * Used when {@link Options#XCC_CONNECTION_RETRY_INTERVAL} is not specified.
     */
    protected static final int DEFAULT_CONNECTION_RETRY_INTERVAL = 60;

    /**
     * Default maximum number of connection retry attempts.
     * Used when {@link Options#XCC_CONNECTION_RETRY_LIMIT} is not specified.
     */
    protected static final int DEFAULT_CONNECTION_RETRY_LIMIT = 3;

    /**
     * Default interval in seconds between content source renewal attempts.
     * Used when {@link Options#CONTENT_SOURCE_RENEW_INTERVAL} is not specified.
     */
    protected static final int DEFAULT_CONTENT_SOURCE_RENEW_INTERVAL = 60;

    /**
     * Configuration properties containing connection settings, retry limits,
     * renewal intervals, and other pool configuration parameters.
     */
    protected Properties properties;

    /**
     * SSL configuration for secure XCC connections.
     * Includes cipher suites, protocols, keystores, and truststores for xccs:// connections.
     */
    protected SSLConfig sslConfig;

    /**
     * Logger instance for logging pool operations, connection errors, and diagnostic information.
     */
    private static final Logger LOG = Logger.getLogger(AbstractContentSourcePool.class.getName());

    /**
     * Initializes the content source pool with configuration properties and SSL settings.
     * If properties are null, an empty Properties object is used.
     * If sslConfig is null, a TrustAnyoneSSLConfig is used as default.
     *
     * @param properties configuration properties for the pool, may be null
     * @param sslConfig SSL configuration for secure connections, may be null
     */
    protected void init(Properties properties, SSLConfig sslConfig) {
        if (properties != null) {
            this.properties = properties;
        } else {
            this.properties = new Properties();
            LOG.warning("Attempt to initialize with null properties. Using empty properties");
        }

        if (sslConfig != null) {
            this.sslConfig = sslConfig;
        } else {
            this.sslConfig = new TrustAnyoneSSLConfig();
            LOG.info("Using TrustAnyoneSSSLConfig as sslConfig is null.");
        }
    }

    /**
     * Returns the SSL configuration used by this content source pool.
     *
     * @return the SSLConfig instance
     */
    @Override
    public SSLConfig sslConfig() {
        return this.sslConfig;
    }

    /**
     * Retrieves the SecurityOptions from the SSL configuration.
     *
     * @return the SecurityOptions for XCC connections, or null if no SSL config is set
     * @throws KeyManagementException if the security configuration cannot be initialized
     * @throws NoSuchAlgorithmException if the SSL algorithm is not available
     */
    protected SecurityOptions getSecurityOptions() throws KeyManagementException, NoSuchAlgorithmException {
        return this.sslConfig != null ? this.sslConfig.getSecurityOptions() : null;
    }

    /**
     * Determines whether content sources should be automatically renewed.
     * Reads the CONTENT_SOURCE_RENEW property, defaulting to true if not set.
     *
     * @return true if content sources should be renewed, false otherwise
     */
    protected boolean shouldRenewContentSource() {
        return stringToBoolean(getProperty(CONTENT_SOURCE_RENEW), true);
    }

    /**
     * Gets the maximum number of connection retry attempts.
     * Reads the XCC_CONNECTION_RETRY_LIMIT property.
     *
     * @return the connection retry limit, or DEFAULT_CONNECTION_RETRY_LIMIT if not set or negative
     */
    protected int getConnectRetryLimit() {
        int connectRetryLimit = getIntProperty(XCC_CONNECTION_RETRY_LIMIT);
        return connectRetryLimit < 0 ? DEFAULT_CONNECTION_RETRY_LIMIT : connectRetryLimit;
    }

    /**
     * Gets the interval in seconds between content source renewal attempts.
     * Reads the CONTENT_SOURCE_RENEW_INTERVAL property.
     *
     * @return the renewal interval in seconds, or DEFAULT_CONTENT_SOURCE_RENEW_INTERVAL if not set or negative
     */
    protected int getRenewContentSourceInterval() {
        int interval = getIntProperty(CONTENT_SOURCE_RENEW_INTERVAL);
        return interval < 0 ? DEFAULT_CONTENT_SOURCE_RENEW_INTERVAL : interval;
    }
    /**
     * Gets the interval in seconds between connection retry attempts.
     * Reads the XCC_CONNECTION_RETRY_INTERVAL property.
     *
     * @return the retry interval in seconds, or DEFAULT_CONNECTION_RETRY_INTERVAL if not set or negative
     */
    protected int getConnectRetryInterval() {
        int connectRetryInterval = getIntProperty(XCC_CONNECTION_RETRY_INTERVAL);
        return connectRetryInterval < 0 ? DEFAULT_CONNECTION_RETRY_INTERVAL : connectRetryInterval;
    }

    /**
     * Gets the maximum number of connection retry attempts per host.
     * Reads the XCC_CONNECTION_HOST_RETRY_LIMIT property.
     * Falls back to the general connection retry limit if not set.
     *
     * @return the host-specific retry limit, or the general retry limit if not set or negative
     */
    protected int getConnectHostRetryLimit() {
        int connectHostRetryLimit = getIntProperty(XCC_CONNECTION_HOST_RETRY_LIMIT);
        return connectHostRetryLimit < 0 ? getConnectRetryLimit() : connectHostRetryLimit;
    }

    /**
     * Retrieves an integer property value by key.
     * First checks system properties, then falls back to instance properties.
     *
     * @param key the property key name
     * @return the integer value, or {@code -1} if not found or cannot be parsed as an int
     */
    protected int getIntProperty(String key) {
        int intVal = -1;
        String value = getProperty(key);
        if (isNotEmpty(value)) {
            try {
                intVal = Integer.parseInt(value);
            } catch (NumberFormatException exc) {
                LOG.log(WARNING, MessageFormat.format("Unable to parse `{0}` value `{1}` as an int", key, value), exc);
            }
        }
        return intVal;
    }

    /**
     * Retrieves a string property value by key.
     * First checks system properties, then falls back to instance properties.
     * The returned value is trimmed of leading and trailing whitespace.
     *
     * @param key the property key name
     * @return the trimmed property value, or null if not found
     */
    public String getProperty(String key) {
        String val = System.getProperty(key);
        if (val == null && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }

    /**
     * Creates a ContentSource from an XCC connection string.
     * Parses the connection string as a URI and delegates to {@link #createContentSource(URI)}.
     * If the URI is invalid, attempts to sanitize the connection string by removing credentials
     * before logging the error.
     *
     * @param connectionString the XCC connection string (e.g., "xcc://user:password@host:port/database")
     * @return the ContentSource instance, or null if creation fails
     */
    protected ContentSource createContentSource(String connectionString) {
        if (StringUtils.isNotBlank(connectionString)) {
            try {
                URI connectionUri = new URI(connectionString);
                return createContentSource(connectionUri);
            } catch (URISyntaxException ex) {
                //attempt to strip off credential info if we can
                int hostIndex = connectionString.lastIndexOf('@') + 1;
                connectionString = hostIndex > 1 && connectionString.length() > hostIndex ? connectionString.substring(hostIndex) : connectionString;
                LOG.log(SEVERE, "XCC URI is invalid " + connectionString, ex);
            }
        }
        return null;
    }

    /**
     * Creates a ContentSource from an XCC connection URI.
     * Automatically detects SSL connections (xccs:// scheme) and applies appropriate security options.
     * Handles various exceptions during content source creation and logs appropriate error messages.
     *
     * @param connectionUri the XCC connection URI
     * @return the ContentSource instance, or null if creation fails
     */
    protected ContentSource createContentSource(URI connectionUri) {
        ContentSource contentSource = null;
        String scheme = connectionUri.getScheme();
        boolean ssl = "xccs".equals(scheme);
        String hostname = connectionUri.getHost();
        String port = String.valueOf(connectionUri.getPort());
        String path = connectionUri.getPath();
        try {
            contentSource = ssl ? ContentSourceFactory.newContentSource(connectionUri, getSecurityOptions())
                : ContentSourceFactory.newContentSource(connectionUri);
        } catch (XccConfigException ex) {
            LOG.log(SEVERE, "Problem creating content source. Check if URI is valid. If encrypted, check if options are configured correctly for host " + hostname + ":" + port + path, ex);
        } catch (KeyManagementException | NoSuchAlgorithmException ex) {
            LOG.log(SEVERE, "Problem creating content source with ssl for host " + hostname + ":" + port + path, ex);
        } catch (IllegalArgumentException | IllegalStateException ex) {
            LOG.log(SEVERE, "XCC URI is invalid for host " + hostname + ":" + port + path, ex);
        }
        return contentSource;
    }
}
