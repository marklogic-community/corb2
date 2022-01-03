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

import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_HOST_RETRY_LIMIT;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
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
 * @since 2.4.0
 */
public abstract class AbstractContentSourcePool implements ContentSourcePool {
    protected static final int DEFAULT_CONNECTION_RETRY_INTERVAL = 60;
    protected static final int DEFAULT_CONNECTION_RETRY_LIMIT = 3;

    protected Properties properties;
    protected SSLConfig sslConfig;

    private static final Logger LOG = Logger.getLogger(AbstractContentSourcePool.class.getName());

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

    @Override
    public SSLConfig sslConfig() {
        return this.sslConfig;
    }

    protected SecurityOptions getSecurityOptions() throws KeyManagementException, NoSuchAlgorithmException {
        return this.sslConfig != null ? this.sslConfig.getSecurityOptions() : null;
    }

    protected int getConnectRetryLimit() {
        int connectRetryLimit = getIntProperty(XCC_CONNECTION_RETRY_LIMIT);
        return connectRetryLimit < 0 ? DEFAULT_CONNECTION_RETRY_LIMIT : connectRetryLimit;
    }

    protected int getConnectRetryInterval() {
        int connectRetryInterval = getIntProperty(XCC_CONNECTION_RETRY_INTERVAL);
        return connectRetryInterval < 0 ? DEFAULT_CONNECTION_RETRY_INTERVAL : connectRetryInterval;
    }

    protected int getConnectHostRetryLimit() {
        int connectHostRetryLimit = getIntProperty(XCC_CONNECTION_HOST_RETRY_LIMIT);
        return connectHostRetryLimit < 0 ? getConnectRetryLimit() : connectHostRetryLimit;
    }

    /**
     * Retrieves an int value.
     *
     * @param key The key name.
     * @return The requested value ({@code -1} if not found or could not parse
     * value as int).
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

    public String getProperty(String key) {
        String val = System.getProperty(key);
        if (val == null && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }

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

    protected ContentSource createContentSource(URI connectionUri) {
        ContentSource contentSource = null;
        boolean ssl = connectionUri.getScheme() != null && "xccs".equals(connectionUri.getScheme());
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
