/*
 * * Copyright (c) 2004-2025 MarkLogic Corporation
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * SSL configuration for one-way (server-only) SSL/TLS authentication.
 * <p>
 * OneWaySSLConfig provides standard SSL/TLS connectivity where:
 * </p>
 * <ul>
 * <li>The client validates the server's certificate (one-way authentication)</li>
 * <li>The server does not require a client certificate</li>
 * <li>A custom TrustStore can be configured to validate server certificates</li>
 * </ul>
 * <p>
 * This is the most common SSL configuration for client applications connecting
 * to secure servers. The client trusts the server's certificate either through:
 * </p>
 * <ul>
 * <li>The default JVM trust store (cacerts)</li>
 * <li>A custom trust store specified via SSL configuration properties</li>
 * </ul>
 * <p>
 * Configuration is typically provided through properties prefixed with {@code SSL-CONFIG-CLASS.}.
 * See {@link AbstractSSLConfig} for available configuration options.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * # Configure one-way SSL
 * SSL-CONFIG-CLASS=com.marklogic.developer.corb.OneWaySSLConfig
 * SSL-CONFIG-CLASS.SSL-PROPERTIES-FILE=ssl.properties
 *
 * # In ssl.properties:
 * SSL-TRUSTSTORE=/path/to/truststore.jks
 * SSL-TRUSTSTORE-PASSWORD=changeit
 * SSL-ENABLED-PROTOCOLS=TLSv1.2,TLSv1.3
 * </pre>
 *
 * @since 2.5.7
 * @see AbstractSSLConfig
 * @see TwoWaySSLConfig
 * @see TrustAnyoneSSLConfig
 */
public class OneWaySSLConfig extends AbstractSSLConfig {

    /**
     * Creates and configures an SSLContext for one-way SSL/TLS authentication.
     * <p>
     * The method performs the following steps:
     * </p>
     * <ol>
     * <li>Loads SSL properties from the configured properties file</li>
     * <li>Initializes trust managers (using custom TrustStore if configured)</li>
     * <li>Creates an SSLContext with the configured protocols</li>
     * <li>Initializes the SSLContext with trust managers (no key managers)</li>
     * </ol>
     * <p>
     * The returned SSLContext is configured to validate server certificates but
     * does not provide client certificates. This is suitable for most client
     * applications connecting to SSL-enabled MarkLogic servers.
     * </p>
     *
     * @return configured SSLContext for one-way SSL
     * @throws NoSuchAlgorithmException if the specified SSL/TLS protocol is not available
     * @throws KeyManagementException if the SSLContext cannot be initialized
     */
    @Override
    public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        loadPropertiesFile();
        TrustManager[] trustManager = getTrustManagers();
        SSLContext sslContext = getSSLContextInstance(getEnabledProtocols());
        sslContext.init(null, trustManager, null);
        return sslContext;
    }

}
