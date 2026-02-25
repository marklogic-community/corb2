/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import com.marklogic.xcc.SecurityOptions;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import javax.net.ssl.SSLContext;

/**
 * Interface for configuring SSL/TLS connections to MarkLogic.
 * <p>
 * SSLConfig defines the contract for SSL configuration implementations that can be
 * used by CoRB to establish secure connections to MarkLogic servers. Implementations
 * provide:
 * </p>
 * <ul>
 * <li>SSL context configuration (protocols, cipher suites, trust/key managers)</li>
 * <li>Integration with XCC SecurityOptions</li>
 * <li>Property-based configuration</li>
 * <li>Support for encrypted passwords via Decrypter</li>
 * </ul>
 * <p>
 * CoRB includes several built-in implementations:
 * </p>
 * <ul>
 * <li>{@link TrustAnyoneSSLConfig} - Trusts all certificates (default for xccs:// URIs)</li>
 * <li>{@link OneWaySSLConfig} - Standard server authentication with optional custom TrustStore</li>
 * <li>{@link TwoWaySSLConfig} - Mutual authentication (client and server certificates)</li>
 * <li>{@link HostKeyDecrypter} - Uses host-based key derivation</li>
 * </ul>
 * <p>
 * Custom implementations can be specified via the {@link Options#SSL_CONFIG_CLASS} option.
 * The implementation should read configuration from properties set via {@link #setProperties(Properties)}
 * and typically use properties prefixed with "SSL-CONFIG-CLASS.".
 * </p>
 * <p>
 * Common configuration properties (used by built-in implementations):
 * </p>
 * <ul>
 * <li>{@link Options#SSL_ENABLED_PROTOCOLS} - TLS versions to enable (e.g., "TLSv1.2,TLSv1.3")</li>
 * <li>{@link Options#SSL_CIPHER_SUITES} - Cipher suites to enable</li>
 * <li>{@link Options#SSL_KEYSTORE} - Path to keystore file (for client certificates)</li>
 * <li>{@link Options#SSL_KEYSTORE_PASSWORD} - Keystore password (encryptable)</li>
 * <li>{@link Options#SSL_KEY_PASSWORD} - Private key password (encryptable)</li>
 * <li>{@link Options#SSL_TRUSTSTORE} - Path to custom truststore file</li>
 * <li>{@link Options#SSL_TRUSTSTORE_PASSWORD} - Truststore password (encryptable)</li>
 * </ul>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * SSL-CONFIG-CLASS=com.marklogic.developer.corb.TwoWaySSLConfig
 * SSL-CONFIG-CLASS.SSL-KEYSTORE=/path/to/keystore.jks
 * SSL-CONFIG-CLASS.SSL-KEYSTORE-PASSWORD=ENC(encrypted_password)
 * SSL-CONFIG-CLASS.SSL-ENABLED-PROTOCOLS=TLSv1.2,TLSv1.3
 * </pre>
 *
 * @author MarkLogic Corporation
 * @since 2.2.0
 * @see AbstractSSLConfig
 * @see TrustAnyoneSSLConfig
 * @see OneWaySSLConfig
 * @see TwoWaySSLConfig
 * @see Options#SSL_CONFIG_CLASS
 */
public interface SSLConfig {
	/**
	 * Sets the properties for configuring SSL/TLS.
	 * <p>
	 * The properties typically include SSL-related configuration such as keystore
	 * paths, passwords, enabled protocols, and cipher suites. Properties are usually
	 * prefixed with "SSL-CONFIG-CLASS." to distinguish them from other CoRB options.
	 * </p>
	 * <p>
	 * Implementations should store these properties and use them when initializing
	 * the SSLContext via {@link #getSSLContext()}.
	 * </p>
	 *
	 * @param props the properties containing SSL configuration
	 */
	void setProperties(Properties props);
	/**
	 * Sets the decrypter for handling encrypted SSL-related passwords.
	 * <p>
	 * The decrypter is used to decrypt sensitive values such as keystore passwords,
	 * truststore passwords, and private key passwords. This allows encrypted values
	 * to be stored in configuration files and decrypted at runtime.
	 * </p>
	 * <p>
	 * Implementations should store the decrypter and use it to decrypt any
	 * password-related properties before using them to load keystores or
	 * initialize SSL contexts.
	 * </p>
	 *
	 * @param decrypter the decrypter for handling encrypted passwords
	 * @see Decrypter
	 * @see Options#DECRYPTER
	 */
	void setDecrypter(Decrypter decrypter);
	/**
	 * Gets the list of enabled SSL/TLS cipher suites.
	 * <p>
	 * Cipher suites determine the encryption algorithms used for the SSL/TLS connection.
	 * If not configured, implementations may return null to use the JVM defaults.
	 * </p>
	 * <p>
	 * Common cipher suites include:
     * </p>
	 * <ul>
	 * <li>TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384</li>
	 * <li>TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256</li>
	 * <li>TLS_DHE_RSA_WITH_AES_256_GCM_SHA384</li>
	 * </ul>
	 *
	 * @return array of enabled cipher suite names, or null for defaults
	 * @see Options#SSL_CIPHER_SUITES
	 */
	String[] getEnabledCipherSuites();
	/**
	 * Gets the list of enabled SSL/TLS protocols.
	 * <p>
	 * Protocols determine which versions of TLS/SSL are allowed for connections.
	 * If not configured, implementations may return null to use the JVM defaults.
	 * </p>
	 * <p>
	 * Recommended protocols:
     * </p>
	 * <ul>
	 * <li>TLSv1.3 (most secure, if supported)</li>
	 * <li>TLSv1.2 (widely supported, secure)</li>
	 * <li>TLSv1.1 and TLSv1.0 (deprecated, should be avoided)</li>
	 * </ul>
	 *
	 * @return array of enabled protocol names, or null for defaults
	 * @see Options#SSL_ENABLED_PROTOCOLS
	 */
	String[] getEnabledProtocols();
	/**
	 * Creates and configures an SSLContext for secure connections.
	 * <p>
	 * The SSLContext is initialized with:
     * </p>
	 * <ul>
	 * <li>KeyManager(s) - for client authentication (if applicable)</li>
	 * <li>TrustManager(s) - for validating server certificates</li>
	 * <li>SecureRandom - for cryptographic operations</li>
	 * </ul>
	 * <p>
	 * The implementation should use properties set via {@link #setProperties(Properties)}
	 * to configure keystores, truststores, and other SSL parameters.
	 * </p>
	 * <p>
	 * This method is called when establishing XCC connections using the xccs:// protocol.
	 * </p>
	 *
	 * @return a configured SSLContext ready for use
	 * @throws NoSuchAlgorithmException if the specified SSL/TLS protocol is not available
	 * @throws KeyManagementException if the SSLContext cannot be initialized
	 */
	SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException;
	/**
	 * Creates XCC SecurityOptions for establishing secure MarkLogic connections.
	 * <p>
	 * SecurityOptions encapsulate the SSL configuration in a form that XCC can use
	 * when connecting to MarkLogic. The SecurityOptions typically include:
     * </p>
	 * <ul>
	 * <li>The SSLContext from {@link #getSSLContext()}</li>
	 * <li>Enabled protocols from {@link #getEnabledProtocols()}</li>
	 * <li>Enabled cipher suites from {@link #getEnabledCipherSuites()}</li>
	 * </ul>
	 * <p>
	 * This is the method called by CoRB to obtain SSL configuration for XCC connections.
	 * </p>
	 *
	 * @return XCC SecurityOptions configured for secure connections
	 * @throws NoSuchAlgorithmException if the specified SSL/TLS protocol is not available
	 * @throws KeyManagementException if the SSLContext cannot be initialized
	 * @see com.marklogic.xcc.SecurityOptions
	 */
	SecurityOptions getSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException;
}
