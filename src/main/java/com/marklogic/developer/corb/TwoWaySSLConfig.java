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

import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.logging.Logger;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * SSL configuration implementation for two-way (mutual) SSL authentication.
 * <p>
 * Two-way SSL, also known as mutual SSL or client authentication, requires both the client
 * and server to authenticate each other using certificates. This class configures the SSL context
 * with both a keystore (for client certificates) and a truststore (for validating server certificates).
 * </p>
 * <p>
 * This implementation requires the following properties to be configured:
 * </p>
 * <ul>
 *   <li>{@link com.marklogic.developer.corb.Options#SSL_KEYSTORE} - Path to the client keystore file</li>
 *   <li>{@link com.marklogic.developer.corb.Options#SSL_KEYSTORE_PASSWORD} - Password for the keystore</li>
 *   <li>{@link com.marklogic.developer.corb.Options#SSL_KEYSTORE_TYPE} - Type of keystore (e.g., JKS, PKCS12)</li>
 *   <li>{@link com.marklogic.developer.corb.Options#SSL_KEY_PASSWORD} - Password for the private key (optional, defaults to keystore password)</li>
 * </ul>
 * <p>
 * Additionally, truststore properties inherited from {@link OneWaySSLConfig} are required for server certificate validation.
 * </p>
 * <p>
 * Supports password decryption via a configured {@link Decrypter} implementation.
 * </p>
 *
 * @since 2.2.0
 */
public class TwoWaySSLConfig extends AbstractSSLConfig {

    private static final Logger LOG = Logger.getLogger(TwoWaySSLConfig.class.getName());

    /**
     * Property name for SSL cipher suites configuration.
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_CIPHER_SUITES}.
     * @see com.marklogic.developer.corb.Options#SSL_CIPHER_SUITES
     */
    @Deprecated
    public static final String SSL_CIPHER_SUITES = com.marklogic.developer.corb.Options.SSL_CIPHER_SUITES;
    /**
     * Property name for SSL enabled protocols configuration.
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_ENABLED_PROTOCOLS}.
     * @see com.marklogic.developer.corb.Options#SSL_ENABLED_PROTOCOLS
     */
    @Deprecated
    public static final String SSL_ENABLED_PROTOCOLS = com.marklogic.developer.corb.Options.SSL_ENABLED_PROTOCOLS;
    /**
     * Property name for SSL keystore file path.
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_KEYSTORE}.
     * @see com.marklogic.developer.corb.Options#SSL_KEYSTORE
     */
    @Deprecated
    public static final String SSL_KEYSTORE = com.marklogic.developer.corb.Options.SSL_KEYSTORE;
    /**
     * Property name for SSL private key password.
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_KEY_PASSWORD}.
     * @see com.marklogic.developer.corb.Options#SSL_KEY_PASSWORD
     */
    @Deprecated
    public static final String SSL_KEY_PASSWORD = com.marklogic.developer.corb.Options.SSL_KEY_PASSWORD;
    /**
     * Property name for SSL keystore password.
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_KEYSTORE_PASSWORD}.
     * @see com.marklogic.developer.corb.Options#SSL_KEYSTORE_PASSWORD
     */
    @Deprecated
    public static final String SSL_KEYSTORE_PASSWORD = com.marklogic.developer.corb.Options.SSL_KEYSTORE_PASSWORD;
    /**
     * Property name for SSL keystore type (e.g., JKS, PKCS12).
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_KEYSTORE_TYPE}.
     * @see com.marklogic.developer.corb.Options#SSL_KEYSTORE_TYPE
     */
    @Deprecated
    public static final String SSL_KEYSTORE_TYPE = com.marklogic.developer.corb.Options.SSL_KEYSTORE_TYPE;
    /**
     * Property name for SSL properties file path.
     *
     * @deprecated This property is not necessary and will be removed in future versions.
     *             Instead directly reference {@link com.marklogic.developer.corb.Options#SSL_PROPERTIES_FILE}.
     * @see com.marklogic.developer.corb.Options#SSL_PROPERTIES_FILE
     */
    @Deprecated
    public static final String SSL_PROPERTIES_FILE = com.marklogic.developer.corb.Options.SSL_PROPERTIES_FILE;

    /**
     * Retrieves a required property value by name.
     * <p>
     * This method ensures that mandatory SSL configuration properties are provided.
     * If the property is not set or is empty, an exception is thrown.
     * </p>
     *
     * @param propertyName the name of the required property
     * @return the non-empty property value
     * @throws IllegalStateException if the property is not provided or is empty
     */
    private String getRequiredProperty(String propertyName) {
        String property = getProperty(propertyName);
        if (isNotEmpty(property)) {
            return property;
        } else {
            throw new IllegalStateException("Property " + propertyName + " is not provided and is required");
        }
    }

    /**
     * Creates and initializes an SSLContext configured for two-way SSL authentication.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Loads SSL properties from the configured properties file (if specified)</li>
     *   <li>Initializes KeyManagers from the configured keystore for client authentication</li>
     *   <li>Initializes TrustManagers from the configured truststore for server validation</li>
     *   <li>Creates an SSLContext with the specified protocols</li>
     *   <li>Initializes the SSLContext with both KeyManagers and TrustManagers</li>
     * </ol>
     *
     * @return a fully configured SSLContext for two-way SSL
     * @throws NoSuchAlgorithmException if the specified SSL algorithm is not available
     * @throws KeyManagementException if there is an error initializing the SSLContext
     * @throws IllegalStateException if there is an error loading keystores, truststores,
     *                               or if required properties are missing
     */
    @Override
    public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {

        loadPropertiesFile();
        try {
            KeyManager[] keyManagers = getKeyManagers();
            TrustManager[] trustManagers = getTrustManagers();

            SSLContext sslContext = getSSLContextInstance(getEnabledProtocols());
            sslContext.init(keyManagers, trustManagers, null);
            return sslContext;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create SSLContext in TwoWaySSLOptions", e);
        }
    }

    /**
     * Creates and initializes KeyManagers for client certificate authentication.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Retrieves keystore configuration properties (path, password, type)</li>
     *   <li>Retrieves or defaults the private key password</li>
     *   <li>Decrypts passwords using the configured {@link Decrypter} if available</li>
     *   <li>Loads the keystore from the specified file</li>
     *   <li>Initializes a KeyManagerFactory with the keystore</li>
     *   <li>Returns the configured KeyManagers</li>
     * </ol>
     * <p>
     * If the key password is not specified, it defaults to the keystore password.
     * </p>
     *
     * @return an array of KeyManagers initialized with the client keystore
     * @throws KeyStoreException if there is an error with the keystore type or operations
     * @throws NoSuchAlgorithmException if the keystore algorithm is not available
     * @throws IOException if there is an error reading the keystore file
     * @throws UnrecoverableKeyException if the key cannot be recovered from the keystore
     * @throws CertificateException if there is an error with the certificates in the keystore
     */
    private KeyManager[] getKeyManagers() throws KeyStoreException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException, CertificateException {
        String sslKeyStore = getRequiredProperty(SSL_KEYSTORE);
        String sslKeyStorePassword = getRequiredProperty(SSL_KEYSTORE_PASSWORD);
        String sslKeyPassword = getProperty(SSL_KEY_PASSWORD);
        if (isBlank(sslKeyPassword)) {
            sslKeyPassword = sslKeyStorePassword;
        }
        String sslKeyStoreType = getRequiredProperty(SSL_KEYSTORE_TYPE);
        // decrypting password values
        if (decrypter != null) {
            if (sslKeyStorePassword != null) {
                sslKeyStorePassword = decrypter.decrypt(SSL_KEYSTORE_PASSWORD, sslKeyStorePassword);
            }
            if (sslKeyPassword != null) {
                sslKeyPassword = decrypter.decrypt(SSL_KEY_PASSWORD, sslKeyPassword);
            }
        } else {
            LOG.info("Decrypter is not initialized");
        }
        // adding custom key store
        KeyStore clientKeyStore = KeyStore.getInstance(sslKeyStoreType);
        try (InputStream keystoreInputStream = new FileInputStream(sslKeyStore)) {
            char[] sslKeystorePasswordChars = sslKeyStorePassword != null ? sslKeyStorePassword.toCharArray() : null;
            clientKeyStore.load(keystoreInputStream, sslKeystorePasswordChars);
        }

        char[] sslKeyPasswordChars = sslKeyPassword != null ? sslKeyPassword.toCharArray() : null;
        // using SunX509 format
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeyStore, sslKeyPasswordChars);
        return keyManagerFactory.getKeyManagers();
    }
}
