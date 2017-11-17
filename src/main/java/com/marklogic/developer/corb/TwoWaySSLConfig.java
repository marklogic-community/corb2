/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
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
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 *
 * @since 2.2.0
 */
public class TwoWaySSLConfig extends AbstractSSLConfig {

    private static final Logger LOG = Logger.getLogger(TwoWaySSLConfig.class.getName());
    public static final String SSL_CIPHER_SUITES = com.marklogic.developer.corb.Options.SSL_CIPHER_SUITES;
    public static final String SSL_ENABLED_PROTOCOLS = com.marklogic.developer.corb.Options.SSL_ENABLED_PROTOCOLS;
    public static final String SSL_KEYSTORE = com.marklogic.developer.corb.Options.SSL_KEYSTORE;
    public static final String SSL_KEY_PASSWORD = com.marklogic.developer.corb.Options.SSL_KEY_PASSWORD;
    public static final String SSL_KEYSTORE_PASSWORD = com.marklogic.developer.corb.Options.SSL_KEYSTORE_PASSWORD;
    public static final String SSL_KEYSTORE_TYPE = com.marklogic.developer.corb.Options.SSL_KEYSTORE_TYPE;
    public static final String SSL_PROPERTIES_FILE = com.marklogic.developer.corb.Options.SSL_PROPERTIES_FILE;
    private static final String DELIMITER = ",";

    /**
     * @return acceptable list of cipher suites
     */
    @Override
    public String[] getEnabledCipherSuites() {
        if (properties != null) {
            String cipherSuites = properties.getProperty(SSL_CIPHER_SUITES);
            if (isNotEmpty(cipherSuites)) {
                String[] cipherSuitesList = cipherSuites.split(DELIMITER);
                LOG.log(Level.INFO, () -> MessageFormat.format("Using cipher suites: {0}", (Object[]) cipherSuitesList));
                return cipherSuitesList;
            }
        }
        return new String[]{};
    }

    /**
     * @return list of acceptable protocols
     */
    @Override
    public String[] getEnabledProtocols() {
        if (properties != null) {
            String enabledProtocols = properties.getProperty(SSL_ENABLED_PROTOCOLS);
            if (isNotEmpty(enabledProtocols)) {
                String[] enabledProtocolsList = enabledProtocols.split(DELIMITER);
                LOG.log(Level.INFO, () -> MessageFormat.format("Using enabled protocols: {0}", (Object[]) enabledProtocolsList));
                return enabledProtocolsList;
            }
        }
        return new String[]{};
    }

    private String getRequiredProperty(String propertyName) {
        String property = getProperty(propertyName);
        if (isNotEmpty(property)) {
            return property;
        } else {
            throw new IllegalStateException("Property " + propertyName + " is not provided and is required");
        }
    }

    /**
     * loads properties file and adds it to properties
     *
     */
    protected void loadPropertiesFile() {
        String securityFileName = getProperty(SSL_PROPERTIES_FILE);
        if (isNotBlank(securityFileName)) {
            File f = new File(securityFileName);
            if (f.exists() && !f.isDirectory()) {
                LOG.log(Level.INFO, () -> MessageFormat.format("Loading SSL configuration file {0} from filesystem", securityFileName));

                try (InputStream is = new FileInputStream(f)) {
                    if (properties == null) {
                        properties = new Properties();
                    }
                    properties.load(is);
                } catch (IOException e) {
                    LOG.log(Level.SEVERE, () -> MessageFormat.format("Error loading ssl properties file {0}", SSL_PROPERTIES_FILE));
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalStateException("Unable to load " + securityFileName);
            }
        } else {
            LOG.log(Level.INFO, () -> MessageFormat.format("Property {0} not present", SSL_PROPERTIES_FILE));
        }
    }

    @Override
    public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {

        loadPropertiesFile();

        String sslkeyStore = getRequiredProperty(SSL_KEYSTORE);
        String sslkeyStorePassword = getRequiredProperty(SSL_KEYSTORE_PASSWORD);
        String sslkeyPassword = getProperty(SSL_KEY_PASSWORD);
        if (isBlank(sslkeyPassword)) {
            sslkeyPassword = sslkeyStorePassword;
        }
        String sslkeyStoreType = getRequiredProperty(SSL_KEYSTORE_TYPE);
        // decrypting password values
        if (decrypter != null) {
            if (sslkeyStorePassword != null) {
                sslkeyStorePassword = decrypter.decrypt(SSL_KEYSTORE_PASSWORD, sslkeyStorePassword);
            }
            if (sslkeyPassword != null) {
                sslkeyPassword = decrypter.decrypt(SSL_KEY_PASSWORD, sslkeyPassword);
            }
        } else {
            LOG.info("Decrypter is not initialized");
        }
        try {
            // adding default trust store
            TrustManager[] trust = null;

            // adding custom key store
            KeyStore clientKeyStore = KeyStore.getInstance(sslkeyStoreType);
            char[] sslkeyStorePasswordChars = sslkeyStorePassword != null ? sslkeyStorePassword.toCharArray() : null;

            try (InputStream keystoreInputStream = new FileInputStream(sslkeyStore)) {
                clientKeyStore.load(keystoreInputStream, sslkeyStorePasswordChars);
            }
            char[] sslkeyPasswordChars = sslkeyPassword != null ? sslkeyPassword.toCharArray() : null;
            // using SunX509 format
            KeyManagerFactory keyManagerFactory = KeyManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, sslkeyPasswordChars);
            KeyManager[] key = keyManagerFactory.getKeyManagers();
            SSLContext sslContext = SSLContext.getInstance("TLSv1");
            sslContext.init(key, trust, null);
            return sslContext;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create SSLContext in TwoWaySSLOptions", e);
        }
    }
}
