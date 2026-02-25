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

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.util.StringUtils.*;

import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.SecurityOptions;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract base class for SSL configuration implementations.
 * Provides common functionality for configuring SSL/TLS connections including
 * cipher suites, protocols, trust managers, and truststore loading.
 * Subclasses must implement {@link SSLConfig#getSSLContext()} to provide
 * the specific SSL context configuration with keystores and truststore.
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Configurable cipher suites (e.g., TLS_RSA_WITH_AES_256_CBC_SHA, ECDHE-RSA-AES128-SHA256)</li>
 *   <li>Configurable SSL/TLS protocols (defaults to TLSv1.2)</li>
 *   <li>Custom truststore support with password decryption</li>
 *   <li>Properties file loading for SSL configuration</li>
 * </ul>
 *
 * @author rkennedy
 */
public abstract class AbstractSSLConfig implements SSLConfig {
	/**
	 * Configuration properties containing SSL settings such as cipher suites, protocols,
	 * truststore location, and other security parameters.
	 */
	protected Properties properties;

	/**
	 * Decrypter instance for decrypting sensitive configuration values such as
	 * passwords for keystores and truststores.
	 */
	protected Decrypter decrypter;

	/**
	 * Regular expression delimiter pattern used to split comma or colon-separated values.
	 * Used for parsing cipher suites and protocol lists.
	 */
	protected static final String DELIMITER = ",|:";

	/**
	 * Default SSL/TLS protocol to use when no protocol is explicitly configured.
	 * TLSv1.2 is the minimum recommended protocol for secure connections.
	 */
    protected static final String DEFAULT_PROTOCOL = "TLSv1.2";

    /**
     * Logger instance for logging SSL configuration and diagnostic messages.
     */
    private static final Logger LOG = Logger.getLogger(AbstractSSLConfig.class.getName());

    /**
     * Sets the configuration properties.
     *
     * @param props the Properties object containing SSL configuration
     */
    @Override
	public void setProperties(Properties props){
		this.properties = props;
	}

    /**
     * Sets the decrypter for decrypting sensitive configuration values.
     *
     * @param decrypter the Decrypter instance
     */
    @Override
	public void setDecrypter(Decrypter decrypter) {
		this.decrypter = decrypter;
	}

    /**
     * Parses and returns the list of enabled cipher suites from {@link Options#SSL_CIPHER_SUITES}.
     * The cipher suites can be comma or colon-separated.
     *
     * @return array of enabled cipher suites, or empty array if not configured
     */
    public String[] getEnabledCipherSuites() {
        return getPropertyAndSplitToArray(SSL_CIPHER_SUITES);
    }

    /**
     * Parses and returns the list of enabled SSL/TLS protocols from {@link Options#SSL_ENABLED_PROTOCOLS}.
     * Falls back to the JVM property "jdk.tls.client.protocols" if not configured.
     * Defaults to TLSv1.2 if no protocols are configured.
     *
     * @return array of enabled protocols, defaults to ["TLSv1.2"] if not configured
     */
    public String[] getEnabledProtocols() {
        String[] protocols = getPropertyAndSplitToArray(SSL_ENABLED_PROTOCOLS);
        if (protocols.length == 0){
            protocols = StringUtils.split(System.getProperty("jdk.tls.client.protocols"), DELIMITER);
            List<String> protocolList = new ArrayList<>(Arrays.asList(protocols));
            protocolList.removeAll(Arrays.asList("", null));
            protocols = protocolList.toArray(new String[protocolList.size()]);
        }
        if (protocols.length == 0) {
            LOG.log(Level.FINE, "No protocol configured, using default: {0}", DEFAULT_PROTOCOL);
            protocols = new String[] {DEFAULT_PROTOCOL};
        }
        return protocols;
    }

    /**
     * Loads and configures trust managers from a custom truststore.
     * If {@link Options#SSL_TRUSTSTORE} is configured, loads the specified truststore file.
     * Otherwise, returns null to use the default JRE truststore.
     * Supports password decryption if a decrypter is configured.
     *
     * @return array containing a single X509TrustManager, or null to use default JRE truststore
     * @throws NoSuchAlgorithmException if the trust manager algorithm is not available
     * @throws IllegalStateException if the truststore cannot be loaded or is invalid
     */
    public TrustManager[] getTrustManagers() throws NoSuchAlgorithmException {
        TrustManager[] trustManagers = null;
        String trustStoreFile = getProperty(SSL_TRUSTSTORE);

        if (StringUtils.isNotBlank(trustStoreFile)) {
            try (FileInputStream customTrust = new FileInputStream(trustStoreFile)) {

                String keystoreType = getProperty(SSL_TRUSTSTORE_TYPE);
                if (StringUtils.isBlank(keystoreType)) {
                    keystoreType = KeyStore.getDefaultType();
                }

                String trustStorePassword = getProperty(SSL_TRUSTSTORE_PASSWORD);
                if (decrypter != null && trustStorePassword != null) {
                    trustStorePassword = decrypter.decrypt(SSL_TRUSTSTORE_PASSWORD, trustStorePassword);
                }
                char[] trustStorePasswordChars = trustStorePassword != null ? trustStorePassword.toCharArray() : null;

                KeyStore trustStore = KeyStore.getInstance(keystoreType);
                trustStore.load(customTrust, trustStorePasswordChars);

                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);

                for (TrustManager manager : trustManagerFactory.getTrustManagers()) {
                    if (manager instanceof X509TrustManager) {
                        trustManagers = new TrustManager[]{ manager };
                        break;
                    }
                }
            } catch (CertificateException | IOException | KeyStoreException e) {
                LOG.log(Level.SEVERE, "Unable to load custom truststore: " + trustStoreFile, e);
                throw new IllegalStateException("Unable to create TrustManager from truststore: " + trustStoreFile, e);
            }
        }
        return trustManagers;
    }

    /**
     * Retrieves a property value, splits it by delimiter, and returns as an array.
     * Logs the configured values if found.
     *
     * @param propertyName the property name to retrieve
     * @return array of trimmed values, or empty array if property is not set
     */
    private String[] getPropertyAndSplitToArray(String propertyName) {
        if (properties != null) {
            String values = properties.getProperty(propertyName);
            String[] valueArray = StringUtils.split(values, DELIMITER);
            if (valueArray.length > 0) {
                LOG.log(Level.INFO, "Configured {0}: {1}", new Object[]{propertyName, Arrays.toString(valueArray)});
                return valueArray;
            }
        }
        return new String[]{};
    }

    /**
     * Gets an SSLContext instance using the first available protocol from the array.
     * Recursively tries each protocol in the array until one succeeds.
     * This allows fallback to alternative protocols if the preferred one is not available.
     *
     * @param protocols array of protocol names to try (e.g., ["TLSv1.3", "TLSv1.2"])
     * @return an SSLContext instance for the first available protocol
     * @throws NoSuchAlgorithmException if none of the protocols are available
     */
    protected SSLContext getSSLContextInstance(String[] protocols) throws NoSuchAlgorithmException {
        String head = protocols[0];
        try {
            SSLContext sslContext = SSLContext.getInstance(head);
            LOG.log(Level.FINE, "Using protocol: {0}", head);
            return sslContext;
        } catch (NoSuchAlgorithmException ex) {
            LOG.log(Level.WARNING, "No such algorithm: {0}", protocols[0]);
            if (protocols.length == 1) {
                throw ex;
            } else {
                String[] tail = Arrays.copyOfRange(protocols, 1, protocols.length);
                return getSSLContextInstance(tail);
            }
        }
    }

    /**
     * Creates and returns SecurityOptions with configured SSL context, protocols, and cipher suites.
     * This is the main entry point for getting SSL security configuration.
     *
     * @return SecurityOptions configured with SSL context, enabled protocols, and cipher suites
     * @throws NoSuchAlgorithmException if the SSL algorithm is not available
     * @throws KeyManagementException if the key management initialization fails
     */
    @Override
	public SecurityOptions getSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException {
		SecurityOptions securityOptions = new SecurityOptions(getSSLContext());
		String[] enabledCipherSuites = getEnabledCipherSuites();
		if (enabledCipherSuites != null && enabledCipherSuites.length > 0) {
			securityOptions.setEnabledCipherSuites(enabledCipherSuites);
		}
		String[] enabledProtocols = getEnabledProtocols();
		if (enabledProtocols != null && enabledProtocols.length > 0) {
			securityOptions.setEnabledProtocols(enabledProtocols);
		}
		return securityOptions;
	}

    /**
     * Loads SSL configuration properties from the file specified by {@link Options#SSL_PROPERTIES_FILE}.
     * The file must exist on the filesystem.
     * Loaded properties are merged into the existing properties object.
     *
     * @throws IllegalStateException if the specified file does not exist
     * @throws RuntimeException if an I/O error occurs reading the file
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

    /**
     * Retrieves a property value by key.
     * First checks system properties, then falls back to instance properties.
     * The returned value is trimmed of leading and trailing whitespace.
     *
     * @param key the property key name
     * @return the trimmed property value, or null if not found
     */
	protected String getProperty(String key){
		String val = System.getProperty(key);
		if (properties != null && isBlank(val)) {
			val = properties.getProperty(key);
		}
		return trim(val);
	}
}
