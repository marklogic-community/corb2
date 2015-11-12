package com.marklogic.developer.corb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class TwoWaySSLOptions extends AbstractSSLOptions {

	private static final Logger LOG = Logger.getLogger(TwoWaySSLOptions.class.getSimpleName());

	/**
	 * @return acceptable list of cipher suites
	 */
	@Override
	public String[] getEnabledCipherSuites() {
		String cipherSuites = properties.getProperty("SSL-CIPHER-SUITES");
		if (cipherSuites != null && !cipherSuites.isEmpty()) {
			String[] cipherSuitesList = cipherSuites.split(",");
			LOG.info("Using cipher suites: " + cipherSuitesList);
			return cipherSuitesList;
		}
		return null;
	}

	/**
	 * @return list of acceptable protocols
	 */
	@Override
	public String[] getEnabledProtocols() {
		String enabledProtocols = properties.getProperty("SSL-ENABLED-PROTOCOLS");
		if (enabledProtocols != null && !enabledProtocols.isEmpty()) {
			String[] enabledProtocolsList = enabledProtocols.split(",");
			LOG.info("Using enabled protocols: "+enabledProtocolsList);
			return enabledProtocolsList;
		}
		return null;
	}

	private String getRequiredProperty(String propertyName) {
		String property = getProperty(propertyName);
		if (property != null && property.length() != 0) {
			return property;
		} else {
			throw new IllegalStateException("Property " + propertyName + " is not provided and is required");
		}
	}

	/**
	 * loads properties file and adds it to properties 
	 * @throws IOException
	 */
	protected void loadPropertiesFile() throws IOException {
		String securityFileName = getProperty("SSL-PROPERTIES-FILE");
		if (securityFileName != null && securityFileName.trim().length() != 0) {
			File f = new File(securityFileName);
			if (f.exists() && !f.isDirectory()) {
				LOG.info("Loading SSL configuration file " + securityFileName + " from filesystem");
				InputStream is = null;
				try {
					is = new FileInputStream(f);
					properties.load(is);
				} catch (IOException e) {
					LOG.severe("Error loading ssl properties file");
					throw new RuntimeException(e);
				} finally {
					if (is != null) {
						is.close();
					}
				}

			} else {
				throw new IllegalStateException("Unable to load " + securityFileName);
			}
		} else {
			LOG.info("Property SSL-PROPERTIES-FILE not present");
		}
	}
	
	
	@Override
	public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
		try {
			loadPropertiesFile();
		} catch (IOException e1) {
			LOG.severe("Error loading SSL-PROPERTIES-FILE");
			throw new RuntimeException(e1);
		}

		String sslkeyStore = getRequiredProperty("SSL-KEYSTORE");
		String sslkeyStorePassword = getRequiredProperty("SSL-KEYSTORE-PASSWORD");
		String sslkeyPassword = getRequiredProperty("SSL-KEY-PASSWORD");
		String sslkeyStoreType = getRequiredProperty("SSL-KEYSTORE-TYPE");
		// decrypting password values
		if (decrypter != null) {
			if (sslkeyStorePassword != null) {
				sslkeyStorePassword = decrypter.decrypt("SSL-KEYSTORE-PASSWORD", sslkeyStorePassword);
			}
			if (sslkeyPassword != null) {
				sslkeyPassword = decrypter.decrypt("SSL-KEY-PASSWORD", sslkeyPassword);
			}
		} else {
			LOG.info("Decrypter is not initialized");
		}
		try {
			// adding default trust store
			TrustManager[] trust = null;

			// adding custom key store
			KeyStore clientKeyStore = KeyStore.getInstance(sslkeyStoreType);
			clientKeyStore.load(new FileInputStream(sslkeyStore), sslkeyStorePassword.toCharArray());
			// using SunX509 format
			KeyManagerFactory keyManagerFactory = KeyManagerFactory
					.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(clientKeyStore, sslkeyPassword.toCharArray());
			KeyManager[] key = keyManagerFactory.getKeyManagers();
			SSLContext sslContext = SSLContext.getInstance("TLSv1");
			sslContext.init(key, trust, null);
			return sslContext;
		} catch (Exception e) {
			throw new IllegalStateException("Unable to create SSLContext in TwoWaySSLOptions", e);
		}

	}
}
