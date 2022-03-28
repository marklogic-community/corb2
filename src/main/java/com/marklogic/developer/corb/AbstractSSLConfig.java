/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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

import static com.marklogic.developer.corb.Options.SSL_CIPHER_SUITES;
import static com.marklogic.developer.corb.Options.SSL_ENABLED_PROTOCOLS;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;

import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.SecurityOptions;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * AbstractSSLOptions has 3 abstract methods:
 * getSSLContext - build SSLContext with Truststore and Keystore
 * getEnabledCipherSuites - enabled cipher suites such as TLS_RSA_WITH_AES_256_CBC_SHA, ECDHE-RSA-AES128-SHA256"
 * getEnabledProtocols - ssl protocols such as TLSv1.2 (default)
 * @author rkennedy
 */
public abstract class AbstractSSLConfig implements SSLConfig {
	protected Properties properties;
	protected Decrypter decrypter;
	protected static final String DELIMITER = ",|:";
    protected static final String DEFAULT_PROTOCOL = "TLSv1.2";
    private static final Logger LOG = Logger.getLogger(AbstractSSLConfig.class.getName());

    @Override
	public void setProperties(Properties props){
		this.properties = props;
	}

    @Override
	public void setDecrypter(Decrypter decrypter) {
		this.decrypter = decrypter;
	}

    /**
     * Parse the list of {@link com.marklogic.developer.corb.Options#SSL_CIPHER_SUITES} values
     * @return acceptable list of cipher suites
     */
    public String[] getEnabledCipherSuites() {
        return getPropertyAndSplitToArray(SSL_CIPHER_SUITES);
    }

    /**
     * Parse the list of {@link com.marklogic.developer.corb.Options#SSL_ENABLED_PROTOCOLS} values
     * @return list of acceptable protocols
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
            LOG.log(Level.INFO, "No protocol configured, using default: {0}", DEFAULT_PROTOCOL);
            protocols = new String[] {DEFAULT_PROTOCOL};
        }
        return protocols;
    }

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

    protected SSLContext getSSLContextInstance(String[] protocols) throws NoSuchAlgorithmException {
        String head = protocols[0];
        try {
            SSLContext sslContext = SSLContext.getInstance(head);
            LOG.log(Level.INFO, "Using protocol: {0}", head);
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
	 * Returns SecurityOptions with configured protocol and cipher suites
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws KeyManagementException
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

	protected String getProperty(String key){
		String val = System.getProperty(key);
		if (properties != null && isBlank(val)) {
			val = properties.getProperty(key);
		}
		return trim(val);
	}
}
