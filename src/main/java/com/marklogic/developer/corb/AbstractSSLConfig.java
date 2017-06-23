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
import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.SecurityOptions;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * AbstractSSLOptions has 3 abstract methods:
 * getSSLContext - build SSLContext with Truststore and Keystore
 * getEnabledCipherSuites - enabled cipher suites such as TLS_RSA_WITH_AES_256_CBC_SHA"
 * getEnabledProtocols - ssl protocols such as TLSv1
 * @author rkennedy
 */
public abstract class AbstractSSLConfig implements SSLConfig{
	protected Properties properties;
	protected Decrypter decrypter;
	
    @Override
	public void setProperties(Properties props){
		this.properties = props;
	}
	
    @Override
	public void setDecrypter(Decrypter decrypter) {
		this.decrypter = decrypter;
	}
	
	/**
	 * Returns SecurityOptions with 
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