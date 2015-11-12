package com.marklogic.developer.corb;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import com.marklogic.xcc.SecurityOptions;

/**
 * AbstractSSLOptions has 3 abstract methods:
 * getSSLContext - build SSLContext with Truststore and Keystore
 * getEnabledCipherSuites - enabled cipher suites such as TLS_RSA_WITH_AES_256_CBC_SHA"
 * getEnabledProtocols - ssl protocols such as TLSv1
 * @author rkennedy
 */
public abstract class AbstractSSLConfig implements SSLConfig{
	protected Properties properties = null;
	protected Decrypter decrypter = null;
	
	public void setProperties(Properties props){
		this.properties = props;
	}
	
	public void setDecrypter(Decrypter decrypter) {
		this.decrypter = decrypter;
	}
	
	/**
	 * Returns SecurityOptions with 
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws KeyManagementException
	 */
	public SecurityOptions getSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException {
		SecurityOptions securityOptions = new SecurityOptions(getSSLContext());
		String[] enabledCipherSuites = getEnabledCipherSuites();
		if (enabledCipherSuites != null) {
			securityOptions.setEnabledCipherSuites(enabledCipherSuites);
		}
		String[] enabledProtocols = getEnabledProtocols();
		if (enabledProtocols != null) {
			securityOptions.setEnabledProtocols(enabledProtocols);
		}
		return securityOptions;
	}
	
	protected String getProperty(String key){
		String val = System.getProperty(key);
		if(properties != null && (val == null || val.trim().length() == 0)){
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}
}