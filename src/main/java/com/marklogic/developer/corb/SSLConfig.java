package com.marklogic.developer.corb;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import com.marklogic.xcc.SecurityOptions;

public interface SSLConfig {
	public void setProperties(Properties props);
	public void setDecrypter(Decrypter decrypter);
	public String[] getEnabledCipherSuites();
	public String[] getEnabledProtocols();
	public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException;
	public SecurityOptions getSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException;
}
