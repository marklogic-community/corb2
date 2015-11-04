package com.marklogic.developer.corb;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import com.marklogic.xcc.SecurityOptions;

public abstract class AbstractSSLOptions {
	protected Properties properties = null;
	
	public void setProperties(Properties props){
		this.properties = props;
	}
	
	public abstract SSLContext getSSLContext() throws NoSuchAlgorithmException,KeyManagementException;
	
	public  SecurityOptions getSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException {
		return new SecurityOptions(getSSLContext());
	}
	
	protected String getProperty(String key){
		String val = System.getProperty(key);
		if(properties != null && (val == null || val.trim().length() == 0)){
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}
}