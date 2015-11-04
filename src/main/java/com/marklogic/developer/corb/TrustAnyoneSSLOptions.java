package com.marklogic.developer.corb;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;


public class TrustAnyoneSSLOptions extends AbstractSSLOptions{
	
	@Override
	public SSLContext getSSLContext() throws NoSuchAlgorithmException,KeyManagementException {
		SSLContext sslContext = SSLContext.getInstance("SSLv3");
		TrustManager[] trust = new TrustManager[] {new TrustAnyoneManager()};
		sslContext.init(null, trust, null);
		return sslContext;
	}
	
	private class TrustAnyoneManager implements X509TrustManager{

		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[0];
		}

		public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
			// no exception means it's okay
		}

		public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
			// no exception means it's okay
		}
	}
}
