package com.marklogic.developer.corb;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * Standard SSL/TLS with the option to configure a custom TrustStore
 * @since 2.5.7
 */
public class OneWaySSLConfig extends AbstractSSLConfig {

    @Override
    public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        loadPropertiesFile();
        TrustManager[] trustManager = getTrustManagers();
        SSLContext sslContext = getSSLContextInstance(getEnabledProtocols());
        sslContext.init(null, trustManager, null);
        return sslContext;
    }

}
