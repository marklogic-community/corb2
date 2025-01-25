package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.FileUtils;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.*;

import static org.junit.Assert.*;

public class OneWaySSLConfigTest {

    private static final Logger LOG = Logger.getLogger(OneWaySSLConfigTest.class.getName());

    @Test
    public void getSSLContext() {
        try {
            SSLConfig instance = new OneWaySSLConfig();
            SSLContext context = instance.getSSLContext();
            assertNotNull(context);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetTrustManagers() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        Properties properties = new Properties();
        properties.setProperty(SSL_TRUSTSTORE, FileUtils.getFile("keystore.jks").getAbsolutePath());

        instance.setProperties(properties);
        try {
            TrustManager[] trustManagers = instance.getTrustManagers();
            assertNotNull(trustManagers);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testGetTrustManagersWrongType() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        Properties properties = new Properties();
        properties.setProperty(SSL_TRUSTSTORE, FileUtils.getFile("keystore.jks").getAbsolutePath());
        properties.setProperty(SSL_TRUSTSTORE_TYPE, "notjks");
        instance.setProperties(properties);
        TrustManager[] trustManagers = null;
        try {
            trustManagers = instance.getTrustManagers();
            fail("should have had a problem with the truststore type");
        } catch (Exception e) {

        }
        assertNull(trustManagers);
    }

}
