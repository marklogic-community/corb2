/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.marklogic.developer.corb;

import com.marklogic.xcc.ContentSource;
import java.util.Properties;
import org.junit.Test;

import javax.xml.bind.annotation.XmlType;

import static com.marklogic.developer.corb.AbstractContentSourceManager.DEFAULT_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.AbstractContentSourceManager.DEFAULT_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractContentSourceManagerTest {

    @Test
    public void testInitWithNullSSLConfig() {
        Properties properties = new Properties();
        properties.put("foo", "bar");
        SSLConfig sslConfig = null;
        AbstractContentSourceManager contentSourceManager = new AbstractContentSourceManagerImpl();
        contentSourceManager.init(properties, sslConfig);
        assertEquals("bar", contentSourceManager.getProperty("foo"));
        assertNotNull(contentSourceManager.sslConfig);
    }

    @Test
    public void testInitWithSSLConfig() {
        Properties properties = null;
        SSLConfig sslConfig = new TrustAnyoneSSLConfig();
        AbstractContentSourceManager contentSourceManager = new AbstractContentSourceManagerImpl();
        contentSourceManager.init(properties, sslConfig);
        assertNotNull(contentSourceManager.sslConfig);
    }

    @Test
    public void testGetSecurityOptions() throws Exception {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourceManager contentSourceManager = new AbstractContentSourceManagerImpl();
        contentSourceManager.init(properties, sslConfig);
        assertNotNull(contentSourceManager.getSecurityOptions());
    }

    @Test
    public void testGetConnectRetryLimit() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourceManager contentSourceManager = new AbstractContentSourceManagerImpl();
        contentSourceManager.init(properties, sslConfig);
        assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, contentSourceManager.getConnectRetryLimit());
        contentSourceManager.properties.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(5));
        assertEquals(5, contentSourceManager.getConnectRetryLimit());
    }

    @Test
    public void testGetConnectRetryInterval() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourceManager contentSourceManager = new AbstractContentSourceManagerImpl();
        contentSourceManager.init(properties, sslConfig);
        assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, contentSourceManager.getConnectRetryInterval());
        contentSourceManager.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(10));
        assertEquals(10, contentSourceManager.getConnectRetryInterval());
    }

    /**
     * Test of getIntProperty method, of class AbstractContentSourceManager.
     */
    @Test
    public void testGetIntProperty() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourceManager contentSourceManager = new AbstractContentSourceManagerImpl();
        contentSourceManager.init(properties, sslConfig);
        contentSourceManager.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, "ten");
        assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, contentSourceManager.getConnectRetryInterval());
    }

    public class AbstractContentSourceManagerImpl extends AbstractContentSourceManager {
        @Override
        public ContentSource[] getAllContentSources() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public boolean available() {
            throw new UnsupportedOperationException("Not supported");
        }
        @Override
        public void remove(ContentSource contentSource) {
            throw new UnsupportedOperationException("Not supported");
        }
        @Override
        public ContentSource get() {
            throw new UnsupportedOperationException("Not supported");
        }
        @Override
        public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings){
            throw new UnsupportedOperationException("Not supported");
        }
        @Override
        public void close() {
        }
    }

}
