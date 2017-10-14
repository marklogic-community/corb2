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

import com.marklogic.xcc.ContentSource;
import java.util.Properties;
import org.junit.Test;

import static com.marklogic.developer.corb.AbstractContentSourcePool.DEFAULT_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.AbstractContentSourcePool.DEFAULT_CONNECTION_RETRY_LIMIT;
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
        AbstractContentSourcePool contentSourcePool = new AbstractContentSourcePoolImpl();
        contentSourcePool.init(properties, sslConfig);
        assertEquals("bar", contentSourcePool.getProperty("foo"));
        assertNotNull(contentSourcePool.sslConfig);
    }

    @Test
    public void testInitWithSSLConfig() {
        Properties properties = null;
        SSLConfig sslConfig = new TrustAnyoneSSLConfig();
        AbstractContentSourcePool contentSourcePool = new AbstractContentSourcePoolImpl();
        contentSourcePool.init(properties, sslConfig);
        assertNotNull(contentSourcePool.sslConfig);
    }

    @Test
    public void testGetSecurityOptions() throws Exception {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool contentSourcePool = new AbstractContentSourcePoolImpl();
        contentSourcePool.init(properties, sslConfig);
        assertNotNull(contentSourcePool.getSecurityOptions());
    }

    @Test
    public void testGetConnectRetryLimit() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool contentSourcePool = new AbstractContentSourcePoolImpl();
        contentSourcePool.init(properties, sslConfig);
        assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, contentSourcePool.getConnectRetryLimit());
        contentSourcePool.properties.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(5));
        assertEquals(5, contentSourcePool.getConnectRetryLimit());
    }

    @Test
    public void testGetConnectRetryInterval() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool contentSourcePool = new AbstractContentSourcePoolImpl();
        contentSourcePool.init(properties, sslConfig);
        assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, contentSourcePool.getConnectRetryInterval());
        contentSourcePool.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(10));
        assertEquals(10, contentSourcePool.getConnectRetryInterval());
    }

    /**
     * Test of getIntProperty method, of class AbstractContentSourceManager.
     */
    @Test
    public void testGetIntProperty() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool contentSourcePool = new AbstractContentSourcePoolImpl();
        contentSourcePool.init(properties, sslConfig);
        contentSourcePool.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, "ten");
        assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, contentSourcePool.getConnectRetryInterval());
    }

    public class AbstractContentSourcePoolImpl extends AbstractContentSourcePool {
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
