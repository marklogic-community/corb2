/*
 * * Copyright (c) 2004-2021 MarkLogic Corporation
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 * *
 * * The use of the Apache License does not indicate that this project is
 * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import org.junit.Before;
import org.junit.Test;

import com.marklogic.xcc.ContentSource;

import static com.marklogic.developer.corb.AbstractContentSourcePool.DEFAULT_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.AbstractContentSourcePool.DEFAULT_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.FileNotFoundException;
import java.util.Properties;

public class AbstractContentSourcePoolTest {

    private String foo = "foo";
    private String bar = "bar";
    private String localhostXccUri = "xcc://user:pass@localhost:8000";

	@Before
	public void setUp() throws FileNotFoundException {
		clearSystemProperties();
	}

	@Test
    public void testInitWithNullSSLConfig() {
        Properties properties = new Properties();
        properties.put(foo, bar);
        SSLConfig sslConfig = null;
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        assertEquals(bar, csp.getProperty(foo));
        assertNotNull(csp.sslConfig);
    }

    @Test
    public void testInitWithSSLConfig() {
        Properties properties = null;
        SSLConfig sslConfig = new TrustAnyoneSSLConfig();
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        assertNotNull(csp.sslConfig);
    }

    @Test
    public void testGetSecurityOptions() throws Exception {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        assertNotNull(csp.getSecurityOptions());
    }

    @Test
    public void testGetConnectRetryLimit() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, csp.getConnectRetryLimit());
        csp.properties.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(5));
        assertEquals(5, csp.getConnectRetryLimit());
    }

    @Test
    public void testGetConnectRetryInterval() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, csp.getConnectRetryInterval());
        csp.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(10));
        assertEquals(10, csp.getConnectRetryInterval());
    }

    /**
     * Test of getIntProperty method, of class AbstractContentSourceManager.
     */
    @Test
    public void testGetIntProperty() {
        Properties properties = new Properties();
        SSLConfig sslConfig = null;
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        csp.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, "ten");
        assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, csp.getConnectRetryInterval());
    }

	@Test
	public void testInit() {
		SSLConfig sslConfig = mock(SSLConfig.class);
		Properties props = new Properties();
		props.put(foo, bar);
		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
		csp.init(props, sslConfig);
		assertEquals(sslConfig,csp.sslConfig());
		assertEquals(bar,csp.getProperty(foo));
	}

	@Test
	public void testGetIntPropertyFromSystemProperty() {
		System.setProperty("foo", "123");
		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
		assertEquals(123,csp.getIntProperty(foo));
	}

	@Test
    public void testPrepareContentSource() {
		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource cs = csp.createContentSource(localhostXccUri);
        assertEquals("localhost",cs.getConnectionProvider().getHostName());
        assertEquals(8000,cs.getConnectionProvider().getPort());
    }

    @Test
    public void testPrepareContentSourceSecureXCC() {
        AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource cs = csp.createContentSource(localhostXccUri);
        assertEquals("localhost",cs.getConnectionProvider().getHostName());
        assertEquals(8000,cs.getConnectionProvider().getPort());
    }

    @Test
    public void testPrepareContentSourceNoScheme() throws CorbException {
        AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource cs = csp.createContentSource("//user:pass@localhost:8000");
        assertNull(cs);
    }

    @Test
    public void testCreateContentSourceWithInvalidUri() {
        AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource contentSource = csp.createContentSource("not a valid uri");
        assertNull(contentSource);
    }

    public class AbstractContentSourcePoolImpl extends AbstractContentSourcePool {
        private UnsupportedOperationException unsupported = new UnsupportedOperationException("Not supported");
        @Override
        public boolean available() {
            throw unsupported;
        }
        @Override
        public ContentSource get() {
            throw unsupported;
        }
        @Override
        public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings){
            throw new UnsupportedOperationException("Not supported");
        }
        @Override
        public void close() {
        }
		@Override
		public void remove(ContentSource contentSource) {
			throw unsupported;
		}
		@Override
		public ContentSource[] getAllContentSources() {
			throw unsupported;
		}
    }
}
