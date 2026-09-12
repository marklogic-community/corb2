/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import static com.marklogic.developer.corb.AbstractContentSourcePool.DEFAULT_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.AbstractContentSourcePool.DEFAULT_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.CONTENT_SOURCE_RENEW;
import static com.marklogic.developer.corb.Options.CONTENT_SOURCE_RENEW_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_HOST_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.marklogic.xcc.ContentSource;
import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractContentSourcePoolTest {

    private static final String foo = "foo";
    private static final String bar = "bar";
    private static final String localhostXccUri = "xcc://user:pass@localhost:8000";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    private AbstractContentSourcePoolImpl newPool(Properties properties, SSLConfig sslConfig) {
        AbstractContentSourcePoolImpl pool = new AbstractContentSourcePoolImpl();
        pool.init(properties, sslConfig);
        return pool;
    }

    @Test
    void testInitWithNullSSLConfig() {
        Properties properties = new Properties();
        properties.put(foo, bar);

        try (AbstractContentSourcePool csp = newPool(properties, null)) {
            assertEquals(bar, csp.getProperty(foo));
            assertNotNull(csp.sslConfig);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testInitWithSSLConfig() {
        try (AbstractContentSourcePool csp = newPool(null, new TrustAnyoneSSLConfig())) {
            assertNotNull(csp.sslConfig);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testGetSecurityOptions() throws Exception {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            assertNotNull(csp.getSecurityOptions());
        }
    }

    @Test
    void testGetConnectRetryLimit() {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, csp.getConnectRetryLimit());
            csp.properties.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(5));
            assertEquals(5, csp.getConnectRetryLimit());
            csp.properties.setProperty(XCC_CONNECTION_RETRY_LIMIT, "-1");
            assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, csp.getConnectRetryLimit());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testGetConnectRetryInterval() {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, csp.getConnectRetryInterval());
            csp.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(10));
            assertEquals(10, csp.getConnectRetryInterval());
            csp.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, "-1");
            assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, csp.getConnectRetryInterval());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testGetConnectHostRetryLimit() {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, csp.getConnectHostRetryLimit());
            csp.properties.setProperty(XCC_CONNECTION_HOST_RETRY_LIMIT, "7");
            assertEquals(7, csp.getConnectHostRetryLimit());
            csp.properties.setProperty(XCC_CONNECTION_HOST_RETRY_LIMIT, "-1");
            assertEquals(DEFAULT_CONNECTION_RETRY_LIMIT, csp.getConnectHostRetryLimit());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testShouldRenewContentSource() {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            assertTrue(csp.shouldRenewContentSource());
            csp.properties.setProperty(CONTENT_SOURCE_RENEW, "false");
            assertFalse(csp.shouldRenewContentSource());
            csp.properties.setProperty(CONTENT_SOURCE_RENEW, "bogus");
            assertTrue(csp.shouldRenewContentSource());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testGetRenewContentSourceInterval() {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            assertEquals(60, csp.getRenewContentSourceInterval());
            csp.properties.setProperty(CONTENT_SOURCE_RENEW_INTERVAL, "42");
            assertEquals(42, csp.getRenewContentSourceInterval());
            csp.properties.setProperty(CONTENT_SOURCE_RENEW_INTERVAL, "-1");
            assertEquals(60, csp.getRenewContentSourceInterval());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testGetIntProperty() {
        try (AbstractContentSourcePool csp = newPool(new Properties(), null)) {
            csp.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, "ten");
            assertEquals(DEFAULT_CONNECTION_RETRY_INTERVAL, csp.getConnectRetryInterval());
            csp.properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, "  12  ");
            assertEquals(12, csp.getConnectRetryInterval());
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testInit() {
        SSLConfig sslConfig = mock(SSLConfig.class);
        Properties props = new Properties();
        props.put(foo, bar);
        try (AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl()) {
            csp.init(props, sslConfig);
            assertEquals(sslConfig, csp.sslConfig());
            assertEquals(bar, csp.getProperty(foo));
        }
    }

    @Test
    void testGetIntPropertyFromSystemProperty() {
        System.setProperty("foo", "123");
        try (AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl()) {
            int value = csp.getIntProperty(foo);
            assertEquals(123, value);
        }
    }

    @Test
    void testGetPropertyPrefersSystemProperty() {
        System.setProperty(foo, "before");
        Properties properties = new Properties();
        properties.setProperty(foo, "after");
        try (AbstractContentSourcePool csp = newPool(properties, null)) {
            assertEquals("before", csp.getProperty(foo));
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    void testPrepareContentSource() {
        ContentSource cs;
        try (AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl()) {
            cs = csp.createContentSource(localhostXccUri);
        }
        if (cs == null || cs.getConnectionProvider() == null) {
            fail("ContentSource should not be null");
        } else {
            assertEquals("localhost", cs.getConnectionProvider().getHostName());
            assertEquals(8000, cs.getConnectionProvider().getPort());
        }
    }

    @Test
    void testPrepareContentSourceNoScheme() {
        ContentSource cs;
        try (AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl()) {
            cs = csp.createContentSource("//user:pass@localhost:8000");
        }
        assertNull(cs);
    }

    @Test
    void testCreateContentSourceWithInvalidUri() {
        ContentSource contentSource;
        try (AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl()) {
            contentSource = csp.createContentSource("not a valid uri");
        }
        assertNull(contentSource);
    }

    @Test
    void testCreateContentSourceWithBlankString() {
        try (AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl()) {
            assertNull(csp.createContentSource("   "));
            assertNull(csp.createContentSource((String) null));
        }
    }

    public static class AbstractContentSourcePoolImpl extends AbstractContentSourcePool {
        private final UnsupportedOperationException unsupported = new UnsupportedOperationException("Not supported");

        @Override
        public boolean available() {
            throw unsupported;
        }

        @Override
        public ContentSource get() {
            throw unsupported;
        }

        @Override
        public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings) {
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
