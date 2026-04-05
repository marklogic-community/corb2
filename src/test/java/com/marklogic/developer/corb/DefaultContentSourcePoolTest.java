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

import static com.marklogic.developer.corb.DefaultContentSourcePool.CONNECTION_POLICY_LOAD;
import static com.marklogic.developer.corb.DefaultContentSourcePool.CONNECTION_POLICY_RANDOM;
import static com.marklogic.developer.corb.DefaultContentSourcePool.CONNECTION_POLICY_ROUND_ROBIN;
import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.*;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.IntStream;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.spi.ConnectionProvider;
import com.marklogic.xcc.types.XdmVariable;

import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.impl.AdhocImpl;
import com.marklogic.xcc.impl.SessionImpl;
import com.marklogic.xcc.impl.SocketPoolProvider;

class DefaultContentSourcePoolTest {

    private static final String localhost = "localhost";
    private static final String localIP1 = "192.168.0.1";
    private static final String localIP2 = "192.168.0.2";
    private static final String localIP3 = "192.168.0.3";
    private static final String localhostXccUri = "xcc://foo:bar@localhost:8000";

	@BeforeEach
	void setUp() {
		clearSystemProperties();
	}

	private void assertHostAndPort(ContentSource contentSource, String hostname, int port) {
        ConnectionProvider connectionProvider = contentSource != null ? contentSource.getConnectionProvider() : null;
        assertNotNull(connectionProvider, "ConnectionProvider should not be null");
        assertEquals(hostname, normalizeHostName(connectionProvider.getHostName()));
		assertEquals(port, connectionProvider.getPort());
	}

    //CircleCI has Amazon EC2 internal IP hostnames, so normalize
	private String normalizeHostName(String hostName) {
	    if (hostName != null && hostName.contains("ec2.internal")) {
	        return hostName.substring(3, hostName.indexOf(".ec2.internal")).replaceAll("-", ".");
        } else {
	        return hostName;
        }
    }

	@Test
	void testInitContentSources() throws CorbException {
        clearSystemProperties();
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri);
            assertTrue(contentSourcePool.available());
            assertNotNull(contentSourcePool.get());
            assertEquals(1, contentSourcePool.getAllContentSources().length);
            assertEquals(CONNECTION_POLICY_ROUND_ROBIN, contentSourcePool.connectionPolicy);
        }
	}

	@Test
	void testInitInvalidContentSources() {
        CorbException thrown = assertThrows(CorbException.class, () -> {
                try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
                    contentSourcePool.init(null, null, "xcc://foo:bar@localhost1:8000");
                    assertFalse(contentSourcePool.available());
                    contentSourcePool.get();
                }
            }
        );
        assertEquals("ContentSource not available.", thrown.getMessage());
	}

    @Test
    void testGetWillPauseWhenError() throws CorbException {
	    Properties properties = new Properties();
	    properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(properties, null, localhostXccUri);

            ContentSource contentSource = contentSourcePool.nextContentSource();
            if (contentSource != null) {
                contentSourcePool.error(contentSource);
            }
            long before = System.currentTimeMillis();
            contentSourcePool.get();
            long after = System.currentTimeMillis();

            assertTrue(contentSourcePool.getConnectRetryInterval() * 1000L <= after - before);
        }
    }

    @Test
    void testInitNullConnectionStrings() {
        assertThrows(NullPointerException.class, () -> {
            try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
                contentSourcePool.init(null, null, null);
            }
        });
    }

	@Test
	void testInitTwoContentSources() {
        clearSystemProperties();
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri, "xcc://foo:bar@192.168.0.1:8000/dbase");
            assertTrue(contentSourcePool.available());
            assertEquals(2, contentSourcePool.getAllContentSources().length);
            assertHostAndPort(contentSourcePool.get(), localhost, 8000);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8000);
        } catch (CorbException ex) {
            fail();
        }
	}

	@Test
	void testInitTwoWithOneInvalidContentSource() throws CorbException{
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri, "xcc://foo:bar@localhost1:8000");
            assertTrue(contentSourcePool.available());
            assertEquals(1, contentSourcePool.getAllContentSources().length);
            assertHostAndPort(contentSourcePool.get(), localhost, 8000);
            assertHostAndPort(contentSourcePool.get(), localhost, 8000);
        }
	}

	@Test
    void testInitConnectionPolicyRandom() {
	    Properties properties = new Properties();
	    properties.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_RANDOM);
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(properties, null, localhostXccUri, "xcc://foo:bar@localhost:8010", "xcc://foo:bar@localhost:8020");
            assertEquals(CONNECTION_POLICY_RANDOM, contentSourcePool.connectionPolicy);
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
        }
    }

    @Test
    void testInitConnectionPolicyLoad() {
        Properties properties = new Properties();
        properties.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(properties, null, localhostXccUri, "xcc://foo:bar@localhost:8010", "xcc://foo:bar@localhost:8020");

            assertEquals(CONNECTION_POLICY_LOAD, contentSourcePool.connectionPolicy);
            assertTrue(contentSourcePool.isLoadPolicy());
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
        }
    }

    @Test
    void testInitConnectionPolicyRoundRobin() {
        clearSystemProperties();
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            assertEquals(CONNECTION_POLICY_ROUND_ROBIN, contentSourcePool.connectionPolicy);
            assertEquals(3, contentSourcePool.getAvailableContentSources().size());
            ContentSource firstContentSource = contentSourcePool.nextContentSource();
            String ipAndPort = contentSourcePool.asString(firstContentSource);
            assertEquals(1, contentSourcePool.ipAddressByHostAndPort.get(ipAndPort).size());
            assertNotNull(firstContentSource);
            assertNotNull(contentSourcePool.nextContentSource());
            assertNotNull(contentSourcePool.nextContentSource());
            assertEquals(firstContentSource, contentSourcePool.nextContentSource());
            contentSourcePool.remove(firstContentSource);
            assertEquals(0, contentSourcePool.ipAddressByHostAndPort.get(ipAndPort).size());
            assertEquals(2, contentSourcePool.getAvailableContentSources().size());

            contentSourcePool.remove(contentSourcePool.nextContentSource());
            contentSourcePool.remove(contentSourcePool.nextContentSource());
            contentSourcePool.remove(contentSourcePool.nextContentSource());
            assertNull(contentSourcePool.nextContentSource());
       }
    }

    @Test
    void testClose() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri);
            assertTrue(contentSourcePool.connectionCountForContentSource.isEmpty());
            assertFalse(contentSourcePool.connectionStringForContentSource.isEmpty());
            assertTrue(contentSourcePool.errorTimeForContentSource.isEmpty());
            assertTrue(contentSourcePool.errorCountForContentSource.isEmpty());
            assertFalse(contentSourcePool.ipAddressByHostAndPort.isEmpty());
            assertFalse(contentSourcePool.renewalTimeForContentSource.isEmpty());
            contentSourcePool.close();
            assertTrue(contentSourcePool.connectionCountForContentSource.isEmpty());
            assertTrue(contentSourcePool.connectionStringForContentSource.isEmpty());
            assertTrue(contentSourcePool.errorTimeForContentSource.isEmpty());
            assertTrue(contentSourcePool.errorCountForContentSource.isEmpty());
            assertTrue(contentSourcePool.ipAddressByHostAndPort.isEmpty());
            assertTrue(contentSourcePool.renewalTimeForContentSource.isEmpty());
        }
    }

    @Test
    void getSessionFromProxy(){
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri);
            ContentSource contentSource = contentSourcePool.get();
            if (contentSource == null) {
                fail("ContentSource should not be null");
            }
            Session session = contentSource.newSession();
            assertTrue(Proxy.isProxyClass(session.getClass()));
            assertEquals(session, DefaultContentSourcePool.getSessionFromProxy(session));
        } catch ( CorbException ex) {
            fail();
        }
    }
    @Test
    void testError() {
        clearSystemProperties();
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            //we started with 3
            assertEquals(3, contentSourcePool.getAllContentSources().length);
            IntStream.rangeClosed(1, 3).forEach(x -> {
                ContentSource contentSource = contentSourcePool.nextContentSource();
                assertNotNull(contentSource);
                //initially there are no errors
                assertEquals(0, contentSourcePool.errorCount(contentSource));
                contentSourcePool.error(contentSource);
            } );
            ContentSource nextContentSource = contentSourcePool.nextContentSource();
            assertNotNull(nextContentSource);
            IntStream.rangeClosed(1, 6).forEach(x -> contentSourcePool.error(nextContentSource));
            ContentSource contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            //after the third error ContentSource is removed
            assertEquals( 2, contentSourcePool.getAllContentSources().length);
            contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            //and another
            assertEquals(1, contentSourcePool.getAllContentSources().length);
            contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            //now no more left
            assertEquals(0, contentSourcePool.getAllContentSources().length);
        }
    }

    @Test
    void testGetAvailableContentSources() {
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            ContentSource contentSource = contentSourcePool.nextContentSource();
            assertEquals(3, contentSourcePool.getAvailableContentSources().size());
            if (contentSource != null) {
                contentSourcePool.error(contentSource);
            }
            assertEquals(2, contentSourcePool.getAvailableContentSources().size());
        }
    }

    @Test
    void testRenewContentSource() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri);
            assertEquals(1, contentSourcePool.getAvailableContentSources().size());
            ContentSource contentSource = contentSourcePool.nextContentSource();
            assertNotNull(contentSource);
            contentSourcePool.renewContentSource(contentSource);
            assertNotNull(contentSource);
            //IP hasn't changed for localhost, so no new ContentSource
            assertEquals(1, contentSourcePool.getAvailableContentSources().size());
            //No new IP added to list of IP for hostAndPort
            assertEquals(1, contentSourcePool.ipAddressByHostAndPort.get(contentSourcePool.asString(contentSource)).size());
        }
    }

    @Test
    void testHoldAndRelease() {
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            ContentSource contentSource = contentSourcePool.nextContentSource();
            assertNull(contentSourcePool.connectionCountForContentSource.get(contentSource));
            contentSourcePool.hold(contentSource);
            assertEquals(1, contentSourcePool.connectionCountForContentSource.get(contentSource).intValue());
            contentSourcePool.hold(contentSource);
            assertEquals(2, contentSourcePool.connectionCountForContentSource.get(contentSource).intValue());
            contentSourcePool.release(contentSource);
            assertEquals(1, contentSourcePool.connectionCountForContentSource.get(contentSource).intValue());
            contentSourcePool.release(contentSource);
            assertEquals(0, contentSourcePool.connectionCountForContentSource.get(contentSource).intValue());
        }
    }

    private DefaultContentSourcePool initRoundRobinPool() {
        Properties properties = new Properties();
        properties.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_ROUND_ROBIN);
        DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool();
        contentSourcePool.init(properties, null, localhostXccUri, "xcc://foo:bar@localhost:8010", "xcc://foo:bar@localhost:8020");
        return contentSourcePool;
    }

	@Test
	void testRoundRobinPolicy() throws CorbException{
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            assertEquals(3, contentSourcePool.getAllContentSources().length);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
        }
	}

	@Test
	void testRoundRobinPolicyWithOneError() throws CorbException {
        clearSystemProperties();
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            assertEquals(3, contentSourcePool.getAllContentSources().length);
            ContentSource contentSource = contentSourcePool.get();
            assertHostAndPort(contentSource, localIP1, 8001);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource));
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
        }
	}

	@Test
	void testRoundRobinPolicyWithTwoErrors() throws CorbException {
        clearSystemProperties();
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            assertEquals(3, contentSourcePool.getAllContentSources().length);
            ContentSource contentSource1;
            ContentSource contentSource2;
            assertHostAndPort((contentSource1 = contentSourcePool.get()), localIP1, 8001);
            assertHostAndPort((contentSource2 = contentSourcePool.get()), localIP2, 8002);
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource1));
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource2));
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
        }
	}

	@Test
	void testRoundRobinPolicyWithUnexpiredContentSource() throws CorbException, InterruptedException {
        clearSystemProperties();
		System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            ContentSource contentSource = null;
            assertHostAndPort((contentSource = contentSourcePool.get()), localIP1, 8001);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource));
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            Thread.sleep(1000L);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
        }
	}

	@Test
	void testRoundRobinPolicyWithReactivatedContentSource() throws CorbException {
        clearSystemProperties();
		System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            ContentSource contentSource;
            assertHostAndPort((contentSource = contentSourcePool.get()), localIP1, 8001);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource));
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            contentSourcePool.success(contentSourcePool.getAllContentSources()[0]);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
        }
	}

	@Test
	void testRoundRobinPolicyWithAllErrors() throws CorbException{
	    Properties properties = new Properties();
        properties.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(1));
        properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(0));
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(properties, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            assertEquals(3, contentSourcePool.getAllContentSources().length);
            ContentSource contentSource1;
            ContentSource contentSource2;
            ContentSource contentSource3;
            assertHostAndPort((contentSource1 = contentSourcePool.get()), localIP1, 8001);
            assertHostAndPort((contentSource2 = contentSourcePool.get()), localIP2, 8002);
            assertHostAndPort((contentSource3 = contentSourcePool.get()), localIP3, 8003);
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource1));
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource2));
            contentSourcePool.error(DefaultContentSourcePool.getContentSourceFromProxy(contentSource3));
            contentSourcePool.get();
        }
	}

	@Test
	void tryToTestRandomPolicy() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_RANDOM);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            assertTrue(Arrays.asList(new String[]{localIP1, localIP2}).contains(normalizeHostName(contentSourcePool.get().getConnectionProvider().getHostName())));
            assertTrue(Arrays.asList(new String[]{localIP1, localIP2}).contains(normalizeHostName(contentSourcePool.get().getConnectionProvider().getHostName())));
        }
	}

	@Test
	void testRandomPolicyWithOneError() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_RANDOM);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            assertTrue(Arrays.asList(new String[]{localIP1, localIP2}).contains(normalizeHostName(contentSourcePool.get().getConnectionProvider().getHostName())));
            assertTrue(Arrays.asList(new String[]{localIP1, localIP2}).contains(normalizeHostName(contentSourcePool.get().getConnectionProvider().getHostName())));
            contentSourcePool.error(contentSourcePool.getAllContentSources()[0]);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
        }
	}

	@Test
	void testLoadPolicy() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            ContentSource[] contentSourceList = contentSourcePool.getAllContentSources();
            assertEquals(2, contentSourceList.length);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001); //should get the same host as there is no load
            contentSourcePool.hold(contentSourceList[0]);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            contentSourcePool.release(contentSourceList[0]);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
        }
	}

	@Test
	void testLoadPolicy2() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            ContentSource[] contentSourceList = contentSourcePool.getAllContentSources();
            assertEquals(2, contentSourceList.length);
            contentSourcePool.hold(contentSourceList[0]);
            contentSourcePool.hold(contentSourceList[1]);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.hold(contentSourceList[0]);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
        }
	}

	@Test
	void testLoadPolicyWithOneError() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            ContentSource[] contentSourceList = contentSourcePool.getAllContentSources();
            assertEquals(3, contentSourceList.length);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.error(contentSourceList[0]);
            contentSourcePool.hold(contentSourceList[1]);
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
            contentSourcePool.release(contentSourceList[1]);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
        }
	}

	@Test
	void testLoadPolicyWithTwoErrors() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            ContentSource[] contentSourceList = contentSourcePool.getAllContentSources();
            assertEquals(3, contentSourceList.length);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.error(contentSourceList[0]);
            contentSourcePool.error(contentSourceList[1]);
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
            contentSourcePool.hold(contentSourceList[2]);
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
        }
	}

	@Test
	void testLoadPolicyWithReactivatedContentSource() throws CorbException {
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002", "xcc://foo:bar@192.168.0.3:8003");
            ContentSource[] contentSourceList = contentSourcePool.getAllContentSources();
            assertEquals(3, contentSourceList.length);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.hold(contentSourceList[0]);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[1]);
            //first has an open connection, second has an error, so third is returned
            assertHostAndPort( contentSourcePool.get(), localIP3, 8003);
            contentSourcePool.success(contentSourcePool.getAllContentSources()[1]);
            //second had a success, so now second is returned next
            assertHostAndPort( contentSourcePool.get(), localIP2, 8002);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[1]);
            //second had an error, so third is returned
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
            contentSourcePool.release(contentSourcePool.getAllContentSources()[0]);
            //the request from the first is finally released, so now first is returned
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[0]);
            //an error on the first, so now third returned
            assertHostAndPort(contentSourcePool.get(), localIP3, 8003);
        }
	}

	@Test
	void testLoadPolicyWithAllErrors() throws CorbException {
		System.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(1));
	    System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(0));
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_LOAD);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            ContentSource[] contentSourceList = contentSourcePool.getAllContentSources();
            assertEquals(2, contentSourceList.length);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[0]);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[1]);
            assertHostAndPort(contentSourcePool.get(), localIP1, 8001);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[0]);
            assertHostAndPort(contentSourcePool.get(), localIP2, 8002);
            contentSourcePool.error(contentSourcePool.getAllContentSources()[0]);
            assertThrows(CorbException.class, contentSourcePool::get);
        }
	}

	@Test
	void testSubmitWithMockRequest() {
		ContentSource contentSource = mock(ContentSource.class);
		Session session = mock(Session.class);
		AdhocImpl request = mock(AdhocImpl.class);
		ResultSequence rs = mock(ResultSequence.class);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.contentSources.add(contentSource);
            when(contentSource.newSession()).thenReturn(session);
            when(session.newAdhocQuery(any())).thenReturn(request);
            when(request.getSession()).thenReturn(session);
            when(session.submitRequest(request)).thenReturn(rs);

            assertEquals(rs, contentSourcePool.get().newSession().submitRequest(request));
        } catch (RequestException | CorbException ex) {
            fail();
        }
	}

	@Test
	void testSubmitWithMockRequestWithFailOver() throws RequestException, CorbException {
		System.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(1));
	    System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
		ContentSource contentSource1 = mock(ContentSource.class);
		Session session1 = mock(SessionImpl.class);

		ContentSource contentSource2 = mock(ContentSource.class);
		Session session2 = mock(SessionImpl.class);

		AdhocImpl request = mock(AdhocImpl.class);
		ResultSequence result = mock(ResultSequence.class);
        XdmVariable variable = mock(XdmVariable.class);
        XdmVariable[] variables = { variable };

        when(request.getVariables()).thenReturn(variables);
		when(contentSource1.newSession()).thenReturn(session1);
		when(contentSource1.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost,8001));

		when(contentSource2.newSession()).thenReturn(session2);
		when(contentSource2.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost,8002));

		when(session1.newAdhocQuery(any())).thenReturn(request);
		when(session1.submitRequest(any())).thenThrow(mock(ServerConnectionException.class));

		when(session2.newAdhocQuery(any())).thenReturn(request);
		when(session2.submitRequest(any())).thenReturn(result);

		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "");
            contentSourcePool.contentSources.add(contentSource1);
            contentSourcePool.connectionStringForContentSource.put(contentSource1, "xcc://localhost:8000");
            contentSourcePool.contentSources.add(contentSource2);
            contentSourcePool.connectionStringForContentSource.put(contentSource2, "xcc://localhost:8000");
            //no entries in errorMap
            assertEquals(0, contentSourcePool.errorCountForContentSource.size() );
            ResultSequence gotResult = contentSourcePool.get().newSession().submitRequest(request);
            //now there is
            assertEquals(1, contentSourcePool.errorCountForContentSource.size() );
            assertEquals(result, gotResult);
        }
	}

	@Test
	void testSubmitWithMockInsertWithFailOver() throws RequestException, CorbException {
		System.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(1));
	    System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
		ContentSource contentSource1 = mock(ContentSource.class);
		Session session1 = mock(SessionImpl.class);

		ContentSource contentSource2 = mock(ContentSource.class);
		Session session2 = mock(SessionImpl.class);

		Content content = mock(Content.class);

		when(contentSource1.newSession()).thenReturn(session1);
		when(contentSource1.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost,8001));

		when(contentSource2.newSession()).thenReturn(session2);
		when(contentSource2.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost,8002));

		doThrow(mock(ServerConnectionException.class)).when(session1).insertContent(content);
		doNothing().when(session2).insertContent(content);

		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "");
            contentSourcePool.contentSources.add(contentSource1);
            contentSourcePool.contentSources.add(contentSource2);
            ContentSource contentSource = contentSourcePool.get();
            assertNotNull(contentSource);
            try (Session session = contentSource.newSession()) {
                session.insertContent(content);
                assertEquals(1L, (long) contentSourcePool.errorCountForContentSource.get(contentSource1));
            }
        }
	}

	@Test
	void testSubmitWithMockRequestAndErrorAndWait() throws RequestException, CorbException {
        System.setProperty(XCC_CONNECTION_RETRY_LIMIT, Integer.toString(1));
        System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(SessionImpl.class);
        AdhocImpl request = mock(AdhocImpl.class);
        ResultSequence result = mock(ResultSequence.class);
        when(contentSource.newSession()).thenReturn(session);
        when(contentSource.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost, 8001));
        when(session.newAdhocQuery(any())).thenReturn(request);
        when(session.submitRequest(any())).thenReturn(result);
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "");
            contentSourcePool.contentSources.add(contentSource);
            contentSourcePool.connectionStringForContentSource.put(contentSource, "xcc://user:secret@localhost:8000");
            ResultSequence gotResult = contentSourcePool.get().newSession().submitRequest(request);
            //mock result returned
            assertEquals(result, gotResult);
            contentSourcePool.error(contentSource);
            //after an error, still one ContentSource
            assertEquals(1, contentSourcePool.contentSources.size());
            //and is the same contentsource, because IP is same
            assertTrue(contentSourcePool.contentSources.contains(contentSource));
        }
	}

	@Test
	void testSubmitWithMockRequestAndError() throws RequestException {
		System.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
		ContentSource contentSource1 = mock(ContentSource.class);
		Session session1 = mock(SessionImpl.class);
		ContentSource contentSource2 = mock(ContentSource.class);
		Session session2 = mock(SessionImpl.class);
		AdhocImpl request = mock(AdhocImpl.class);
		when(contentSource1.newSession()).thenReturn(session1);
		when(contentSource1.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost,8001));
		when(contentSource2.newSession()).thenReturn(session2);
		when(contentSource2.getConnectionProvider()).thenReturn(new SocketPoolProvider(localhost,8002));
		when(session1.newAdhocQuery(any())).thenReturn(request);
		when(session1.submitRequest(any())).thenThrow(mock(ServerConnectionException.class));
		when(session2.newAdhocQuery(any())).thenReturn(request);
		when(session2.submitRequest(any())).thenThrow(mock(ServerConnectionException.class));

		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.contentSources.add(contentSource1);
            contentSourcePool.contentSources.add(contentSource2);
            ContentSource nextContentSource = contentSourcePool.get();
            assertNotNull(nextContentSource);
            assertThrows(ServerConnectionException.class, () -> nextContentSource.newSession().submitRequest(request));
        } catch (CorbException ex) {
            fail();
        }
	}

    @Test
    void testGetIPAddress() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@localhost:8000");
            ContentSource contentSource = contentSourcePool.get();
            if (contentSource == null) {
                fail("ContentSource should not be null");
            }
            String ip = contentSourcePool.getIPAddress(contentSource);
            assertEquals("127.0.0.1", ip);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testGetIPAddressWithIPFromConnectionstring() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@127.0.0.1:8000");
            ContentSource contentSource = contentSourcePool.get();
            if (contentSource == null) {
                fail("ContentSource should not be null");
            }
            String ip = contentSourcePool.getIPAddress(contentSource);
            assertEquals("127.0.0.1", ip);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testGetIPAddressNotCastable() {
        ContentSource contentSource = mock(ContentSource.class);
        when(contentSource.getConnectionProvider()).thenReturn(mock(ConnectionProvider.class));
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@localhost:8000");
            String ip = contentSourcePool.getIPAddress(contentSource);
            assertNull(ip);
        }
    }



    @Test
    void testHaveDifferentIP() {
        DefaultContentSourcePool contentSourcePool = mock(DefaultContentSourcePool.class);
        ContentSource contentSourceA = mock(ContentSource.class);
        ContentSource contentSourceB = mock(ContentSource.class);
        ConnectionProvider provider = mock(ConnectionProvider.class);
        when(provider.getHostName()).thenReturn("localhost");
        when(contentSourcePool.getIPAddress(contentSourceA)).thenReturn("127.0.0.1");
        when(contentSourceA.getConnectionProvider()).thenReturn(provider);
        when(contentSourcePool.getIPAddress(contentSourceB)).thenReturn("127.0.0.2");
        when(contentSourcePool.haveDifferentIP(contentSourceA, contentSourceB)).thenCallRealMethod();

        assertTrue(contentSourcePool.haveDifferentIP(contentSourceA, contentSourceB));
        assertFalse(contentSourcePool.haveDifferentIP(contentSourceA, contentSourceA));
    }

    @Test
    void testDefaultPolicy() {
        clearSystemProperties();
        Properties properties = new Properties();
        properties.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_RANDOM);
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            assertFalse(contentSourcePool.isRandomPolicy());
            assertFalse(contentSourcePool.isLoadPolicy());
            assertEquals(CONNECTION_POLICY_ROUND_ROBIN, contentSourcePool.connectionPolicy);
            contentSourcePool.init(properties, null, "");
            assertTrue(contentSourcePool.isRandomPolicy());
            assertEquals(CONNECTION_POLICY_RANDOM, contentSourcePool.connectionPolicy);
        }
    }

    @Test
    void testAsString() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://user:pass@localhost:8000");
            ContentSource contentSource = contentSourcePool.get();
            assertEquals("null", contentSourcePool.asString(null));
            assertEquals("localhost:8000", contentSourcePool.asString(contentSource));
        } catch (CorbException ex){
            fail();
        }
    }

}
