/*
 * * Copyright (c) 2004-2022 MarkLogic Corporation
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
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.IntStream;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.spi.ConnectionProvider;
import com.marklogic.xcc.types.XdmVariable;
import org.junit.Before;
import org.junit.Test;

import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.impl.AdhocImpl;
import com.marklogic.xcc.impl.SessionImpl;
import com.marklogic.xcc.impl.SocketPoolProvider;

public class DefaultContentSourcePoolTest {

    private String localhost = "localhost";
    private String localIP1 = "192.168.0.1";
    private String localIP2 = "192.168.0.2";
    private String localIP3 = "192.168.0.3";
    private String localhostXccUri = "xcc://foo:bar@localhost:8000";

	@Before
	public void setUp() throws FileNotFoundException {
		clearSystemProperties();
	}

	private void assertHostAndPort(ContentSource contentSource, String hostname, int port) {
        assertEquals(hostname, normalizeHostName(contentSource.getConnectionProvider().getHostName()));
		assertEquals(port, contentSource.getConnectionProvider().getPort());
	}

    //CircleCI has Amazon EC2 internal IP hostnames, so normalize
	private String normalizeHostName(String hostName) {
	    if (hostName.contains("ec2.internal")) {
	        return hostName.substring(3, hostName.indexOf(".ec2.internal")).replaceAll("-", ".");
        } else {
	        return hostName;
        }
    }
	@Test
	public void testInitContentSources() throws CorbException{
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri);
            assertTrue(contentSourcePool.available());
            assertNotNull(contentSourcePool.get());
            assertEquals(1, contentSourcePool.getAllContentSources().length);
            assertEquals(CONNECTION_POLICY_ROUND_ROBIN, contentSourcePool.connectionPolicy);
        }
	}

	@Test
	public void testInitInvalidContentSources() {
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
    public void testGetWillPauseWhenError() throws CorbException {
	    Properties properties = new Properties();
	    properties.setProperty(XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(1));
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(properties, null, localhostXccUri);

            ContentSource contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            long before = System.currentTimeMillis();
            contentSourcePool.get();
            long after = System.currentTimeMillis();

            assertTrue(contentSourcePool.getConnectRetryInterval() * 1000 <= after - before);
        }
    }

    @Test
    public void testInitNullConnectionStrings() {
        assertThrows(NullPointerException.class, () -> {
            try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
                contentSourcePool.init(null, null, null);
            }
        });
    }

	@Test
	public void testInitTwoContentSources() {
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
	public void testInitTwoWithOneInvalidContentSource() throws CorbException{
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, localhostXccUri, "xcc://foo:bar@localhost1:8000");
            assertTrue(contentSourcePool.available());
            assertEquals(1, contentSourcePool.getAllContentSources().length);
            assertHostAndPort(contentSourcePool.get(), localhost, 8000);
            assertHostAndPort(contentSourcePool.get(), localhost, 8000);
        }
	}

	@Test
    public void testInitConnectionPolicyRandom() {
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
    public void testInitConnectionPolicyLoad() {
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
    public void testInitConnectionPolicyRoundRobin() {
       try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
           assertEquals(CONNECTION_POLICY_ROUND_ROBIN, contentSourcePool.connectionPolicy);
           assertEquals(3, contentSourcePool.getAvailableContentSources().size());
           ContentSource firstContentSource = contentSourcePool.nextContentSource();
           assertNotNull(firstContentSource);
           assertNotNull(contentSourcePool.nextContentSource());
           assertNotNull(contentSourcePool.nextContentSource());
           assertEquals(firstContentSource, contentSourcePool.nextContentSource());
           contentSourcePool.remove(firstContentSource);
           assertEquals(2, contentSourcePool.getAvailableContentSources().size());
           contentSourcePool.remove(contentSourcePool.nextContentSource());
           contentSourcePool.remove(contentSourcePool.nextContentSource());
           contentSourcePool.remove(contentSourcePool.nextContentSource());
           assertNull(contentSourcePool.nextContentSource());
       }
    }

    @Test
    public void testError(){
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            assertEquals("we started with 3", 3, contentSourcePool.getAllContentSources().length);
            IntStream.rangeClosed(1, 3).forEach(x -> {
                ContentSource contentSource = contentSourcePool.nextContentSource();
                assertEquals("initially there are no errors",0, contentSourcePool.errorCount(contentSource));
                contentSourcePool.error(contentSource);
            } );
            IntStream.rangeClosed(1, 6).forEach(x -> contentSourcePool.error(contentSourcePool.nextContentSource()));
            ContentSource contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            assertEquals("after the third error ContentSource is removed", 2, contentSourcePool.getAllContentSources().length);
            contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            assertEquals("and another", 1, contentSourcePool.getAllContentSources().length);
            contentSource = contentSourcePool.nextContentSource();
            contentSourcePool.error(contentSource);
            assertEquals("now no more left", 0, contentSourcePool.getAllContentSources().length);
        }
    }

    @Test
    public void testGetAvailableContentSources(){
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            ContentSource contentSource = contentSourcePool.nextContentSource();
            assertEquals(3, contentSourcePool.getAvailableContentSources().size());
            contentSourcePool.error(contentSource);
            assertEquals(2, contentSourcePool.getAvailableContentSources().size());
        }
    }

    @Test
    public void testHoldAndRelease() {
        try (DefaultContentSourcePool contentSourcePool = initRoundRobinPool()) {
            ContentSource contentSource = contentSourcePool.nextContentSource();
            assertNull(contentSourcePool.connectionCountsMap.get(contentSource));
            contentSourcePool.hold(contentSource);
            assertEquals(1, contentSourcePool.connectionCountsMap.get(contentSource).intValue());
            contentSourcePool.hold(contentSource);
            assertEquals(2, contentSourcePool.connectionCountsMap.get(contentSource).intValue());
            contentSourcePool.release(contentSource);
            assertEquals(1, contentSourcePool.connectionCountsMap.get(contentSource).intValue());
            contentSourcePool.release(contentSource);
            assertEquals(0, contentSourcePool.connectionCountsMap.get(contentSource).intValue());
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
	public void testRoundRobinPolicy() throws CorbException{
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
	public void testRoundRobinPolicyWithOneError() throws CorbException{
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
	public void testRoundRobinPolicyWithTwoErrors() throws CorbException{
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
	public void testRoundRobinPolicyWithUnexpiredContentSource() throws CorbException, InterruptedException{
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
	public void testRoundRobinPolicyWithReactivatedContentSource() throws CorbException {
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
	public void testRoundRobinPolicyWithAllErrors() throws CorbException{
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
	public void tryToTestRandomPolicy() throws CorbException{
		System.setProperty(CONNECTION_POLICY, CONNECTION_POLICY_RANDOM);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@192.168.0.1:8001", "xcc://foo:bar@192.168.0.2:8002");
            assertTrue(Arrays.asList(new String[]{localIP1, localIP2}).contains(normalizeHostName(contentSourcePool.get().getConnectionProvider().getHostName())));
            assertTrue(Arrays.asList(new String[]{localIP1, localIP2}).contains(normalizeHostName(contentSourcePool.get().getConnectionProvider().getHostName())));
        }
	}

	@Test
	public void testRandomPolicyWithOneError() throws CorbException{
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
	public void testLoadPolicy() throws CorbException{
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
	public void testLoadPolicy2() throws CorbException{
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
	public void testLoadPolicyWithOneError() throws CorbException{
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
	public void testLoadPolicyWithTwoErrors() throws CorbException{
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
	public void testLoadPolicyWithReactivatedContentSource() throws CorbException{
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

	@Test(expected = CorbException.class)
	public void testLoadPolicyWithAllErrors() throws CorbException{
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
            contentSourcePool.get();
        }
	}

	@Test
	public void testSubmitWithMockRequest() {
		ContentSource contentSource = mock(ContentSource.class);
		Session session = mock(Session.class);
		AdhocImpl request = mock(AdhocImpl.class);
		ResultSequence rs = mock(ResultSequence.class);
		try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.contentSourceList.add(contentSource);
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
	public void testSubmitWithMockRequestWithFailOver() throws RequestException, CorbException {
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
            contentSourcePool.contentSourceList.add(contentSource1);
            contentSourcePool.connectionStringMap.put(contentSource1, "xcc://localhost:8000");
            contentSourcePool.contentSourceList.add(contentSource2);
            contentSourcePool.connectionStringMap.put(contentSource2, "xcc://localhost:8000");

            assertEquals("no entries in errorMap", 0, contentSourcePool.errorCountsMap.size() );
            ResultSequence gotResult = contentSourcePool.get().newSession().submitRequest(request);
            assertEquals("now there is", 1, contentSourcePool.errorCountsMap.size() );
            assertEquals(result, gotResult);
        }
	}

	@Test
	public void testSubmitWithMockInsertWithFailOver() throws RequestException, CorbException {
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
            contentSourcePool.contentSourceList.add(contentSource1);
            contentSourcePool.contentSourceList.add(contentSource2);

            contentSourcePool.get().newSession().insertContent(content);
            assertEquals(1L, (long)contentSourcePool.errorCountsMap.get(contentSource1));
        }
	}

	@Test
	public void testSubmitWithMockRequestAndErrorAndWait() throws RequestException, CorbException {
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
            contentSourcePool.contentSourceList.add(contentSource);
            contentSourcePool.connectionStringMap.put(contentSource, "xcc://user:secret@localhost:8000");
            ResultSequence gotResult = contentSourcePool.get().newSession().submitRequest(request);
            assertEquals("mock result returned", result, gotResult);
            contentSourcePool.error(contentSource);
            assertEquals("after an error, still one ContentSource",1, contentSourcePool.contentSourceList.size());
            assertTrue("and is the same contentsource, because IP is same", contentSourcePool.contentSourceList.contains(contentSource));
        }
	}

	@Test(expected = ServerConnectionException.class)
	public void testSubmitWithMockRequestAndError() throws RequestException, CorbException {
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
            contentSourcePool.contentSourceList.add(contentSource1);
            contentSourcePool.contentSourceList.add(contentSource2);

            contentSourcePool.get().newSession().submitRequest(request);
        }
	}

    @Test
    public void testGetIPAddress() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@localhost:8000");
            ContentSource contentSource = contentSourcePool.get();
            String ip = contentSourcePool.getIPAddress(contentSource);
            assertEquals("127.0.0.1", ip);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void testGetIPAddressWithIPFromConnectionstring() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@1.1.1.1:8000");
            ContentSource contentSource = contentSourcePool.get();
            String ip = contentSourcePool.getIPAddress(contentSource);
            assertEquals("1.1.1.1", ip);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void testGetIPAddressNotCastable() {
        ContentSource contentSource = mock(ContentSource.class);
        when(contentSource.getConnectionProvider()).thenReturn(mock(ConnectionProvider.class));
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@localhost:8000");
            String ip = contentSourcePool.getIPAddress(contentSource);
            assertNull("Since the mock ConnectionProvider is not an instance of SingleHostAddress it couldn't get the IP", ip);
        }
    }

    @Test
    public void testReplaceContentSource() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://foo:bar@1.1.1.1:8000");
            ContentSource contentSource = contentSourcePool.get();
            long time = System.currentTimeMillis();
            ContentSource freshContentSource = mock(ContentSource.class);
            contentSourcePool.errorTimeMap.put(contentSource, time);
            contentSourcePool.connectionCountsMap.put(contentSource, 5);
            contentSourcePool.errorCountsMap.put(contentSource, 3);
            contentSourcePool.replaceContentSource(contentSource, freshContentSource);
            assertEquals(1, contentSourcePool.contentSourceList.size());
            assertEquals(freshContentSource, contentSourcePool.contentSourceList.get(0));
            assertEquals(3L, (long)contentSourcePool.errorCountsMap.get(freshContentSource));
            assertEquals("xcc://foo:bar@1.1.1.1:8000", contentSourcePool.connectionStringMap.get(freshContentSource));
            assertEquals(5L, (long)contentSourcePool.connectionCountsMap.get(freshContentSource));
            assertEquals(time, (long)contentSourcePool.errorTimeMap.get(freshContentSource));

        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void testHaveDifferentIP() {
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
    public void testDefaultPolicy() {
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
    public void testAsString() {
        try (DefaultContentSourcePool contentSourcePool = new DefaultContentSourcePool()) {
            contentSourcePool.init(null, null, "xcc://user:pass@localhost:8000");
            ContentSource contentSource = contentSourcePool.get();
            assertEquals("if null, prints the word", "null", contentSourcePool.asString(null));
            assertEquals("otherwise, is just the contentsource.toString()", contentSource.toString(), contentSourcePool.asString(contentSource));
        } catch (CorbException ex){
            fail();
        }
    }

}
