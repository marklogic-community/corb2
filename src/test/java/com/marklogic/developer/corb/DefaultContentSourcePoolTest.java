package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.marklogic.xcc.ContentSource;

public class DefaultContentSourcePoolTest {
	@Before
	public void setUp() throws FileNotFoundException {
		clearSystemProperties();
	}
	
	private void assertHostAndPort(ContentSource cs, String hostname, int port) {
		assertEquals(hostname,cs.getConnectionProvider().getHostName());
		assertEquals(port,cs.getConnectionProvider().getPort());
	}
	
	@Test
	public void testInitContentSources() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost:8000"});
		assertTrue(csp.available());
		assertNotNull(csp.get());
	}
	
	@Test(expected = CorbException.class)
	public void testInvalidContentSources() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost1:8000"});
		assertFalse(csp.available());
		csp.get();
	}
	
	@Test
	public void testInitTwoContentSources() throws CorbException {
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@192.168.0.1:8000"});
		assertTrue(csp.available());
		assertEquals(2,csp.getAllContentSources().length);
		assertHostAndPort(csp.get(),"localhost",8000);
		assertHostAndPort(csp.get(),"192.168.0.1",8000);
	}
	
	@Test
	public void testInitTwoWithOneInvalidContentSource() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@localhost1:8000"});
		assertTrue(csp.available());
		assertEquals(1,csp.getAllContentSources().length);
		assertHostAndPort(csp.get(),"localhost",8000);
		assertHostAndPort(csp.get(),"localhost",8000);
	}
		
	@Test
	public void testRoundRobinPolicy() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		assertEquals(3,csp.getAllContentSources().length);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
	}
		
	@Test
	public void testRoundRobinPolicyWithOneError() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		assertEquals(3,csp.getAllContentSources().length);
		ContentSource ecs = null;
		assertHostAndPort((ecs=csp.get()),"192.168.0.1",8001);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		csp.error(csp.getContentSourceFromProxy(ecs));
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
	}
	
	@Test
	public void testRoundRobinPolicyWithTwoErrors() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		assertEquals(3,csp.getAllContentSources().length);
		ContentSource ecs1 = null;
		ContentSource ecs2 = null;
		assertHostAndPort((ecs1=csp.get()),"192.168.0.1",8001);
		assertHostAndPort((ecs2=csp.get()),"192.168.0.2",8002);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		csp.error(csp.getContentSourceFromProxy(ecs1));
		csp.error(csp.getContentSourceFromProxy(ecs2));
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
	}
	
	@Test
	public void testRoundRobinPolicyWithUnexpiredContentSource() throws CorbException, InterruptedException{
		System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "1");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		ContentSource ecs1 = null;
		assertHostAndPort((ecs1=csp.get()),"192.168.0.1",8001);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		csp.error(csp.getContentSourceFromProxy(ecs1));
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		Thread.sleep(1000L);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
	}
	
	@Test
	public void testRoundRobinPolicyWithReactivatedContentSource() throws CorbException, InterruptedException{
		System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "1");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		ContentSource ecs1 = null;
		assertHostAndPort((ecs1=csp.get()),"192.168.0.1",8001);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		csp.error(csp.getContentSourceFromProxy(ecs1));
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		csp.success(csp.getContentSourceFromProxy(ecs1));
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
	}
	
	@Test
	public void testRoundRobinPolicyWithAllErrors() throws CorbException{
		System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "1");
	    System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		assertEquals(3,csp.getAllContentSources().length);
		ContentSource ecs1 = null;
		ContentSource ecs2 = null;
		ContentSource ecs3 = null;
		assertHostAndPort((ecs1=csp.get()),"192.168.0.1",8001);
		assertHostAndPort((ecs2=csp.get()),"192.168.0.2",8002);
		assertHostAndPort((ecs3=csp.get()),"192.168.0.3",8003);
		csp.error(csp.getContentSourceFromProxy(ecs1));
		csp.error(csp.getContentSourceFromProxy(ecs2));
		csp.error(csp.getContentSourceFromProxy(ecs3));
		csp.get();	
	}
	
	@Test
	public void tryToTestRandomPolicy() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "RANDOM");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		assertTrue(Arrays.asList(new String[]{"192.168.0.1","192.168.0.2"}).contains(csp.get().getConnectionProvider().getHostName()));
		assertTrue(Arrays.asList(new String[]{"192.168.0.1","192.168.0.2"}).contains(csp.get().getConnectionProvider().getHostName()));
	}
	
	@Test
	public void testRandomPolicyWithOneError() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "RANDOM");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		assertTrue(Arrays.asList(new String[]{"192.168.0.1","192.168.0.2"}).contains(csp.get().getConnectionProvider().getHostName()));
		assertTrue(Arrays.asList(new String[]{"192.168.0.1","192.168.0.2"}).contains(csp.get().getConnectionProvider().getHostName()));
		csp.error(csp.getAllContentSources()[0]);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
	}
	
	@Test
	public void testLoadPolicy() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "LOAD");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		ContentSource[] csList = csp.getAllContentSources();
		assertEquals(2,csList.length);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		assertHostAndPort(csp.get(),"192.168.0.1",8001); //should get the same host as there is no load
		csp.hold(csList[0]);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		csp.release(csList[0]);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
	}
	
	@Test
	public void testLoadPolicy2() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "LOAD");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		ContentSource[] csList = csp.getAllContentSources();
		assertEquals(2,csList.length);
		csp.hold(csList[0]);
		csp.hold(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		csp.hold(csList[0]);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
	}
	
	@Test
	public void testLoadPolicyWithOneError() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "LOAD");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		ContentSource[] csList = csp.getAllContentSources();
		assertEquals(3,csList.length);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		csp.error(csList[0]);
		csp.hold(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		csp.release(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
	}
	
	@Test
	public void testLoadPolicyWithTwoErrors() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "LOAD");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		ContentSource[] csList = csp.getAllContentSources();
		assertEquals(3,csList.length);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		csp.error(csList[0]);
		csp.error(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		csp.hold(csList[2]);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
	}
	
	@Test
	public void testLoadPolicyWithReactivatedContentSource() throws CorbException{
		System.setProperty(Options.CONNECTION_POLICY, "LOAD");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		ContentSource[] csList = csp.getAllContentSources();
		assertEquals(3,csList.length);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		csp.hold(csList[0]);
		csp.error(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		csp.success(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
	}
	
	@Test(expected = CorbException.class)
	public void testLoadPolicyWithAllErrors() throws CorbException{
		System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "1");
	    System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
		System.setProperty(Options.CONNECTION_POLICY, "LOAD");
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002"});
		ContentSource[] csList = csp.getAllContentSources();
		assertEquals(2,csList.length);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		csp.error(csList[0]);
		csp.error(csList[1]);
		assertHostAndPort(csp.get(),"192.168.0.1",8001);
		csp.error(csList[0]);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		csp.error(csList[1]);
		csp.get();
	}
	
	public void testSubmitWithMockRequest() {
		
	}
	
	public void testSubmitWithMockRequestAndError() {
		
	}
	
	public void testSubmitWithMockRequestAndErrorAndReactivate() {
		
	}
	
	public void testSubmitWithMockRequestAndErrorAndUnexpired() {
		
	}
	
	public void testSubmitWithMockRequestLoadPolicy() {
	
	}
	
	public void testSubmitWithMockRequestAndErrorLoadPolicy() {
		
	}
	
	public void testSubmitWithMockRequestAndErrorAndReactivateLoadPolicy() {
		
	}

}
