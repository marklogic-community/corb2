package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;

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
		csp.error(csp.getContentSourceFromProxy(ecs));
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		assertHostAndPort(csp.get(),"192.168.0.2",8002);
	}
	
	@Test
	public void testRoundRobinPolicyWithTwoErrors() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@192.168.0.1:8001","xcc://foo:bar@192.168.0.2:8002","xcc://foo:bar@192.168.0.3:8003"});
		assertEquals(3,csp.getAllContentSources().length);
		ContentSource ecs = null;
		assertHostAndPort((ecs=csp.get()),"192.168.0.1",8001);
		csp.error(csp.getContentSourceFromProxy(ecs));
		assertHostAndPort((ecs=csp.get()),"192.168.0.2",8002);
		csp.error(csp.getContentSourceFromProxy(ecs));
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
		assertHostAndPort(csp.get(),"192.168.0.3",8003);
	}
	
	public void testRoundRobinPolicyWithReactivatedContentSource() throws CorbException{
		
	}
	
	public void testRoundRobinPolicyWithAllErrors() throws CorbException{
		
	}
	
	public void tryToTestRandomPolicy() throws CorbException{
		
	}
		
	public void testRandomPolicyWithOneError() throws CorbException{
		
	}
	
	public void testLoadPolicy() throws CorbException{
		
	}
	
	public void testLoadPolicyWithOneError() throws CorbException{
		
	}
	
	public void testLoadPolicyWithTwoErrors() throws CorbException{
		
	}
	
	public void testLoadPolicyWithReactivatedContentSource() throws CorbException{
		
	}
	
	public void testLoadPolicyWithAllErrors() throws CorbException{
		
	}

}
