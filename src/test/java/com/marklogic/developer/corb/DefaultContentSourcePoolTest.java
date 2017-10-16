package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;

import org.junit.Before;
import org.junit.Test;

public class DefaultContentSourcePoolTest {
	@Before
	public void setUp() throws FileNotFoundException {
		clearSystemProperties();
	}
	
	@Test
	public void testInitContentSources() {
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost:8000"});
		assertTrue(csp.available());
		assertEquals(csp.getAllContentSources().length,1);
	}
	
	@Test(expected = CorbException.class)
	public void testInvalidContentSources() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost1:8000"});
		assertFalse(csp.available());
		assertEquals(csp.getAllContentSources().length,0);
		csp.get();
	}
	
	@Test
	public void testInitTwoContentSources() {
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@127.0.0.1:8000"});
		assertTrue(csp.available());
		assertEquals(csp.getAllContentSources().length,2);
	}
}
