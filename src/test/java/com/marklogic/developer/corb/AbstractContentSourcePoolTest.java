package com.marklogic.developer.corb;

import org.junit.Before;
import org.junit.Test;

import com.marklogic.xcc.ContentSource;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class AbstractContentSourcePoolTest {
	@Before
	public void setUp() throws FileNotFoundException {
		clearSystemProperties();
	}
	 
	@Test
	public void testInit() {
		SSLConfig sslConfig = mock(SSLConfig.class);
		Properties props = new Properties();
		props.put("foo", "bar");
		ContentSourcePoolImpl csp = new ContentSourcePoolImpl();
		csp.init(props, sslConfig);
		assertEquals(sslConfig,csp.getSSLConfig());
		assertEquals("bar",csp.getProperty("foo"));
	}
	
	@Test
	public void testNoInit() {
		ContentSourcePoolImpl csp = new ContentSourcePoolImpl();
		assertTrue(csp.getSSLConfig().getClass().equals(TrustAnyoneSSLConfig.class));
		assertNotNull(csp.properties);
	}
	
	public void testGetIntPropertyFromSystemProperty() {
		System.setProperty("foo", "123");
		ContentSourcePoolImpl csp = new ContentSourcePoolImpl();
		assertEquals(123,csp.getIntProperty("foo"));
	}
	
	
	private class ContentSourcePoolImpl extends AbstractContentSourcePool{
		@Override public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings) {}
		@Override public ContentSource get() {return null;}
		@Override public void remove(ContentSource contentSource) {}
		@Override public boolean available() {return false;}
		@Override public ContentSource[] getAllContentSources() {return null;}
		@Override public void close() throws IOException {}		
	}
}
