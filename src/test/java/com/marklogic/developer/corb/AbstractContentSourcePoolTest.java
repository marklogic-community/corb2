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
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class AbstractContentSourcePoolTest {
	@Before
	public void setUp() throws FileNotFoundException {
		clearSystemProperties();
	}
		
	@Test
    public void testInitWithNullSSLConfig() {
        Properties properties = new Properties();
        properties.put("foo", "bar");
        SSLConfig sslConfig = null;
        AbstractContentSourcePool csp = new AbstractContentSourcePoolImpl();
        csp.init(properties, sslConfig);
        assertEquals("bar", csp.getProperty("foo"));
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
		props.put("foo", "bar");
		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
		csp.init(props, sslConfig);
		assertEquals(sslConfig,csp.sslConfig());
		assertEquals("bar",csp.getProperty("foo"));
	}
	
	@Test
	public void testGetIntPropertyFromSystemProperty() {
		System.setProperty("foo", "123");
		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
		assertEquals(123,csp.getIntProperty("foo"));
	}
	
	@Test
    public void testPrepareContentSource() {
		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource cs = csp.createContentSource("xcc://user:pass@localhost:8000");
        assertEquals("localhost",cs.getConnectionProvider().getHostName());
        assertEquals(8000,cs.getConnectionProvider().getPort());
    }

    @Test
    public void testPrepareContentSourceSecureXCC() {
    		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource cs = csp.createContentSource("xccs://user:pass@localhost:8000");
        assertEquals("localhost",cs.getConnectionProvider().getHostName());
        assertEquals(8000,cs.getConnectionProvider().getPort());
    }
    
    @Test
    public void testPrepareContentSourceNoScheme() throws CorbException {
    		AbstractContentSourcePoolImpl csp = new AbstractContentSourcePoolImpl();
        ContentSource cs = csp.createContentSource("//user:pass@localhost:8000");
        assertNull(cs);
    }
	
    public class AbstractContentSourcePoolImpl extends AbstractContentSourcePool {
        @Override
        public boolean available() {
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
		@Override
		public void remove(ContentSource contentSource) {
			throw new UnsupportedOperationException("Not supported");
		}
		@Override
		public ContentSource[] getAllContentSources() {
			throw new UnsupportedOperationException("Not supported");
		}
    }
}
