package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.util.Properties;

import com.marklogic.xcc.ContentSource;
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
        assertEquals(DefaultContentSourcePool.CONNECTION_POLICY_ROUND_ROBIN, csp.connectionPolicy);
	}

	@Test(expected = CorbException.class)
	public void testInitInvalidContentSources() throws CorbException{
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost1:8000"});
		assertFalse(csp.available());
		assertEquals(csp.getAllContentSources().length,0);
		csp.get();
	}

    @Test(expected = NullPointerException.class)
    public void testInitNullConnectionStrings() throws CorbException{
        DefaultContentSourcePool csp = new DefaultContentSourcePool();
        csp.init(null, null, null);
    }

	@Test
	public void testInitTwoContentSources() {
		DefaultContentSourcePool csp = new DefaultContentSourcePool();
		csp.init(null, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@localhost:8010"});
		assertTrue(csp.available());
		assertEquals(2, csp.getAllContentSources().length);
	}

    @Test
    public void testInitConnectionPolicyRandom() {
	    Properties properties = new Properties();
	    properties.setProperty(Options.CONNECTION_POLICY, DefaultContentSourcePool.CONNECTION_POLICY_RANDOM);
        DefaultContentSourcePool csp = new DefaultContentSourcePool();
        csp.init(properties, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@localhost:8010","xcc://foo:bar@localhost:8020"});
        assertEquals(DefaultContentSourcePool.CONNECTION_POLICY_RANDOM, csp.connectionPolicy);
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
    }

    @Test
    public void testInitConnectionPolicyLoad() {
        Properties properties = new Properties();
        properties.setProperty(Options.CONNECTION_POLICY, DefaultContentSourcePool.CONNECTION_POLICY_LOAD);
        DefaultContentSourcePool csp = new DefaultContentSourcePool();
        csp.init(properties, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@localhost:8010","xcc://foo:bar@localhost:8020"});

        assertEquals(DefaultContentSourcePool.CONNECTION_POLICY_LOAD, csp.connectionPolicy);
        assertTrue(csp.isLoadPolicy());
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
    }

    @Test
    public void testInitConnectionPolicyRoundRobin() {
        DefaultContentSourcePool csp = initRoundRobinPool();

        assertEquals(DefaultContentSourcePool.CONNECTION_POLICY_ROUND_ROBIN, csp.connectionPolicy);
        assertEquals(3, csp.getAvailableContentSources().size());
        ContentSource firstContentSource = csp.nextContentSource();
        assertNotNull(firstContentSource);
        assertNotNull(csp.nextContentSource());
        assertNotNull(csp.nextContentSource());
        assertEquals(firstContentSource, csp.nextContentSource());
        csp.remove(firstContentSource);
        assertEquals(2, csp.getAvailableContentSources().size());
        csp.remove(csp.nextContentSource());
        csp.remove(csp.nextContentSource());
        csp.remove(csp.nextContentSource());
        assertNull(csp.nextContentSource());
    }

    @Test
    public void testError(){
        DefaultContentSourcePool csp = initRoundRobinPool();
        ContentSource cs = csp.nextContentSource();
        assertEquals(0, csp.errorCount(cs));
        csp.error(cs);
        assertEquals(1, csp.errorCount(cs));
        csp.success(cs);
        assertEquals(0, csp.errorCount(cs));
        csp.error(cs);
        csp.error(cs);
        assertEquals(2, csp.errorCount(cs));
        csp.error(cs);
        assertEquals(3, csp.errorCount(cs));
        csp.error(cs);
        assertEquals(0, csp.errorCount(cs));
    }

    @Test
    public void testHoldAndRelease() {
        DefaultContentSourcePool csp = initRoundRobinPool();
        ContentSource cs = csp.nextContentSource();
        assertNull(csp.connectionCountsMap.get(cs));
        csp.hold(cs);
        assertEquals(1, csp.connectionCountsMap.get(cs).intValue());
        csp.hold(cs);
        assertEquals(2, csp.connectionCountsMap.get(cs).intValue());
        csp.release(cs);
        assertEquals(1, csp.connectionCountsMap.get(cs).intValue());
        csp.release(cs);
        assertEquals(0, csp.connectionCountsMap.get(cs).intValue());
    }

    private DefaultContentSourcePool initRoundRobinPool() {
        Properties properties = new Properties();
        properties.setProperty(Options.CONNECTION_POLICY, DefaultContentSourcePool.CONNECTION_POLICY_ROUND_ROBIN);
        DefaultContentSourcePool csp = new DefaultContentSourcePool();
        csp.init(properties, null, new String[] {"xcc://foo:bar@localhost:8000","xcc://foo:bar@localhost:8010","xcc://foo:bar@localhost:8020"});
        return csp;
    }
}
