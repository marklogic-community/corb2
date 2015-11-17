/*
 */
package com.marklogic.developer.corb;

import java.util.Properties;
import javax.net.ssl.SSLContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author mhansen
 */
public class TwoWaySSLConfigTest {
    
    public static final String SSL_PROPERTIES = "src/test/resources/SSL.properties";
    
    public TwoWaySSLConfigTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of getEnabledCipherSuites method, of class TwoWaySSLConfig.
     */
    @Test
    public void testGetEnabledCipherSuites_nullProperties() {
        System.out.println("getEnabledCipherSuites");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledCipherSuites();
        assertNull(result);
    }
    
    @Test
    public void testGetEnabledCipherSuites_nullCipherProperty() {
        System.out.println("getEnabledCipherSuites");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledCipherSuites();
        assertNull(result);
    }
    
    @Test
    public void testGetEnabledCipherSuites() {
        System.out.println("getEnabledCipherSuites");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(TwoWaySSLConfig.SSL_CIPHER_SUITES, "a,b,c");
        instance.setProperties(props);
        String[] result = instance.getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }
    /**
     * Test of getEnabledProtocols method, of class TwoWaySSLConfig.
     */
    @Test
    public void testGetEnabledProtocols_nullProperties() {
        System.out.println("getEnabledProtocols");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledProtocols();
        assertNull(result);
    }

    @Test
    public void testGetEnabledProtocols_nullProtocols() {
        System.out.println("getEnabledProtocols");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledProtocols();
        assertNull(result);
    }
    
    @Test
    public void testGetEnabledProtocols_null() {
        System.out.println("getEnabledProtocols");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(TwoWaySSLConfig.SSL_ENABLED_PROTOCOLS, "a,b,c");
        instance.setProperties(props);
        String[] result = instance.getEnabledProtocols();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }
    
    /**
     * Test of loadPropertiesFile method, of class TwoWaySSLConfig.
     */
    @Test
    public void testLoadPropertiesFile_nullSSLPropertiesFile() throws Exception {
        System.out.println("loadPropertiesFile");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
    }

     @Test (expected = IllegalStateException.class)
    public void testLoadPropertiesFile_directory() throws Exception {
        System.out.println("loadPropertiesFile");
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, "src/test/resources");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
    }
    
    @Test
    public void testLoadPropertiesFile() throws Exception {
        System.out.println("loadPropertiesFile");
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals(instance.getEnabledCipherSuites()[1], "SSLv3");
    }
    
    @Test
    public void testLoadPropertiesFile_doesNotExist() throws Exception {
        System.out.println("loadPropertiesFile");
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, "");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNull(instance.properties);
    }
    
    /**
     * Test of getSSLContext method, of class TwoWaySSLConfig.
     */
    @Test (expected = IllegalStateException.class)
    public void testGetSSLContext_NoProperties() throws Exception {
        System.out.println("getSSLContext");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        SSLContext context = instance.getSSLContext();
    }
   
    @Test
    public void testGetSSLContext() throws Exception {
        System.out.println("getSSLContext");
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        SSLContext context = instance.getSSLContext();
        assertNotNull(context);
    }
}
