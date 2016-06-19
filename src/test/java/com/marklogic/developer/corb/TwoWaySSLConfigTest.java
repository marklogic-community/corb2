/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.util.Properties;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TwoWaySSLConfigTest {

    public static final String SSL_PROPERTIES = "src/test/resources/SSL.properties";
    public static final String SSLV3 = "SSLv3";
    
    @Before
    public void setUp() {
        clearSystemProperties();
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    /**
     * Test of getEnabledCipherSuites method, of class TwoWaySSLConfig.
     */
    @Test
    public void testGetEnabledCipherSuites_nullProperties() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledCipherSuites_nullCipherProperty() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledCipherSuites() {
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
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledProtocols();
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledProtocols_nullProtocols() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledProtocols();
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledProtocols_null() {
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
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_directory() throws Exception {
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, "src/test/resources");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        fail();
    }

    @Test
    public void testLoadPropertiesFile() throws Exception {
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals(instance.getEnabledCipherSuites()[1], SSLV3);
    }

    @Test
    public void testLoadPropertiesFile_withNullProperties() throws Exception {
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.properties = null;
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals(instance.getEnabledCipherSuites()[1], SSLV3);
    }
    
    @Test
    public void testLoadPropertiesFile_doesNotExist() throws Exception {
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, "");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNull(instance.properties);
    }

    /**
     * Test of getSSLContext method, of class TwoWaySSLConfig.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetSSLContext_NoProperties() throws Exception {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.getSSLContext();
        fail();
    }
    /* TODO: uncomment when building with JDK 1.7+, currently throwing NoSuchAlgorithm when building with 1.6

    @Test
    public void testGetSSLContext() throws Exception {
        System.setProperty(TwoWaySSLConfig.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        SSLContext context = instance.getSSLContext();
        assertNotNull(context);
    }
     */
}
