/*
 * Copyright (c) 2004-2021 MarkLogic Corporation
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
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

import com.marklogic.xcc.SecurityOptions;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TwoWaySSLConfigTest {

    public static final String SSL_PROPERTIES = "src/test/resources/SSL.properties";
    public static final String A_B_C = "a,b,c";
    private static final Logger LOG = Logger.getLogger(TwoWaySSLConfigTest.class.getName());

    @Before
    public void setUp() {
        clearSystemProperties();
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    @Test
    public void testGetEnabledCipherSuitesNullProperties() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledCipherSuitesNullCipherProperty() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledCipherSuites() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(Options.SSL_CIPHER_SUITES, A_B_C);
        instance.setProperties(props);
        String[] result = instance.getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }

    @Test
    public void testGetEnabledCipherSuitesColonSeparator() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(Options.SSL_CIPHER_SUITES, "a:b:c");
        instance.setProperties(props);
        String[] result = instance.getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }

    @Test
    public void testGetEnabledProtocolsNullProperties() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledProtocols();
        assertEquals(TwoWaySSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test (expected=ArrayIndexOutOfBoundsException.class)
    public void testGetSSLContextInstanceNoProtocols() throws NoSuchAlgorithmException {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.getSSLContextInstance(new String[]{});
    }

    @Test (expected=NoSuchAlgorithmException.class)
    public void testGetSSLContextInstanceNoValidProtocols() throws NoSuchAlgorithmException {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.getSSLContextInstance(new String[]{"DoesNotExist"});
    }

    @Test
    public void testGetEnabledProtocolsNullProtocols() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledProtocols();
        assertEquals(TwoWaySSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    public void testGetEnabledProtocols() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(Options.SSL_ENABLED_PROTOCOLS, A_B_C);
        instance.setProperties(props);
        String[] result = instance.getEnabledProtocols();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }

    @Test
    public void testGetSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        instance.setProperties(props);
        SecurityOptions securityOptions = instance.getSecurityOptions();
        assertEquals(10, securityOptions.getEnabledCipherSuites().length);
    }

    @Test
    public void testLoadPropertiesFileNullSSLPropertiesFile() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        System.clearProperty(Options.SSL_PROPERTIES_FILE);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFileDirectory() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, "src/test/resources");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        fail();
    }

    @Test
    public void testLoadPropertiesFileWithEmptyProperties() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.properties = new Properties();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals("ECDHE-ECDSA-AES256-GCM-SHA384", instance.getEnabledCipherSuites()[0]);
    }

    @Test
    public void testLoadPropertiesFileWithNullProperties() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.properties = null;
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals("ECDHE-ECDSA-AES256-GCM-SHA384", instance.getEnabledCipherSuites()[0]);
    }

    @Test
    public void testLoadPropertiesFileDoesNotExist() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, "");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNull(instance.properties);
    }

    /**
     * Test of getSSLContext method, of class TwoWaySSLConfig.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetSSLContextNoProperties() {
        try {
            System.clearProperty(Options.SSL_PROPERTIES_FILE);
            TwoWaySSLConfig instance = new TwoWaySSLConfig();
            instance.getSSLContext();
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetSSLContext() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        try {
            SSLContext context = instance.getSSLContext();
            assertNotNull(context);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        } finally {
            System.clearProperty(Options.SSL_PROPERTIES_FILE);
        }
    }

    @Test
    public void testGetSSLContextWithEncryptedValues() {
        testGetSSLContext("changeit");
    }

    @Test
    public void testGetSSLContextWithNullUnencryptedValues() {
        testGetSSLContext(null);
    }

    public void testGetSSLContext(String valueToReturn) {
        Decrypter mockDecrypter = mock(Decrypter.class);
        when(mockDecrypter.decrypt(anyString(), anyString())).thenReturn(valueToReturn);

        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.decrypter = mockDecrypter;
        try {
            SSLContext context = instance.getSSLContext();
            assertNotNull(context);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        } finally {
            System.clearProperty(Options.SSL_PROPERTIES_FILE);
        }
    }
}
