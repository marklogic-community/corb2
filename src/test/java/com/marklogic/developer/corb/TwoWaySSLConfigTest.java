/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class TwoWaySSLConfigTest {

    public static final String SSL_PROPERTIES = "src/test/resources/SSL.properties";
    public static final String A_B_C = "a,b,c";
    private static final Logger LOG = Logger.getLogger(TwoWaySSLConfigTest.class.getName());

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    @Test
    void testGetEnabledCipherSuitesNullProperties() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    void testGetEnabledCipherSuitesNullCipherProperty() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    void testGetEnabledCipherSuites() {
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
    void testGetEnabledCipherSuitesColonSeparator() {
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
    void testGetEnabledProtocolsNullProperties() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        String[] result = instance.getEnabledProtocols();
        assertEquals(TwoWaySSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    void testGetSSLContextInstanceNoProtocols() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> instance.getSSLContextInstance(new String[]{}));
    }

    @Test
    void testGetSSLContextInstanceNoValidProtocols() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        assertThrows(NoSuchAlgorithmException.class, () -> instance.getSSLContextInstance(new String[]{"DoesNotExist"}));
    }

    @Test
    void testGetEnabledProtocolsNullProtocols() {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledProtocols();
        assertEquals(TwoWaySSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    void testGetEnabledProtocols() {
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
    void testGetSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException {
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        Properties props = new Properties();
        props.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        instance.setProperties(props);
        SecurityOptions securityOptions = instance.getSecurityOptions();
        assertEquals(10, securityOptions.getEnabledCipherSuites().length);
    }

    @Test
    void testLoadPropertiesFileNullSSLPropertiesFile() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        System.clearProperty(Options.SSL_PROPERTIES_FILE);
    }

    @Test
    void testLoadPropertiesFileDirectory() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, "src/test/resources");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        assertThrows(IllegalStateException.class, instance::loadPropertiesFile);
    }

    @Test
    void testLoadPropertiesFileWithEmptyProperties() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.properties = new Properties();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals("ECDHE-ECDSA-AES256-GCM-SHA384", instance.getEnabledCipherSuites()[0]);
    }

    @Test
    void testLoadPropertiesFileWithNullProperties() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.properties = null;
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals("ECDHE-ECDSA-AES256-GCM-SHA384", instance.getEnabledCipherSuites()[0]);
    }

    @Test
    void testLoadPropertiesFileDoesNotExist() {
        System.setProperty(Options.SSL_PROPERTIES_FILE, "");
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        instance.loadPropertiesFile();
        assertNull(instance.properties);
    }

    /**
     * Test of getSSLContext method, of class TwoWaySSLConfig.
     */
    @Test
    void testGetSSLContextNoProperties() {
        System.clearProperty(Options.SSL_PROPERTIES_FILE);
        TwoWaySSLConfig instance = new TwoWaySSLConfig();
        assertThrows(IllegalStateException.class, instance::getSSLContext);
    }

    @Test
    void testGetSSLContext() {
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
    void testGetSSLContextWithEncryptedValues() {
        testGetSSLContext("changeit");
    }

    @Test
    void testGetSSLContextWithNullUnencryptedValues() {
        testGetSSLContext(null);
    }

    static void testGetSSLContext(String valueToReturn) {
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
