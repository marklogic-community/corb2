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
import static com.marklogic.developer.corb.TestUtils.newProperties;
import static com.marklogic.developer.corb.TestUtils.withSystemProperty;
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

    private TwoWaySSLConfig newConfig() {
        return new TwoWaySSLConfig();
    }

    private static void clearJdkProtocols() {
        System.clearProperty("jdk.tls.client.protocols");
    }

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
        String[] result = newConfig().getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    void testGetEnabledCipherSuitesNullCipherProperty() {
        TwoWaySSLConfig instance = newConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledCipherSuites();
        assertEquals(0, result.length);
    }

    @Test
    void testGetEnabledCipherSuites() {
        TwoWaySSLConfig instance = newConfig();
        instance.setProperties(newProperties(Options.SSL_CIPHER_SUITES, A_B_C));
        String[] result = instance.getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }

    @Test
    void testGetEnabledCipherSuitesColonSeparator() {
        TwoWaySSLConfig instance = newConfig();
        instance.setProperties(newProperties(Options.SSL_CIPHER_SUITES, "a:b:c"));
        String[] result = instance.getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }

    @Test
    void testGetEnabledProtocolsNullProperties() {
        clearJdkProtocols();
        String[] result = newConfig().getEnabledProtocols();
        assertEquals(TwoWaySSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    void testGetSSLContextInstanceNoProtocols() {
        TwoWaySSLConfig instance = newConfig();
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> instance.getSSLContextInstance(new String[]{}));
    }

    @Test
    void testGetSSLContextInstanceNoValidProtocols() {
        TwoWaySSLConfig instance = newConfig();
        assertThrows(NoSuchAlgorithmException.class, () -> instance.getSSLContextInstance(new String[]{"DoesNotExist"}));
    }

    @Test
    void testGetEnabledProtocolsNullProtocols() {
        TwoWaySSLConfig instance = newConfig();
        instance.setProperties(new Properties());
        String[] result = instance.getEnabledProtocols();
        assertEquals(TwoWaySSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    void testGetEnabledProtocols() {
        TwoWaySSLConfig instance = newConfig();
        instance.setProperties(newProperties(Options.SSL_ENABLED_PROTOCOLS, A_B_C));
        String[] result = instance.getEnabledProtocols();
        assertNotNull(result);
        assertEquals(3, result.length);
        assertEquals("a", result[0]);
        assertEquals("c", result[2]);
    }

    @Test
    void testGetSecurityOptions() throws NoSuchAlgorithmException, KeyManagementException {
        TwoWaySSLConfig instance = newConfig();
        instance.setProperties(newProperties(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES));
        SecurityOptions securityOptions = instance.getSecurityOptions();
        assertEquals(10, securityOptions.getEnabledCipherSuites().length);
    }

    @Test
    void testLoadPropertiesFileNullSSLPropertiesFile() {
        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES, () -> {
            TwoWaySSLConfig instance = newConfig();
            instance.loadPropertiesFile();
            assertNotNull(instance.properties);
        }));
    }

    @Test
    void testLoadPropertiesFileDirectory() {
        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, "src/test/resources", () -> {
            TwoWaySSLConfig instance = newConfig();
            assertThrows(IllegalStateException.class, instance::loadPropertiesFile);
        }));
    }

    @Test
    void testLoadPropertiesFileWithEmptyProperties() {
        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES, () -> {
        TwoWaySSLConfig instance = newConfig();
        instance.properties = new Properties();
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals("ECDHE-ECDSA-AES256-GCM-SHA384", instance.getEnabledCipherSuites()[0]);
        }));
    }

    @Test
    void testLoadPropertiesFileWithNullProperties() {
        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES, () -> {
        TwoWaySSLConfig instance = newConfig();
        instance.properties = null;
        instance.loadPropertiesFile();
        assertNotNull(instance.properties);
        assertEquals("ECDHE-ECDSA-AES256-GCM-SHA384", instance.getEnabledCipherSuites()[0]);
        }));
    }

    @Test
    void testLoadPropertiesFileDoesNotExist() {
        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, "", () -> {
            TwoWaySSLConfig instance = newConfig();
            instance.loadPropertiesFile();
            assertNull(instance.properties);
        }));
    }

    /**
     * Test of getSSLContext method, of class TwoWaySSLConfig.
     */
    @Test
    void testGetSSLContextNoProperties() {
        TwoWaySSLConfig instance = newConfig();
        assertThrows(IllegalStateException.class, instance::getSSLContext);
    }

    @Test
    void testGetSSLContext() {
        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES, () -> {
            SSLContext context = newConfig().getSSLContext();
            assertNotNull(context);
        }));
    }

    @Test
    void testGetSSLContextWithEncryptedValues() {
        testGetSSLContext("changeit");
    }

    @Test
    void testGetSSLContextWithNullUnencryptedValues() {
        testGetSSLContext(null);
    }

    void testGetSSLContext(String valueToReturn) {
        Decrypter mockDecrypter = mock(Decrypter.class);
        when(mockDecrypter.decrypt(anyString(), anyString())).thenReturn(valueToReturn);

        assertDoesNotThrow(() -> withSystemProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES, () -> {
            TwoWaySSLConfig instance = newConfig();
            instance.decrypter = mockDecrypter;
            SSLContext context = instance.getSSLContext();
            assertNotNull(context);
        }));
    }

    @Test
    void testGetSecurityOptionsSetsProtocolsAndCipherSuites() throws Exception {
        System.setProperty(Options.SSL_PROPERTIES_FILE, SSL_PROPERTIES);
        TwoWaySSLConfig instance = newConfig();
        SecurityOptions securityOptions = instance.getSecurityOptions();
        assertNotNull(securityOptions.getEnabledCipherSuites());
        assertNotNull(securityOptions.getEnabledProtocols());
    }

    @Test
    void testGetEnabledProtocolsUsesJdkFallbackAfterClearingProperty() {
        assertDoesNotThrow(() -> withSystemProperty("jdk.tls.client.protocols", "TLSv1.1:TLSv1.2", () -> {
            TwoWaySSLConfig instance = newConfig();
            String[] result = instance.getEnabledProtocols();
            assertArrayEquals(new String[]{"TLSv1.1", "TLSv1.2"}, result);
        }));
    }
}
