/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 * *
 * * The use of the Apache License does not indicate that this project is
 * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.jupiter.api.Assertions.*;

class OneWaySSLConfigTest {

    private static final Logger LOG = Logger.getLogger(OneWaySSLConfigTest.class.getName());

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
        System.clearProperty("jdk.tls.client.protocols");
    }

    @Test
    void getSSLContext() {
        try {
            SSLConfig instance = new OneWaySSLConfig();
            SSLContext context = instance.getSSLContext();
            assertNotNull(context);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetTrustManagersWithoutCustomTrustStore() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        instance.setProperties(new Properties());

        try {
            assertNull(instance.getTrustManagers());
        } catch (NoSuchAlgorithmException e) {
            fail("Unexpected JDK trust manager algorithm failure");
        }
    }

    @Test
    void testGetTrustManagers() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        instance.setProperties(newTrustStoreProperties());

        try {
            TrustManager[] trustManagers = instance.getTrustManagers();
            assertNotNull(trustManagers);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    void testGetTrustManagersWrongType() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        Properties properties = newTrustStoreProperties();
        properties.setProperty(SSL_TRUSTSTORE_TYPE, "notjks");
        instance.setProperties(properties);
        TrustManager[] trustManagers = null;
        try {
            trustManagers = instance.getTrustManagers();
            fail("should have had a problem with the truststore type");
        } catch (Exception e) {
            // expected
        }
        assertNull(trustManagers);
    }

    @Test
    void testGetEnabledProtocolsDefaultsWhenUnset() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        instance.setProperties(new Properties());

        String[] protocols = instance.getEnabledProtocols();

        assertNotNull(protocols);
        assertEquals(AbstractSSLConfig.DEFAULT_PROTOCOL, protocols[0]);
    }

    @Test
    void testGetEnabledProtocolsUsesJdkTlsClientProtocolsFallback() {
        String existing = System.getProperty("jdk.tls.client.protocols");
        System.setProperty("jdk.tls.client.protocols", "TLSv1.1:TLSv1.2");

        try {
            OneWaySSLConfig instance = new OneWaySSLConfig();
            instance.setProperties(new Properties());

            String[] protocols = instance.getEnabledProtocols();

            assertArrayEquals(new String[]{"TLSv1.1", "TLSv1.2"}, protocols);
        } finally {
            if (existing == null) {
                System.clearProperty("jdk.tls.client.protocols");
            } else {
                System.setProperty("jdk.tls.client.protocols", existing);
            }
        }
    }

    private Properties newTrustStoreProperties() {
        Properties properties = new Properties();
        properties.setProperty(SSL_TRUSTSTORE, FileUtils.getFile("keystore.jks").getAbsolutePath());
        return properties;
    }
}
