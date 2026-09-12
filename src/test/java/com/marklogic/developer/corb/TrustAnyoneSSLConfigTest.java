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

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static com.marklogic.developer.corb.Options.SSL_ENABLED_PROTOCOLS;
import static com.marklogic.developer.corb.TestUtils.newProperties;
import static com.marklogic.developer.corb.TestUtils.withSystemProperty;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class TrustAnyoneSSLConfigTest {

    private static final Logger LOG = Logger.getLogger(TrustAnyoneSSLConfigTest.class.getName());

    private TrustAnyoneSSLConfig newConfig() {
        return new TrustAnyoneSSLConfig();
    }

    private static void clearJdkProtocols() {
        System.clearProperty("jdk.tls.client.protocols");
    }

    @Test
    void testGetSSLContext() {
        try {
            SSLContext result = newConfig().getSSLContext();
            assertNotNull(result);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetEnabledCipherSuites() {
        String[] result = newConfig().getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void testGetEnabledProtocols() {
        clearJdkProtocols();
        String[] result = newConfig().getEnabledProtocols();
        assertNotNull(result);
        assertEquals(AbstractSSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    void testGetEnabledProtocolsSSL_ENABLED_PROTOCOLS() {
        Properties properties = newProperties(SSL_ENABLED_PROTOCOLS, "SSLv3");
        TrustAnyoneSSLConfig instance = newConfig();
        instance.setProperties(properties);
        String[] result = instance.getEnabledProtocols();
        assertEquals("SSLv3", result[0]);
    }

    @Test
    void testGetEnabledProtocolsUsingJdkTlsClientProtocols() {
        try {
            withSystemProperty("jdk.tls.client.protocols", "foo", () -> {
                String[] result = newConfig().getEnabledProtocols();
                assertEquals("foo", result[0]);
            });
        } catch (Exception ex) {
            fail(ex);
        }
    }

    @Test
    void testGetTrustManagers() {
        TrustManager[] managers = newConfig().getTrustManagers();
        assertEquals(1, managers.length);
        assertInstanceOf(X509TrustManager.class, managers[0]);
    }

    // -------------------------------------------------------------------------
    // TrustAnyoneSSLConfig$TrustAnyoneManager (private inner class)
    // Accessed via getTrustManagers() cast to X509TrustManager
    // -------------------------------------------------------------------------

    private X509TrustManager getTrustAnyoneManager() {
        TrustManager[] managers = newConfig().getTrustManagers();
        assertEquals(1, managers.length);
        assertInstanceOf(X509TrustManager.class, managers[0]);
        return (X509TrustManager) managers[0];
    }

    @Test
    void testTrustAnyoneManagerGetAcceptedIssuers() {
        X509Certificate[] issuers = getTrustAnyoneManager().getAcceptedIssuers();
        assertNotNull(issuers);
        assertEquals(0, issuers.length);
    }

    @Test
    void testTrustAnyoneManagerCheckClientTrusted() {
        assertDoesNotThrow(() -> getTrustAnyoneManager().checkClientTrusted(null, "RSA"));
    }

    @Test
    void testTrustAnyoneManagerCheckServerTrusted() {
        assertDoesNotThrow(() -> getTrustAnyoneManager().checkServerTrusted(null, "RSA"));
    }

}
