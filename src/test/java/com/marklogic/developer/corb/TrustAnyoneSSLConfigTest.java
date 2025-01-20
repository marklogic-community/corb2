/*
 * * Copyright (c) 2004-2023 MarkLogic Corporation
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

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import org.junit.Test;

import static com.marklogic.developer.corb.Options.SSL_ENABLED_PROTOCOLS;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TrustAnyoneSSLConfigTest {

    private static final Logger LOG = Logger.getLogger(TrustAnyoneSSLConfigTest.class.getName());

    @Test
    public void testGetSSLContext() {
        try {
            TrustAnyoneSSLConfig instance = new TrustAnyoneSSLConfig();
            SSLContext result = instance.getSSLContext();
            assertNotNull(result);
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetEnabledCipherSuites() {
        TrustAnyoneSSLConfig instance = new TrustAnyoneSSLConfig();
        String[] result = instance.getEnabledCipherSuites();
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    public void testGetEnabledProtocols() {
        TrustAnyoneSSLConfig instance = new TrustAnyoneSSLConfig();
        String[] result = instance.getEnabledProtocols();
        assertNotNull(result);
        assertEquals(AbstractSSLConfig.DEFAULT_PROTOCOL, result[0]);
    }

    @Test
    public void testGetEnabledProtocolsSSL_ENABLED_PROTOCOLS() {
        TrustAnyoneSSLConfig instance = new TrustAnyoneSSLConfig();
        Properties properties = new Properties();
        properties.setProperty(SSL_ENABLED_PROTOCOLS, "SSLv3");
        instance.setProperties(properties);
        String[] result = instance.getEnabledProtocols();
        assertEquals("SSLv3", result[0]);
    }

    @Test
    public void testGetEnabledProtocolsUsingJdkTlsClientProtocols() {
        System.setProperty("jdk.tls.client.protocols", "foo");
        TrustAnyoneSSLConfig instance = new TrustAnyoneSSLConfig();
        String[] result = instance.getEnabledProtocols();
        System.setProperty("jdk.tls.client.protocols", "");
        assertEquals("foo", result[0]);
    }

}
