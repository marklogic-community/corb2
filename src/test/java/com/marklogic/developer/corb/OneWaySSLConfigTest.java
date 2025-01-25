/*
 * * Copyright (c) 2004-2025 MarkLogic Corporation
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
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.*;

import static org.junit.Assert.*;

public class OneWaySSLConfigTest {

    private static final Logger LOG = Logger.getLogger(OneWaySSLConfigTest.class.getName());

    @Test
    public void getSSLContext() {
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
    public void testGetTrustManagers() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        Properties properties = new Properties();
        properties.setProperty(SSL_TRUSTSTORE, FileUtils.getFile("keystore.jks").getAbsolutePath());

        instance.setProperties(properties);
        try {
            TrustManager[] trustManagers = instance.getTrustManagers();
            assertNotNull(trustManagers);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testGetTrustManagersWrongType() {
        OneWaySSLConfig instance = new OneWaySSLConfig();
        Properties properties = new Properties();
        properties.setProperty(SSL_TRUSTSTORE, FileUtils.getFile("keystore.jks").getAbsolutePath());
        properties.setProperty(SSL_TRUSTSTORE_TYPE, "notjks");
        instance.setProperties(properties);
        TrustManager[] trustManagers = null;
        try {
            trustManagers = instance.getTrustManagers();
            fail("should have had a problem with the truststore type");
        } catch (Exception e) {

        }
        assertNull(trustManagers);
    }

}
