/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
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

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Creates a TrustManager that <b>does not</b> validate certificate chains.
 * Useful for bypassing issues with self-signed certs, but should be used with caution.
 *
 * @since 2.2.0
 */
public class TrustAnyoneSSLConfig extends OneWaySSLConfig {

    /**
     * Returns a custom TrustManager that will not perform any validation and will trust anyone.  Use with caution!
     * @return
     */
    @Override
    public TrustManager[] getTrustManagers() {
        return new TrustManager[]{new TrustAnyoneManager()};
    }

    private static class TrustAnyoneManager implements X509TrustManager {

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
            // no exception means it's okay
        }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
            // no exception means it's okay
        }
    }
}
