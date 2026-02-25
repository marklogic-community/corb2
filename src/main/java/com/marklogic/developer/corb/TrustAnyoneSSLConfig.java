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

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Creates a TrustManager that <b>does not</b> validate certificate chains.
 * This SSL configuration bypasses all certificate validation, accepting any certificate
 * from any source without verification.
 * <p>
 * <b>SECURITY WARNING:</b> This implementation is inherently insecure and should only be used
 * in development or testing environments. It makes your application vulnerable to
 * man-in-the-middle attacks by accepting any certificate, including self-signed or expired ones.
 * </p>
 * <p>
 * Useful for bypassing issues with self-signed certificates in non-production environments,
 * but should <b>never</b> be used in production systems where security is a concern.
 * </p>
 *
 * @author MarkLogic Corporation
 * @since 2.2.0
 */
public class TrustAnyoneSSLConfig extends OneWaySSLConfig {

    /**
     * Returns a custom TrustManager that will not perform any validation and will trust anyone.
     * <p>
     * <b>WARNING:</b> This TrustManager accepts all certificates without validation,
     * making SSL connections insecure. Use only in development or testing environments.
     * </p>
     *
     * @return an array containing a single TrustManager that trusts all certificates
     */
    @Override
    public TrustManager[] getTrustManagers() {
        return new TrustManager[]{new TrustAnyoneManager()};
    }

    /**
     * Internal implementation of X509TrustManager that accepts all certificates
     * without performing any validation checks.
     * <p>
     * This class intentionally bypasses all certificate validation by implementing
     * empty check methods and returning no accepted issuers.
     * </p>
     */
    private static class TrustAnyoneManager implements X509TrustManager {

        /**
         * Returns an empty array of certificate authority certificates which are trusted
         * for authenticating peers.
         *
         * @return an empty array of X509Certificate
         */
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        /**
         * Performs no validation on the client certificate chain.
         * This method intentionally accepts all client certificates without checking.
         *
         * @param certs the peer certificate chain (ignored)
         * @param authType the authentication type based on the client certificate (ignored)
         * @throws CertificateException never thrown by this implementation
         */
        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
            // no exception means it's okay
        }

        /**
         * Performs no validation on the server certificate chain.
         * This method intentionally accepts all server certificates without checking.
         *
         * @param certs the peer certificate chain (ignored)
         * @param authType the key exchange algorithm used (ignored)
         * @throws CertificateException never thrown by this implementation
         */
        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
            // no exception means it's okay
        }
    }
}
