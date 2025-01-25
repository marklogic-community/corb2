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

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * Standard SSL/TLS with the option to configure a custom TrustStore
 * @since 2.5.7
 */
public class OneWaySSLConfig extends AbstractSSLConfig {

    @Override
    public SSLContext getSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        loadPropertiesFile();
        TrustManager[] trustManager = getTrustManagers();
        SSLContext sslContext = getSSLContextInstance(getEnabledProtocols());
        sslContext.init(null, trustManager, null);
        return sslContext;
    }

}
