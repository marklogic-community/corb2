/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
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

import static com.marklogic.developer.corb.Options.XCC_CONNECTION_URI;
import static com.marklogic.developer.corb.Options.XCC_DBNAME;
import static com.marklogic.developer.corb.Options.XCC_HOSTNAME;
import static com.marklogic.developer.corb.Options.XCC_PASSWORD;
import static com.marklogic.developer.corb.Options.XCC_PORT;
import static com.marklogic.developer.corb.Options.XCC_USERNAME;
import static com.marklogic.developer.corb.util.StringUtils.getXccUri;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractDecrypter implements Decrypter {

    protected Properties properties;
    private static final Pattern ENCRYPTED_VALUE_REGEX = Pattern.compile("^ENC\\((.*)\\)$");

    @Override
    public void init(Properties properties) throws IOException, ClassNotFoundException {
        this.properties = properties == null ? new Properties() : properties;

        init_decrypter();
    }

    @Override
    public String getConnectionURI(String uri, String username, String password, String host, String port, String dbname) {
        if (uri != null) {
            return decrypt(XCC_CONNECTION_URI, uri);
        } else {
            return getXccUri(decrypt(XCC_USERNAME, username), 
                    decrypt(XCC_PASSWORD, password), 
                    decrypt(XCC_HOSTNAME, host), 
                    decrypt(XCC_PORT, port), 
                    dbname == null ?  null : decrypt(XCC_DBNAME, dbname));
        }
    }

    @Override
    public String decrypt(String property, String value) {
        String val = value;
        Matcher match = ENCRYPTED_VALUE_REGEX.matcher(value);
        if (match.matches()) {
            val = match.group(1);
        }
        return doDecrypt(property, val);
    }

    protected abstract void init_decrypter() throws IOException, ClassNotFoundException;

    protected abstract String doDecrypt(String property, String value);

    protected String getProperty(String key) {
        String val = System.getProperty(key);
        if (isBlank(val) && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }
}
