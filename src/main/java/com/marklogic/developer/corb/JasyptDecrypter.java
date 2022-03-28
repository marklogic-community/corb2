/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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

import static com.marklogic.developer.corb.AbstractManager.loadPropertiesFile;
import static com.marklogic.developer.corb.Options.JASYPT_PROPERTIES_FILE;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Properties;
import static java.util.logging.Level.INFO;
import java.util.logging.Logger;

public class JasyptDecrypter extends AbstractDecrypter {

    protected Properties jaspytProperties;
    protected Class<?> decrypterCls;
    protected Object decrypter;

    protected static final Logger LOG = Logger.getLogger(JasyptDecrypter.class.getName());

    @Override
    protected void init_decrypter() throws IOException, ClassNotFoundException {
        String decryptPropsFile = getProperty(JASYPT_PROPERTIES_FILE);
        if (decryptPropsFile == null || isBlank(decryptPropsFile)) {
            decryptPropsFile = "jasypt.properties";
        }
        jaspytProperties = loadPropertiesFile(decryptPropsFile, false);

        String algorithm = jaspytProperties.getProperty("jasypt.algorithm");
        if (isBlank(algorithm)) {
            algorithm = "PBEWithMD5AndTripleDES"; // select a secure algorithm as default
        }
        String passphrase = jaspytProperties.getProperty("jasypt.password");
        if (isNotBlank(passphrase)) {
            try {
                decrypterCls = Class.forName("org.jasypt.encryption.pbe.StandardPBEStringEncryptor");
                decrypter = decrypterCls.newInstance();
                Method setAlgorithm = decrypterCls.getMethod("setAlgorithm", String.class);
                setAlgorithm.invoke(decrypter, algorithm);

                Method setPassword = decrypterCls.getMethod("setPassword", String.class);
                setPassword.invoke(decrypter, passphrase);
                LOG.log(INFO, "Initialized JasyptDecrypter");
            } catch (ClassNotFoundException exc) {
                throw exc;
            } catch (Exception exc) {
                throw new IllegalStateException("Unable to initialize org.jasypt.encryption.pbe.StandardPBEStringEncryptor - check if jasypt libraries are in classpath", exc);
            }
        } else {
            LOG.severe("Unable to initialize jasypt decrypter. Couldn't find jasypt.password");
        }
    }

    @Override
    protected String doDecrypt(String property, String value) {
        String dValue = null;
        if (decrypter != null) {
            try {
                Method decrypt = decrypterCls.getMethod("decrypt", String.class);
                dValue = (String) decrypt.invoke(decrypter, value);
            } catch (Exception exc) {
                Throwable th = exc instanceof InvocationTargetException ? exc.getCause() : exc;
                LOG.log(INFO, MessageFormat.format("Cannot decrypt {0}. Ignore if clear text. Error: {1}", property, th.getClass().getName()));
            }
        }
        return dValue == null ? value : dValue.trim();
    }

}
