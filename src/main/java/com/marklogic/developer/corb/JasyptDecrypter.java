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

/**
 * A decrypter implementation that uses Jasypt (Java Simplified Encryption) library
 * to decrypt encrypted property values.
 * <p>
 * This class uses reflection to interact with the Jasypt StandardPBEStringEncryptor
 * to provide decryption capabilities without requiring a direct compile-time dependency
 * on the Jasypt library.
 * </p>
 * <p>
 * Configuration is loaded from a properties file (default: jasypt.properties) which should contain:
 * </p>
 * <ul>
 * <li>jasypt.password - The passphrase used for encryption/decryption (required)</li>
 * <li>jasypt.algorithm - The encryption algorithm (optional, defaults to PBEWithMD5AndTripleDES)</li>
 * </ul>
 *
 * @see AbstractDecrypter
 */
public class JasyptDecrypter extends AbstractDecrypter {

    /**
     * Configuration properties loaded from the Jasypt properties file.
     * <p>
     * This properties object contains Jasypt-specific configuration including:
     * </p>
     * <ul>
     *   <li>{@code jasypt.password} - The passphrase used for encryption/decryption (required)</li>
     *   <li>{@code jasypt.algorithm} - The encryption algorithm to use (optional, defaults to PBEWithMD5AndTripleDES)</li>
     * </ul>
     * <p>
     * The properties are loaded during {@link #init_decrypter()} from a file specified by
     * {@link Options#JASYPT_PROPERTIES_FILE}, defaulting to "jasypt.properties" if not specified.
     * </p>
     */
    protected Properties jaspytProperties;

    /**
     * The Class object representing the Jasypt StandardPBEStringEncryptor class.
     * <p>
     * This class reference is obtained via reflection in {@link #init_decrypter()} to avoid
     * a compile-time dependency on the Jasypt library. It is used to invoke encryption/decryption
     * methods on the {@link #decrypter} instance.
     * </p>
     * <p>
     * The class is loaded using {@code Class.forName("org.jasypt.encryption.pbe.StandardPBEStringEncryptor")}.
     * </p>
     */
    protected Class<?> decrypterCls;

    /**
     * An instance of Jasypt's StandardPBEStringEncryptor used for decryption operations.
     * <p>
     * This object is created via reflection in {@link #init_decrypter()} and configured with:
     * </p>
     * <ul>
     *   <li>The encryption algorithm from {@link #jaspytProperties}</li>
     *   <li>The password/passphrase from {@link #jaspytProperties}</li>
     * </ul>
     * <p>
     * The decrypter instance is used in {@link #doDecrypt(String, String)} to perform actual
     * decryption operations. If initialization fails or the password is not provided, this
     * will remain {@code null}, and decryption operations will return the original values.
     * </p>
     */
    protected Object decrypter;

    /**
     * Logger instance for this class.
     */
    protected static final Logger LOG = Logger.getLogger(JasyptDecrypter.class.getName());

    /**
     * Initializes the Jasypt decrypter by loading configuration properties and
     * setting up the StandardPBEStringEncryptor instance via reflection.
     * <p>
     * The method performs the following steps:
     * </p>
     * <ol>
     * <li>Loads the Jasypt properties file (default: jasypt.properties)</li>
     * <li>Retrieves the encryption algorithm (default: PBEWithMD5AndTripleDES)</li>
     * <li>Retrieves the encryption password from properties</li>
     * <li>Creates and configures a StandardPBEStringEncryptor instance using reflection</li>
     * </ol>
     *
     * @throws IOException if the properties file cannot be loaded
     * @throws ClassNotFoundException if the Jasypt StandardPBEStringEncryptor class is not found in the classpath
     * @throws IllegalStateException if the Jasypt library cannot be initialized properly
     */
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

    /**
     * Decrypts the given encrypted value using the configured Jasypt decrypter.
     * <p>
     * If decryption fails (e.g., value is not encrypted or invalid), the original
     * value is returned. This allows the method to handle both encrypted and
     * plain-text values gracefully.
     * </p>
     *
     * @param property the name of the property being decrypted (used for logging)
     * @param value the encrypted value to decrypt
     * @return the decrypted value (trimmed) if decryption succeeds, otherwise the original value
     */
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
