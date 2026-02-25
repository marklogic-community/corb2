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

import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract base class for implementing decryption strategies.
 * Provides common functionality for decrypting encrypted property values,
 * including pattern matching for encrypted value detection and property retrieval.
 * Encrypted values are expected to be in the format "ENC(encrypted_value)".
 * Subclasses must implement the specific decryption algorithm.
 */
public abstract class AbstractDecrypter implements Decrypter {

    /**
     * Configuration properties containing decryption settings such as encryption keys,
     * algorithm parameters, and other configuration values needed for decryption.
     * Initialized in {@link #init(Properties)} and used by subclasses for decryption operations.
     */
    protected Properties properties;

    /**
     * Regular expression pattern to detect encrypted values in the format "ENC(...)".
     * The pattern matches strings that start with "ENC(", contain any content, and end with ")".
     * The content within the parentheses (capture group 1) is extracted for decryption.
     * For example, "ENC(myEncryptedValue)" matches and captures "myEncryptedValue".
     */
    private static final Pattern ENCRYPTED_VALUE_REGEX = Pattern.compile("^ENC\\((.*)\\)$");

    /**
     * Initializes the decrypter with configuration properties.
     * If properties are null, an empty Properties object is used.
     * Delegates to {@link #init_decrypter()} for subclass-specific initialization.
     *
     * @param properties configuration properties for the decrypter, may be null
     * @throws IOException if an I/O error occurs during initialization
     * @throws ClassNotFoundException if a required class cannot be found during initialization
     */
    @Override
    public void init(Properties properties) throws IOException, ClassNotFoundException {
        this.properties = properties == null ? new Properties() : properties;

        init_decrypter();
    }

    /**
     * Decrypts a property value.
     * If the value is wrapped in "ENC(...)", extracts the encrypted content and decrypts it.
     * Otherwise, decrypts the value as-is.
     * Delegates to {@link #doDecrypt(String, String)} for the actual decryption.
     *
     * @param property the property name (may be used by decryption algorithm)
     * @param value the value to decrypt (may be in "ENC(...)" format)
     * @return the decrypted value
     */
    @Override
    public String decrypt(String property, String value) {
        String val = value;
        Matcher match = ENCRYPTED_VALUE_REGEX.matcher(value);
        if (match.matches()) {
            val = match.group(1);
        }
        return doDecrypt(property, val);
    }

    /**
     * Performs subclass-specific initialization.
     * Called by {@link #init(Properties)} after properties are set.
     * Subclasses should override this method to perform any necessary setup,
     * such as loading encryption keys or initializing cipher algorithms.
     *
     * @throws IOException if an I/O error occurs during initialization
     * @throws ClassNotFoundException if a required class cannot be found during initialization
     */
    protected abstract void init_decrypter() throws IOException, ClassNotFoundException;

    /**
     * Performs the actual decryption of the property value.
     * This method is called by {@link #decrypt(String, String)} after extracting
     * the encrypted content from the "ENC(...)" wrapper (if present).
     * Subclasses must implement the specific decryption algorithm.
     *
     * @param property the property name (may be used by decryption algorithm)
     * @param value the encrypted value to decrypt (without "ENC(...)" wrapper)
     * @return the decrypted value
     */
    protected abstract String doDecrypt(String property, String value);

    /**
     * Retrieves a property value by key.
     * First checks system properties, then falls back to instance properties.
     * The returned value is trimmed of leading and trailing whitespace.
     *
     * @param key the property key name
     * @return the trimmed property value, or null if not found or blank
     */
    protected String getProperty(String key) {
        String val = System.getProperty(key);
        if (isBlank(val) && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }
}
