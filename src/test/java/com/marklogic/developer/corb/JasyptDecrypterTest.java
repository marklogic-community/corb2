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

import com.marklogic.developer.TestHandler;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class JasyptDecrypterTest {

    private static final Logger LOG = Logger.getLogger(JasyptDecrypter.class.getName());
    private final TestHandler testLogger = new TestHandler();
    public static final String JASYPT_ALGORITHM = "jasypt.algorithm";
    public static final String JASYPT_PASSWORD = "jasypt.password";
    public static final String ERROR_NO_PASSWORD = "Unable to initialize jasypt decrypter. Couldn't find jasypt.password";
    private static final String UNENCRYPTED_PASSWORD = "corbencrypt";
    private static final String JASYPT_PROPERTIES_FILE_PATH = "src/test/resources/jasypt.properties";
    private static final String ALGORITHM = "PBEWithMD5AndTripleDES";
    private static final String PASSWORD = "password";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
        LOG.addHandler(testLogger);
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    /**
     * Creates a JasyptDecrypter with JASYPT_PROPERTIES_FILE set to the standard test file, plus any
     * additional option key/value pairs supplied as consecutive vararg strings.
     */
    private JasyptDecrypter decrypterWithFile(String... extraOptionPairs) {
        return decrypterWithFilePath(JASYPT_PROPERTIES_FILE_PATH, extraOptionPairs);
    }

    private JasyptDecrypter decrypterWithFilePath(String filePath, String... extraOptionPairs) {
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, filePath);
        for (int i = 0; i + 1 < extraOptionPairs.length; i += 2) {
            props.setProperty(extraOptionPairs[i], extraOptionPairs[i + 1]);
        }
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        return instance;
    }

    /** Calls init_decrypter() and fails the test if any exception is thrown. */
    private void assertInitDecrypterSucceeds(JasyptDecrypter instance) {
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    /**
     * Creates a JasyptDecrypter with the given jasypt password and algorithm already initialized via
     * do_init_decrypter(). Pass null for either argument to omit that property. An optional
     * ivGenerator class name can be supplied as the third argument.
     */
    private JasyptDecrypter preparedDecrypter(String password, String algorithm) {
        return preparedDecrypter(password, algorithm, null);
    }

    private JasyptDecrypter preparedDecrypter(String password, String algorithm, String ivGenerator) {
        Properties jasyptProps = new Properties();
        if (password != null) jasyptProps.setProperty(JASYPT_PASSWORD, password);
        if (algorithm != null) jasyptProps.setProperty(JASYPT_ALGORITHM, algorithm);
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.jasyptProperties = jasyptProps;
        if (ivGenerator != null) {
            Properties props = new Properties();
            props.setProperty(Options.JASYPT_IV_GENERATOR, ivGenerator);
            instance.properties = props;
        }
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }
        return instance;
    }

    @Test
    void testInitDecrypterPropertiesAreBlank() {
        JasyptDecrypter instance = decrypterWithFilePath(" ");
        assertInitDecrypterSucceeds(instance);
        assertEquals(UNENCRYPTED_PASSWORD, instance.jasyptProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitDecrypterWithPassord() {
        JasyptDecrypter instance = decrypterWithFile();
        assertInitDecrypterSucceeds(instance);
        assertEquals(UNENCRYPTED_PASSWORD, instance.jasyptProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitWithCustomEncrypter() {
        JasyptDecrypter instance = decrypterWithFile(
            Options.JASYPT_STRING_ENCRYPTER, "org.jasypt.encryption.pbe.PooledPBEStringEncryptor");
        assertInitDecrypterSucceeds(instance);
        assertEquals(UNENCRYPTED_PASSWORD, instance.jasyptProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitWithCustomInvalidEncrypter() {
        JasyptDecrypter instance = decrypterWithFile(
            Options.JASYPT_STRING_ENCRYPTER, "org.jasypt.encryption.pbe.PooledPBEStringEncryptorrs");
        assertThrows(Exception.class, instance::init_decrypter);
    }

    @Test
    void testInitWithIvGenerator() {
        JasyptDecrypter instance = decrypterWithFile(
            Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGenerator");
        assertInitDecrypterSucceeds(instance);
        assertEquals(UNENCRYPTED_PASSWORD, instance.jasyptProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitWithInvalidIvGenerator() {
        JasyptDecrypter instance = decrypterWithFile(
            Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGeneratorrs");
        assertThrows(Exception.class, instance::init_decrypter);
    }

    @Test
    void testInitWithStringIvGeneratorWithtCharset() {
        JasyptDecrypter instance = decrypterWithFile(
            Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.StringFixedIvGenerator,charset");
        assertInitDecrypterSucceeds(instance);
    }

    @Test
    void testInitWithStringIvGeneratorWithoutCharset() {
        JasyptDecrypter instance = decrypterWithFile(
            Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.StringFixedIvGenerator");
        assertThrows(Exception.class, instance::init_decrypter);
    }

    @Test
    void testDoDecrypt() {
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = new TestDecrypt();
        instance.decrypterCls = TestDecrypt.class;
        assertEquals("value", instance.doDecrypt("prop1", "value"));
    }

    @Test
    void testDoDecryptWithBlankValue() {
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = "";
        instance.doDecrypt("", "");
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertTrue(records.get(0).getMessage().startsWith("Cannot decrypt . Ignore if clear text."));
    }

    @Test
    void testDoDecryptDecryptorIsNull() {
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = null;
        assertEquals("bar", instance.doDecrypt("foo", "bar"));
    }

    @Test
    void testDoDecryptJasyptDefaultDecrypter() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, null);
        assertEquals("value", instance.doDecrypt("property", "RBSskx1057hdi1qWe0ugXg=="));
    }

    @Test
    void testDoDecryptJasyptNoPassword() {
        JasyptDecrypter instance = preparedDecrypter(null, null);
        String value = "RBSskx1057hdi1qWe0ugXg==";
        assertEquals(value, instance.doDecrypt("property", value),
            "Should get original value if unable to initialize decrypter");
    }

    @Test
    void testDoDecryptJasyptWithProperties() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM);
        assertEquals("value", instance.doDecrypt("property", "RBSskx1057hdi1qWe0ugXg=="));
    }

    @Test
    void testDoDecryptInvalidValueWithProperties() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM);
        String value = "RBSskx1057hdi1qWe0ugXg==xx";
        assertEquals(value, instance.doDecrypt("property", value)); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptInvalidAlgorithmWithProperties() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM + "1");
        String value = "RBSskx1057hdi1qWe0ugXg==";
        assertEquals(value, instance.doDecrypt("property", value)); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptInvalidPassphraseWithProperties() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD + "1", ALGORITHM);
        String value = "RBSskx1057hdi1qWe0ugXg==";
        assertEquals(value, instance.doDecrypt("property", value)); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptWithIVGenerator() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM, "org.jasypt.iv.RandomIvGenerator");
        assertEquals("value", instance.doDecrypt("property", "OvlChyf73qNC7DB9tSPDWqDQTnGR3+oo"));
    }

    @Test
    void testDoDecryptWithMissingIVGenerator() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM);
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3+oo";
        assertEquals(value, instance.doDecrypt("property", value)); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptInvalidValueWithIVGenerator() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM, "org.jasypt.iv.RandomIvGenerator");
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3";
        assertEquals(value, instance.doDecrypt("property", value)); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptWithWrongIVGenerator() {
        JasyptDecrypter instance = preparedDecrypter(PASSWORD, ALGORITHM, "org.jasypt.iv.StringFixedIvGenerator,charset");
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3+oo";
        assertEquals(value, instance.doDecrypt("property", value)); //should get original value if unable to decrypt
    }

    private static class TestDecrypt {

        public String decrypt(String value) {
            return value;
        }
    }
}
