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

    @BeforeEach
    void setUp() {
        clearSystemProperties();
        LOG.addHandler(testLogger);
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    @Test
    void testInitDecrypterPropertiesAreBlank() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, " ");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(UNENCRYPTED_PASSWORD, instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitDecrypterWithPassord() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(UNENCRYPTED_PASSWORD, instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitWithCustomEncrypter() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        props.setProperty(Options.JASYPT_STRING_ENCRYPTER, "org.jasypt.encryption.pbe.PooledPBEStringEncryptor");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(UNENCRYPTED_PASSWORD, instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitWithCustomInvalidEncrypter() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        props.setProperty(Options.JASYPT_STRING_ENCRYPTER, "org.jasypt.encryption.pbe.PooledPBEStringEncryptorrs");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
            fail();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.INFO, "Expected with an invalid value test", ex);
        }
    }

    @Test
    void testInitWithIvGenerator() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGenerator");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(UNENCRYPTED_PASSWORD, instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
    }

    @Test
    void testInitWithInvalidIvGenerator() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGeneratorrs");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
            fail();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.INFO, "Expected with an invalid value test", ex);
        }
    }

    @Test
    void testInitWithStringIvGeneratorWithtCharset() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.StringFixedIvGenerator,charset");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.INFO, "", ex);
            fail();
        }
    }

    @Test
    void testInitWithStringIvGeneratorWithoutCharset() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.StringFixedIvGenerator");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
            fail();
        } catch (IOException | ClassNotFoundException | IllegalStateException ex) {
            LOG.log(Level.INFO, "Expected with an invalid value test", ex);
        }
    }

    @Test
    void testDoDecrypt() {
        String property = "prop1";
        String value = "value";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = new TestDecrypt();
        instance.decrypterCls = TestDecrypt.class;
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    void testDoDecryptWithBlankValue() {
        String property = "";
        String value = "";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = "";
        instance.doDecrypt(property, value);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertTrue(records.get(0).getMessage().startsWith("Cannot decrypt . Ignore if clear text."));
    }

    @Test
    void testDoDecryptDecryptorIsNull() {
        String property = "foo";
        String value = "bar";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = null;
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    void testDoDecryptJasyptWithProperties() {
        String property = "property";
        String value = "RBSskx1057hdi1qWe0ugXg==";
        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertEquals("value", result);
    }

    @Test
    void testDoDecryptInvalidValueWithProperties() {
        String property = "property";
        String value = "RBSskx1057hdi1qWe0ugXg==xx";
        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertNotEquals("value", result);
        assertEquals(value, result); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptInvalidAlgorithmWithProperties() {
        String property = "property";
        String value = "RBSskx1057hdi1qWe0ugXg==";
        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES1");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertNotEquals("value", result);
        assertEquals(value, result); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptInvalidPassphraseWithProperties() {
        String property = "property";
        String value = "RBSskx1057hdi1qWe0ugXg==";
        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password1");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertNotEquals("value", result);
        assertEquals(value, result); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptWithIVGenerator() {
        String property = "property";
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3+oo";

        Properties props = new Properties();
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGenerator");

        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();

        instance.properties = props;
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertEquals("value", result);
    }

    @Test
    void testDoDecryptWithMissingIVGenerator() {
        String property = "property";
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3+oo";

        Properties props = new Properties();
        //props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGenerator");

        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();

        instance.properties = props;
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertNotEquals("value", result);
        assertEquals(value, result); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptInvalidValueWithIVGenerator() {
        String property = "property";
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3";

        Properties props = new Properties();
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.RandomIvGenerator");

        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();

        instance.properties = props;
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertNotEquals("value", result);
        assertEquals(value, result); //should get original value if unable to decrypt
    }

    @Test
    void testDoDecryptWithWrongIVGenerator() {
        String property = "property";
        String value = "OvlChyf73qNC7DB9tSPDWqDQTnGR3+oo";

        Properties props = new Properties();
        props.setProperty(Options.JASYPT_IV_GENERATOR, "org.jasypt.iv.StringFixedIvGenerator,charset");

        Properties jasyptProps = new Properties();
        jasyptProps.setProperty("jasypt.password", "password");
        jasyptProps.setProperty("jasypt.algorithm", "PBEWithMD5AndTripleDES");
        JasyptDecrypter instance = new JasyptDecrypter();

        instance.properties = props;
        instance.jaspytProperties = jasyptProps;
        try {
            instance.do_init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, "", ex);
            fail();
        }

        String result = instance.doDecrypt(property, value);
        assertNotEquals("value", result);
        assertEquals(value, result); //should get original value if unable to decrypt
    }

    private static class TestDecrypt {

        public String decrypt(String value) {
            return value;
        }
    }
}
