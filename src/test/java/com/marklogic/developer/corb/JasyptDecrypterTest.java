/*
 * * Copyright (c) 2004-2016 MarkLogic Corporation
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
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class JasyptDecrypterTest {

    private static final Logger LOG = Logger.getLogger(JasyptDecrypter.class.getName());
    private final TestHandler testLogger = new TestHandler();
    public static final String JASYPT_ALGORITHM = "jasypt.algorithm";
    public static final String JASYPT_PASSWORD = "jasypt.password";
    public static final String ERROR_NO_PASSWORD = "Unable to initialize jasypt decrypter. Couldn't find jasypt.password";
    private static final String UNENCRYPTED_PASSWORD = "corbencrypt";
    
    @Before
    public void setUp() {
        clearSystemProperties();    
        LOG.addHandler(testLogger);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    @Test
    public void testInitDecrypterPropertiesAreBlank() {
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
    public void testInitDecrypterWithPassord() {
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
    public void testDoDecrypt() {
        String property = "prop1";
        String value = "value";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = new TestDecrypt();
        instance.decrypterCls = TestDecrypt.class;
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    public void testDoDecryptWithBlankValue() {
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
    public void testDoDecryptDecryptorIsNull() {
        String property = "foo";
        String value = "bar";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = null;
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    private class TestDecrypt {

        public String decrypt(String value) {
            return value;
        }
    }
}
