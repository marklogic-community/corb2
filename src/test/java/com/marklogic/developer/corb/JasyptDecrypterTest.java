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
import java.io.File;
import java.io.FileOutputStream;
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
    private static final String JASYPT_ALGORITHM = "jasypt.algorithm";
    private static final String JASYPT_PASSWORD = "jasypt.password";
    private static final String ERROR_NO_PASSWORD = "Unable to initialize jasypt decrypter. Couldn't find jasypt.password";
    
    @Before
    public void setUp() {
        clearSystemProperties();    
        LOG.addHandler(testLogger);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    /**
     * Test of init_decrypter method, of class JasyptDecrypter.
     */
    //@Test
    public void testInit_decrypter() throws Exception {
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.init_decrypter();
    }

    //@Test
    public void testInit_decrypter_badJasyptPropertiesFilePath() throws Exception {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "does/not/exist");
        props.setProperty(JASYPT_ALGORITHM, "MD5");
        props.setProperty(JASYPT_PASSWORD, "secret");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        assertNull(instance.decrypter);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(ERROR_NO_PASSWORD, records.get(0).getMessage());
    }

    @Test
    public void testInit_decrypter_propertiesAreBlank() throws Exception {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, " ");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        assertEquals("corbencrypt", instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
    }

    //@Test
    public void testInit_decrypter_algorithmIsBlank() throws Exception {
        clearSystemProperties();
        Properties blankProps = new Properties();
        blankProps.setProperty(JASYPT_ALGORITHM, "  ");
        blankProps.setProperty(JASYPT_PASSWORD, "  ");
        File blankPropsFile = File.createTempFile("temp", ".properties");
        blankPropsFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(blankPropsFile);
        blankProps.store(outputStream, "");
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, blankPropsFile.getAbsolutePath());

        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        assertNull(instance.decrypter);
    }

    //@Test
    public void testInit_decrypter_algorithmIsNotBlank() throws Exception {
        clearSystemProperties();
        Properties blankProps = new Properties();
        blankProps.setProperty(JASYPT_ALGORITHM, "PBEWithMD5AndTripleDES");
        blankProps.setProperty(JASYPT_PASSWORD, "password");
        File blankPropsFile = File.createTempFile("temp", ".properties");
        blankPropsFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(blankPropsFile);
        blankProps.store(outputStream, "");
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, blankPropsFile.getAbsolutePath());

        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();

        assertEquals("PBEWithMD5AndTripleDES", instance.jaspytProperties.getProperty(JASYPT_ALGORITHM));
        assertEquals("password", instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
        assertNotNull(instance.decrypter);
    }

    //@Test
    public void testInit_decrypter_noJasyptProperties() throws Exception {
        clearSystemProperties();
        Properties emptyProps = new Properties();
        File emptyFile = File.createTempFile("temp", ".properties");
        emptyFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(emptyFile);
        emptyProps.store(outputStream, "");
        outputStream.close();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, emptyFile.getAbsolutePath());

        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(ERROR_NO_PASSWORD, records.get(0).getMessage());
    }

    @Test
    public void testInit_decrypter_withPassord() throws Exception {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        assertEquals("corbencrypt", instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
    }

    /**
     * Test of doDecrypt method, of class JasyptDecrypter.
     */
    @Test
    public void testDoDecrypt() {
        String property = "foo";
        String value = "bar";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = new TestDecrypt();
        instance.decrypterCls = TestDecrypt.class;
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    public void testDoDecrypt_() {
        String property = "";
        String value = "";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = "";
        instance.doDecrypt(property, value);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("Cannot decrypt {0}. Ignore if clear text.", records.get(0).getMessage());
    }

    @Test
    public void testDoDecrypt_decryptorIsNull() {
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
