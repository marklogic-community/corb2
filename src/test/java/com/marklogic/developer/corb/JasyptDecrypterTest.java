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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class JasyptDecrypterTest {

    private final TestHandler testLogger = new TestHandler();

    public JasyptDecrypterTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        clearSystemProperties();
        Logger logger = Logger.getLogger(JasyptDecrypter.class.getName());
        logger.addHandler(testLogger);
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of init_decrypter method, of class JasyptDecrypter.
     */
    @Test
    public void testInit_decrypter() throws Exception {
        System.out.println("init_decrypter");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.init_decrypter();
    }

    @Test
    public void testInit_decrypter_badJasyptPropertiesFilePath() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty("JASYPT-PROPERTIES-FILE", "does/not/exist");
        props.setProperty("jasypt.algorithm", "MD5");
        props.setProperty("jasypt.password", "secret");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        List<LogRecord> records = testLogger.getLogRecords();
        //TODO figure out why executing this method (or all tests for this class one time) succeeds, but when suite is executing and runs multiple times, gets indexOutOfBounds for Records
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("Unable to initialize jasypt decrypter. Couldn't find jasypt.password", records.get(0).getMessage());
    }

    @Test
    public void testInit_decrypter_propertiesAreBlank() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty("JASYPT-PROPERTIES-FILE", " ");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        assertEquals("corbencrypt", instance.jaspytProperties.getProperty("jasypt.password"));
    }

    @Test
    public void testInit_decrypter_algorithmIsBlank() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        Properties blankProps = new Properties();
        blankProps.setProperty("jasypt.algorithm", "  ");
        blankProps.setProperty("jasypt.passphrase", "  ");
        File blankPropsFile = File.createTempFile("temp", ".properties");
        blankPropsFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(blankPropsFile);
        blankProps.store(outputStream, "");
        Properties props = new Properties();
        props.setProperty("JASYPT-PROPERTIES-FILE", blankPropsFile.getAbsolutePath());

        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
    }

    @Test
    public void testInit_decrypter_noJasyptProperties() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        Properties emptyProps = new Properties();
        File emptyFile = File.createTempFile("temp", ".properties");
        emptyFile.deleteOnExit();
        FileOutputStream outputStream = new FileOutputStream(emptyFile);
        emptyProps.store(outputStream, "");
        outputStream.close();
        Properties props = new Properties();
        props.setProperty("JASYPT-PROPERTIES-FILE", emptyFile.getAbsolutePath());

        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("Unable to initialize jasypt decrypter. Couldn't find jasypt.password", records.get(0).getMessage());
    }

    @Test
    public void testInit_decrypter_withPassord() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty("JASYPT-PROPERTIES-FILE", "src/test/resources/jasypt.properties");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        instance.init_decrypter();
        assertEquals("corbencrypt", instance.jaspytProperties.getProperty("jasypt.password"));
    }

    /**
     * Test of doDecrypt method, of class JasyptDecrypter.
     */
    @Test
    public void testDoDecrypt() {
        System.out.println("doDecrypt");
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
        System.out.println("doDecrypt");
        String property = "";
        String value = "";
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.decrypter = new String();
        instance.doDecrypt(property, value);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("Cannot decrypt {0}. Ignore if clear text.", records.get(0).getMessage());
    }

    @Test
    public void testDoDecrypt_decryptorIsNull() {
        System.out.println("doDecrypt");
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
