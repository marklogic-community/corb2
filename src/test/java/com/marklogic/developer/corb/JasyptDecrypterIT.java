/*
  * * Copyright (c) 2004-2020 MarkLogic Corporation
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
import static com.marklogic.developer.corb.JasyptDecrypterTest.ERROR_NO_PASSWORD;
import static com.marklogic.developer.corb.JasyptDecrypterTest.JASYPT_ALGORITHM;
import static com.marklogic.developer.corb.JasyptDecrypterTest.JASYPT_PASSWORD;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class JasyptDecrypterIT {

    private static final Logger LOG = Logger.getLogger(JasyptDecrypterIT.class.getName());
    private static final Logger JASYPT_DECRYPTER_LOG = Logger.getLogger(JasyptDecrypter.class.getName());
    private final TestHandler testLogger = new TestHandler();
    private static final String TEMP_PREFIX = "temp";
    private static final String PROPERTIES_SUFFIX = ".properties";
    private static final String TWO_SPACES = " ";

    @Before
    public void setUp() {
        clearSystemProperties();
        JASYPT_DECRYPTER_LOG.addHandler(testLogger);
    }

    @Test
    public void testInitDecrypter() {
        JasyptDecrypter instance = new JasyptDecrypter();
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitDecrypterBadJasyptPropertiesFilePath() {
        clearSystemProperties();
        Properties props = new Properties();
        props.setProperty(Options.JASYPT_PROPERTIES_FILE, "does/not/exist");
        props.setProperty(JASYPT_ALGORITHM, "MD5");
        props.setProperty(JASYPT_PASSWORD, "secret");
        JasyptDecrypter instance = new JasyptDecrypter();
        instance.properties = props;
        try {
            instance.init_decrypter();
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNull(instance.decrypter);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(ERROR_NO_PASSWORD, records.get(0).getMessage());
    }

    @Test
    public void testInitDecrypterAlgorithmIsBlank() {
        clearSystemProperties();
        Properties blankProps = new Properties();
        blankProps.setProperty(JASYPT_ALGORITHM, TWO_SPACES);
        blankProps.setProperty(JASYPT_PASSWORD, TWO_SPACES);
        try {
            File blankPropsFile = File.createTempFile(TEMP_PREFIX, PROPERTIES_SUFFIX);
            blankPropsFile.deleteOnExit();
            try (FileOutputStream outputStream = new FileOutputStream(blankPropsFile)) {
                blankProps.store(outputStream, "");
            }
            Properties props = new Properties();
            props.setProperty(Options.JASYPT_PROPERTIES_FILE, blankPropsFile.getAbsolutePath());

            JasyptDecrypter instance = new JasyptDecrypter();
            instance.properties = props;
            instance.init_decrypter();
            assertNull(instance.decrypter);
        } catch (ClassNotFoundException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitDecrypterAlgorithmIsNotBlank() {
        clearSystemProperties();
        String password = "password";
        String alg = "PBEWithMD5AndTripleDES";
        Properties blankProps = new Properties();
        blankProps.setProperty(JASYPT_ALGORITHM, alg);
        blankProps.setProperty(JASYPT_PASSWORD, password);
        try {
            File blankPropsFile = File.createTempFile(TEMP_PREFIX, PROPERTIES_SUFFIX);
            blankPropsFile.deleteOnExit();
            try (FileOutputStream outputStream = new FileOutputStream(blankPropsFile)) {
                blankProps.store(outputStream, "");
            }
            Properties props = new Properties();
            props.setProperty(Options.JASYPT_PROPERTIES_FILE, blankPropsFile.getAbsolutePath());

            JasyptDecrypter instance = new JasyptDecrypter();
            instance.properties = props;
            instance.init_decrypter();

            assertEquals(alg, instance.jaspytProperties.getProperty(JASYPT_ALGORITHM));
            assertEquals(password, instance.jaspytProperties.getProperty(JASYPT_PASSWORD));
            assertNotNull(instance.decrypter);
        } catch (ClassNotFoundException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitDecrypterNoJasyptProperties() {
        clearSystemProperties();
        Properties emptyProps = new Properties();
        try {
            File emptyFile = File.createTempFile(TEMP_PREFIX, PROPERTIES_SUFFIX);
            emptyFile.deleteOnExit();
            try (FileOutputStream outputStream = new FileOutputStream(emptyFile)) {
                emptyProps.store(outputStream, "");
            }
            Properties props = new Properties();
            props.setProperty(Options.JASYPT_PROPERTIES_FILE, emptyFile.getAbsolutePath());

            JasyptDecrypter instance = new JasyptDecrypter();
            instance.properties = props;
            instance.init_decrypter();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals(ERROR_NO_PASSWORD, records.get(0).getMessage());
        } catch (ClassNotFoundException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
}
