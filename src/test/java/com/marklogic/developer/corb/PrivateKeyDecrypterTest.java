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

import com.marklogic.developer.TestHandler;
import static com.marklogic.developer.corb.Options.PRIVATE_KEY_FILE;
import static com.marklogic.developer.corb.PrivateKeyDecrypter.ENCRYPT_USAGE;
import static com.marklogic.developer.corb.PrivateKeyDecrypter.GEN_KEYS_USAGE;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static com.marklogic.developer.corb.TestUtils.assertEqualsNormalizeNewline;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class PrivateKeyDecrypterTest {

    private final TestHandler testLogger = new TestHandler();
    private static final Logger LOG = Logger.getLogger(PrivateKeyDecrypter.class.getName());
    private static final String PRIVATE_KEY_NAME = "privateKey.pem";
    private static final String PRIVATE_KEY_PATH = "src/test/resources/" + PRIVATE_KEY_NAME;
    private static final String PUBLIC_KEY_PATH = "src/test/resources/publicKey.pem";
    private static final String ENCRYPTED_VALUE = "AsBDHqubo00eHVFPkWjV4AmOb8U4wbID6OXXO671cGXntKu4XmicvR0ax8OZgU3QzJDaYIeFzmToOJ3IQ5PzsIs8e0XREKVkOy+wz5RPYg7wBab+y7pmUrXJEPitJoi/jGn6ZwsU6AnImXckqd3NHUazbp7LF8tyC5GqsGL0nYY=";
    private static final String LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY = "PRIVATE-KEY-FILE property must be defined";
    private static final String ACTION_GEN_KEYS = "gen-keys";
    private static final String ACTION_ENCRYPT = "encrypt";
    private static final String ALGORITHM = "RSA";
    private static final String STRENGTH = "2048";
    private static final String SECRET = "secret";
    private static final String NEWLINE = "\n";
    private static final String USAGE = GEN_KEYS_USAGE + NEWLINE + ENCRYPT_USAGE + NEWLINE;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private static final PrintStream systemOut = System.out;
    private static final PrintStream systemErr = System.err;

    @BeforeEach
    void setUp() throws UnsupportedEncodingException {
        clearSystemProperties();
        LOG.addHandler(testLogger);
        System.setOut(new PrintStream(outContent, true, StandardCharsets.UTF_8.name()));
        System.setErr(new PrintStream(errContent, true, StandardCharsets.UTF_8.name()));
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
        System.setOut(systemOut);
        System.setErr(systemErr);
    }

    private static void setSystemProperties() {
        System.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
    }

    @Test
    void testInitDecrypterInitNotInvoked() {
        try {
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init_decrypter();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals(PRIVATE_KEY_FILE + " property must be defined", records.get(0).getMessage());
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitDecrypterMissingPrivateKeyFile() {
        try {
            clearSystemProperties();
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
            testLogger.clear();

            instance.init_decrypter();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals(LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY, records.get(0).getMessage());
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitDecrypterWithAlgorithm() {
        try {
            clearSystemProperties();
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(Options.PRIVATE_KEY_ALGORITHM, ALGORITHM);
            //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
            testLogger.clear();

            instance.init_decrypter();

            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals(LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY, records.get(0).getMessage());
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitDecrypterWithEmptyPrivateKeyPath() {
        try {
            clearSystemProperties();
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, "");
            //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
            testLogger.clear();

            instance.init_decrypter();

            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals(LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY, records.get(0).getMessage());
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitDecrypterWithDirectoryAsPrivateKeyPath() {
        try {
            clearSystemProperties();
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH.replace(PRIVATE_KEY_NAME, ""));
            //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
            testLogger.clear();

            instance.init_decrypter();

            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals("Problem initializing PrivateKeyDecrypter", records.get(0).getMessage());
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitDecrypterWithInvalidPrivateKeyPath() {
        try {
            clearSystemProperties();
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH + "/invalid");
            //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
            testLogger.clear();

            instance.init_decrypter();

            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals("Problem initializing PrivateKeyDecrypter", records.get(0).getMessage());
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitDecrypterLoadPrivateKeyFromClasspath() {
        try {
            clearSystemProperties();
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_NAME);
            //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
            testLogger.clear();

            instance.init_decrypter();

            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.INFO, records.get(0).getLevel());
            String message = records.get(0).getMessage();
            assertTrue(message.startsWith("Loading private key file ") && message.contains("from classpath"));
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    /**
     * Test of doDecrypt method, of class PrivateKeyDecrypter.
     */
    @Test
    void testDoDecryptWithoutPrivateKey() {
        String property = "key";
        String value = "value";
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    void testDoDecryptWithPrivateKey() {
        try {
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
            instance.init_decrypter();
            String result = instance.doDecrypt("key", ENCRYPTED_VALUE);
            assertEquals(SECRET, result);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testDoDecryptValueWithDashes() {
        try {
            String value = "unencrypted-value-with-dashes";
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
            instance.init_decrypter();
            String result = instance.doDecrypt("key", value);
            assertEquals(value, result);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testDoDecryptValueForLocalhost() {
        try {
            String value = "localhost";
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
            instance.init_decrypter();
            String result = instance.doDecrypt("key", value);
            assertEquals(value, result);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    void testDoDecryptUnencryptedValue() {
        try {
            String value = SECRET;
            PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
            instance.init(null);
            instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
            instance.init_decrypter();
            String result = instance.doDecrypt("key", value);
            assertEquals(value, result);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    /**
     * Test of main method, of class PrivateKeyDecrypter.
     */
    @Test
    void testMainGenKeysNoOptions() {
        try {
            String[] args = {ACTION_GEN_KEYS};
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(GEN_KEYS_USAGE + NEWLINE, errContent.toString(StandardCharsets.UTF_8.toString()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private static File createTempFile() throws IOException {
        File temp = File.createTempFile("pub", "pem");
        temp.deleteOnExit();
        return temp;
    }

    @Test
    void testMainGenKeys() {
        try {
            File tempPublic = createTempFile();
            File tempPrivate = createTempFile();
            String[] args = {ACTION_GEN_KEYS, tempPrivate.toString(), tempPublic.toString(), ALGORITHM, STRENGTH};
            PrivateKeyDecrypter.main(args);
            assertTrue(tempPublic.exists());
            assertTrue(tempPrivate.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainGenKeysEmptyPrivateKeyPath() {
        try {
            File tempPublic = createTempFile();
            File tempPrivate = createTempFile();
            if (tempPublic.delete() && tempPrivate.delete()) {
                String[] args = {ACTION_GEN_KEYS, "", tempPublic.toString(), ALGORITHM, STRENGTH};
                PrivateKeyDecrypter.main(args);
            }
            assertFalse(tempPublic.exists());
            assertFalse(tempPrivate.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainGenKeysEmptyPublicKeyPath() {
        try {
            File tempPublic = createTempFile();
            File tempPrivate = createTempFile();
            if (tempPublic.delete() && tempPrivate.delete()) {
                String[] args = {ACTION_GEN_KEYS, tempPrivate.toString(), "", ALGORITHM, STRENGTH};
                PrivateKeyDecrypter.main(args);
            }
            assertFalse(tempPublic.exists());
            assertFalse(tempPrivate.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainGenKeysEmptyArgValues() {
        try {
            File tempPublic = createTempFile();
            File tempPrivate = createTempFile();
            if (tempPublic.delete() && tempPrivate.delete()) {
                String[] args = {ACTION_GEN_KEYS, "", "", "", ""};
                PrivateKeyDecrypter.main(args);
            }
            assertFalse(tempPublic.exists());
            assertFalse(tempPrivate.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainEncrypt() {
        try {
            String[] args = {ACTION_ENCRYPT};
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(ENCRYPT_USAGE + NEWLINE, errContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainEncryptAllParameters() {
        try {
            String[] args = {ACTION_ENCRYPT, PUBLIC_KEY_PATH, SECRET, ALGORITHM};
            setSystemProperties();
            PrivateKeyDecrypter.main(args);
            assertTrue(outContent.toString(StandardCharsets.UTF_8.name()).startsWith("Input: " + SECRET + "\nOutput: "));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainEncryptInvalidAlgorithm() {
        String[] args = {ACTION_ENCRYPT, PUBLIC_KEY_PATH, SECRET, "badAlgorithm"};
        setSystemProperties();
        assertThrows(NoSuchAlgorithmException.class, () -> PrivateKeyDecrypter.main(args));
    }

    @Test
    void testMainEncryptInvalidPublicKey() {
        String[] args = {ACTION_ENCRYPT, PRIVATE_KEY_PATH, SECRET, ALGORITHM};
        setSystemProperties();
        assertThrows(InvalidKeySpecException.class, () -> PrivateKeyDecrypter.main(args));
    }

    @Test
    void testMainEncryptBlankValue() {
        try {
            String[] args = {ACTION_ENCRYPT, PRIVATE_KEY_PATH, "", ALGORITHM};
            setSystemProperties();
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(ENCRYPT_USAGE + NEWLINE, errContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainEncryptNullKey() {
        try {
            String[] args = {ACTION_ENCRYPT, "", SECRET, ALGORITHM};
            setSystemProperties();
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(ENCRYPT_USAGE + NEWLINE, errContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainInvalidFirstArg() {
        try {
            String[] args = {"invalidUsage"};
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(USAGE, outContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainWithNullArgs() {
        try {
            String[] args = null;
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(USAGE, outContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainEmptyArgsArray() {
        try {
            String[] args = {};
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(USAGE, outContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMainBlankArgsArray() {
        try {
            String[] args = {ACTION_ENCRYPT, "", "", ""};
            PrivateKeyDecrypter.main(args);
            assertEqualsNormalizeNewline(ENCRYPT_USAGE + NEWLINE, errContent.toString(StandardCharsets.UTF_8.name()));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
}
