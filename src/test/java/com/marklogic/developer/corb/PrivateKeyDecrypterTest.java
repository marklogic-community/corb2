/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;
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
public class PrivateKeyDecrypterTest {

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
    private PrintStream systemOut = System.out;
    private PrintStream systemErr = System.err;
    
    @Before
    public void setUp() {
        clearSystemProperties();
        LOG.addHandler(testLogger);
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void tearDown() {
        clearSystemProperties();
        System.setOut(systemOut);
        System.setErr(systemErr);
    }

    private void setSystemProperties() {
        System.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
    }

    /**
     * Test of init_decrypter method, of class PrivateKeyDecrypter.
     */
    @Test
    public void testInit_decrypter_initNotInvoked() throws Exception {
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init_decrypter();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(PRIVATE_KEY_FILE + " property must be defined", records.get(0).getMessage());
    }

    @Test
    public void testInit_decrypter_missingPrivateKeyFile() throws Exception {
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
        testLogger.clear();

        instance.init_decrypter();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY, records.get(0).getMessage());
    }

    @Test
    public void testInit_decrypter_withAlgorithm() throws Exception {
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
    }

    @Test
    public void testInit_decrypter_withEmptyPrivateKeyPath() throws Exception {
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
    }

    @Test
    public void testInit_decrypter_withDirectoryAsPrivateKeyPath() throws Exception {
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
    }

    @Test
    public void testInit_decrypter_withInvalidPrivateKeyPath() throws Exception {
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
    }

    @Test
    public void testInit_decrypter_loadPrivateKeyFromClasspath() throws Exception {
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_NAME);
        //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
        testLogger.clear();

        instance.init_decrypter();

        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("Loading private key file {0} from classpath", records.get(0).getMessage());
    }

    /**
     * Test of doDecrypt method, of class PrivateKeyDecrypter.
     */
    @Test
    public void testDoDecrypt_withoutPrivateKey() {
        String property = "key";
        String value = "value";
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    public void testDoDecrypt_withPrivateKey() throws IOException, ClassNotFoundException {
        String value = SECRET;
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
        instance.init_decrypter();
        String result = instance.doDecrypt("key", ENCRYPTED_VALUE);
        assertEquals(value, result);
    }

    @Test
    public void testDoDecrypt_unencryptedValue() throws IOException, ClassNotFoundException {
        String value = SECRET;
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty(PRIVATE_KEY_FILE, PRIVATE_KEY_PATH);
        instance.init_decrypter();
        String result = instance.doDecrypt("key", value);
        assertEquals(value, result);
    }

    /**
     * Test of main method, of class PrivateKeyDecrypter.
     */
    @Test
    public void testMain_genKeys_noOptions() throws Exception {
        String[] args = {ACTION_GEN_KEYS};
        PrivateKeyDecrypter.main(args);
        assertEquals(GEN_KEYS_USAGE + NEWLINE, errContent.toString());
    }
    
    private File createTempFile() throws IOException {
        File temp = File.createTempFile("pub", "pem");
        temp.deleteOnExit();
        return temp;
    }
    
    @Test
    public void testMain_genKeys() throws Exception {
        File tempPublic = createTempFile();
        File tempPrivate = createTempFile();
        String[] args = {ACTION_GEN_KEYS, tempPrivate.toString(), tempPublic.toString(), ALGORITHM, STRENGTH};
        PrivateKeyDecrypter.main(args);
        assertTrue(tempPublic.exists());
        assertTrue(tempPrivate.exists());
    }

    @Test
    public void testMain_genKeys_emptyPrivateKeyPath() throws Exception {
        File tempPublic = createTempFile();
        tempPublic.delete();
        File tempPrivate = createTempFile();
        tempPrivate.delete();
        String[] args = {ACTION_GEN_KEYS, "", tempPublic.toString(), ALGORITHM, STRENGTH};
        PrivateKeyDecrypter.main(args);
        assertFalse(tempPublic.exists());
        assertFalse(tempPrivate.exists());
    }

    @Test
    public void testMain_genKeys_emptyPublicKeyPath() throws Exception {
        File tempPublic = createTempFile();
        tempPublic.delete();
        File tempPrivate = createTempFile();
        tempPrivate.delete();
        String[] args = {ACTION_GEN_KEYS, tempPrivate.toString(), "", ALGORITHM, STRENGTH};
        PrivateKeyDecrypter.main(args);
        assertFalse(tempPublic.exists());
        assertFalse(tempPrivate.exists());
    }

    @Test
    public void testMain_genKeys_emptyArgValues() throws Exception {
        File tempPublic = createTempFile();
        tempPublic.delete();
        File tempPrivate = createTempFile();
        tempPrivate.delete();
        String[] args = {ACTION_GEN_KEYS, "", "", "", ""};
        PrivateKeyDecrypter.main(args);
        assertFalse(tempPublic.exists());
        assertFalse(tempPrivate.exists());
    }

    @Test
    public void testMain_encrypt() throws Exception {
        String[] args = {ACTION_ENCRYPT};
        PrivateKeyDecrypter.main(args);
        assertEquals(ENCRYPT_USAGE + NEWLINE, errContent.toString());
    }

    @Test
    public void testMain_encrypt_allParameters() throws Exception {
        String[] args = {ACTION_ENCRYPT, PUBLIC_KEY_PATH, SECRET, ALGORITHM};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
        assertTrue(outContent.toString().startsWith("Input: " + SECRET + "\nOutput: "));
    }

    //TODO: test with an algorithm other than RSA
    @Test(expected = NoSuchAlgorithmException.class)
    public void testMain_encrypt_invalidAlgorithm() throws Exception {
        String[] args = {ACTION_ENCRYPT, PUBLIC_KEY_PATH, SECRET, "badAlgorithm"};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
        fail();
    }

    @Test(expected = InvalidKeySpecException.class)
    public void testMain_encrypt_invalidPublicKey() throws Exception {
        String[] args = {ACTION_ENCRYPT, PRIVATE_KEY_PATH, SECRET, ALGORITHM};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
        fail();
    }

    @Test
    public void testMain_encrypt_blankValue() throws Exception {
        String[] args = {ACTION_ENCRYPT, PRIVATE_KEY_PATH, "", ALGORITHM};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
        assertEquals(ENCRYPT_USAGE + NEWLINE, errContent.toString());
    }

    @Test
    public void testMain_encrypt_nullKey() throws Exception {
        String[] args = {ACTION_ENCRYPT, "", SECRET, ALGORITHM};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
        assertEquals(ENCRYPT_USAGE + NEWLINE, errContent.toString());
    }

    @Test
    public void testMain_invalidFirstArg() throws Exception {
        String[] args = {"invalidUsage"};
        PrivateKeyDecrypter.main(args);
        assertEquals(USAGE, outContent.toString());
    }

    @Test
    public void testMain_nullArgs() throws Exception {
        String[] args = null;
        PrivateKeyDecrypter.main(args);
        assertEquals(USAGE, outContent.toString());
    }

    @Test
    public void testMain_emptyArgsArray() throws Exception {
        String[] args = {};
        PrivateKeyDecrypter.main(args);
        assertEquals(USAGE, outContent.toString());
    }

    @Test
    public void testMain_blankArgsArray() throws Exception {
        String[] args = {ACTION_ENCRYPT, "", "", ""};
        PrivateKeyDecrypter.main(args);
        assertEquals(ENCRYPT_USAGE + NEWLINE, errContent.toString());
    }
}
