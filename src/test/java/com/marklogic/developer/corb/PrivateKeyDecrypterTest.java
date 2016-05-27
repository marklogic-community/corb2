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
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.io.File;
import java.io.IOException;
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
    private static final String privateKeyPath = "src/test/resources/privateKey.pem";
    private static final String publicKeyPath = "src/test/resources/publicKey.pem";
    private static final String encryptedValue ="AsBDHqubo00eHVFPkWjV4AmOb8U4wbID6OXXO671cGXntKu4XmicvR0ax8OZgU3QzJDaYIeFzmToOJ3IQ5PzsIs8e0XREKVkOy+wz5RPYg7wBab+y7pmUrXJEPitJoi/jGn6ZwsU6AnImXckqd3NHUazbp7LF8tyC5GqsGL0nYY=";
    private static final String LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY = "PRIVATE-KEY-FILE property must be defined";

    @Before
    public void setUp() {
        clearSystemProperties();
        Logger logger = Logger.getLogger(PrivateKeyDecrypter.class.getName());
        logger.addHandler(testLogger);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    private void setSystemProperties() {
        System.setProperty("PRIVATE-KEY-FILE", privateKeyPath);
    }

    /**
     * Test of init_decrypter method, of class PrivateKeyDecrypter.
     */
    @Test
    public void testInit_decrypter_initNotInvoked() throws Exception {
        System.out.println("init_decrypter");
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init_decrypter();
    }

    @Test
    public void testInit_decrypter_missingPrivateKeyFile() throws Exception {
        System.out.println("init_decrypter");
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
        System.out.println("init_decrypter");
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-ALGORITHM", "RSA");
        //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
        testLogger.clear();

        instance.init_decrypter();
        
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY, records.get(0).getMessage());
    }
    
      @Test
    public void testInit_decrypter_withEmptyPrivateKeyPath() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-FILE", "");
        //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
        testLogger.clear();

        instance.init_decrypter();
        
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(LOG_MSG_INVALID_PRIVATE_KEY_FILE_PROPERTY, records.get(0).getMessage());
    }
    
    @Test
    public void testInit_decrypter_withDirectoryAsPrivateKeyPath() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-FILE", privateKeyPath.replace("privateKey.pem", ""));
        //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
        testLogger.clear();

        instance.init_decrypter();
        
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("Problem initializing PrivateKeyDecrypter", records.get(0).getMessage());
    }
    
    @Test
    public void testInit_decrypter_withInvalidPrivateKeyPath() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-FILE", privateKeyPath + "/invalid");
        //init_decrypter() is invoked as part of init(), reset logs and measure only for this invocation
        testLogger.clear();

        instance.init_decrypter();
        
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("Problem initializing PrivateKeyDecrypter", records.get(0).getMessage());
    }
    
    @Test
    public void testInit_decrypter_loadPrivateKeyFromClasspath() throws Exception {
        System.out.println("init_decrypter");
        clearSystemProperties();
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-FILE", "privateKey.pem");
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
        System.out.println("doDecrypt");
        String property = "key";
        String value = "value";
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        String result = instance.doDecrypt(property, value);
        assertEquals(value, result);
    }

    @Test
    public void testDoDecrypt_withPrivateKey() throws IOException, ClassNotFoundException {
        System.out.println("doDecrypt");
        String value = "secret";
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-FILE", privateKeyPath);
        instance.init_decrypter();
        String result = instance.doDecrypt("key", encryptedValue);
        assertEquals(value, result);
    }
    
    @Test
    public void testDoDecrypt_unencryptedValue() throws IOException, ClassNotFoundException {
        System.out.println("doDecrypt");
        String value = "secret";
        PrivateKeyDecrypter instance = new PrivateKeyDecrypter();
        instance.init(null);
        instance.properties.setProperty("PRIVATE-KEY-FILE", privateKeyPath);
        instance.init_decrypter();
        String result = instance.doDecrypt("key", value);
        assertEquals(value, result);
    }

    /**
     * Test of main method, of class PrivateKeyDecrypter.
     */
    @Test
    public void testMain_genKeys_noOptions() throws Exception {
        System.out.println("main");
        String[] args = {"gen-keys"};
        PrivateKeyDecrypter.main(args);
    }
    
    @Test
    public void testMain_genKeys() throws Exception {
        System.out.println("main");
        File tempPublic = File.createTempFile("pub", "pem");
        tempPublic.delete();
        tempPublic.deleteOnExit();
        File tempPrivate = File.createTempFile("pub", "pem");
        tempPrivate.delete();
        tempPrivate.deleteOnExit();
        String[] args = {"gen-keys", tempPrivate.toString(), tempPublic.toString(), "RSA", "2048"};
        PrivateKeyDecrypter.main(args);
        assertTrue(tempPublic.exists());
        assertTrue(tempPrivate.exists());
    }
    
    @Test
    public void testMain_genKeys_emptyPrivateKeyPath() throws Exception {
        System.out.println("main");
        File tempPublic = File.createTempFile("pub", "pem");
        tempPublic.delete();
        tempPublic.deleteOnExit();
        File tempPrivate = File.createTempFile("pub", "pem");
        tempPrivate.delete();
        tempPrivate.deleteOnExit();
        String[] args = {"gen-keys", "", tempPublic.toString(), "RSA", "2048"};
        PrivateKeyDecrypter.main(args);
        assertFalse(tempPublic.exists());
        assertFalse(tempPrivate.exists());
    }
    @Test
    public void testMain_genKeys_emptyPublicKeyPath() throws Exception {
        System.out.println("main");
        File tempPublic = File.createTempFile("pub", "pem");
        tempPublic.delete();
        tempPublic.deleteOnExit();
        File tempPrivate = File.createTempFile("pub", "pem");
        tempPrivate.delete();
        tempPrivate.deleteOnExit();
        String[] args = {"gen-keys", tempPrivate.toString(), "", "RSA", "2048"};
        PrivateKeyDecrypter.main(args);
        assertFalse(tempPublic.exists());
        assertFalse(tempPrivate.exists());
    }
    @Test
    public void testMain_genKeys_emptyArgValues() throws Exception {
        System.out.println("main");
        File tempPublic = File.createTempFile("pub", "pem");
        tempPublic.delete();
        tempPublic.deleteOnExit();
        File tempPrivate = File.createTempFile("pub", "pem");
        tempPrivate.delete();
        tempPrivate.deleteOnExit();
        String[] args = {"gen-keys", "", "", "", ""};
        PrivateKeyDecrypter.main(args);
        assertFalse(tempPublic.exists());
        assertFalse(tempPrivate.exists());
    }
    @Test
    public void testMain_encrypt() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt"};
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_encrypt_allParameters() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", publicKeyPath, "secret", "RSA"};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
    }

    //TODO: test with an algorithm other than RSA
    @Test(expected = NoSuchAlgorithmException.class)
    public void testMain_encrypt_invalidAlgorithm() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", publicKeyPath, "secret", "badAlgorithm"};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
    }

    @Test(expected = InvalidKeySpecException.class)
    public void testMain_encrypt_invalidPublicKey() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", privateKeyPath, "secret", "RSA"};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_encrypt_blankValue() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", privateKeyPath, "", "RSA"};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_encrypt_nullKey() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", "", "secret", "RSA"};
        setSystemProperties();
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_invalidFirstArg() throws Exception {
        System.out.println("main");
        String[] args = {"invalidUsage"};
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_nullArgs() throws Exception {
        System.out.println("main");
        String[] args = null;
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_emptyArgsArray() throws Exception {
        System.out.println("main");
        String[] args = {};
        PrivateKeyDecrypter.main(args);
    }

    @Test
    public void testMain_blankArgsArray() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", "", "", ""};
        PrivateKeyDecrypter.main(args);
    }
}
