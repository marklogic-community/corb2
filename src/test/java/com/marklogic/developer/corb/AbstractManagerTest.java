/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
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
import static com.marklogic.developer.corb.ManagerTest.getMockManagerWithEmptyResults;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.exceptions.RequestException;

import static com.marklogic.developer.corb.Options.OPTIONS_FILE_ENCODING;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.marklogic.xcc.impl.Credentials;
import com.marklogic.xcc.spi.ConnectionProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractManagerTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private static final Logger ABSTRACT_MANAGER_LOG = Logger.getLogger(AbstractManager.class.getName());
    private static final Logger LOG = Logger.getLogger(AbstractManagerTest.class.getName());
    private final TestHandler testLogger = new TestHandler();

    private static final String XCC_URI_USER = "foo";
    private static final String XCC_URI_PASS = "bar";
    private static final String XCC_URI_HOST = "localhost";
    private static final String XCC_URI_PORT = "8008";
    private static final String XCC_URI_DB = "baz";
    private static final String XCC_CONNECTION_URI = "xcc://"+XCC_URI_USER+":"+XCC_URI_PASS+"@"+XCC_URI_HOST+":"+XCC_URI_PORT+"/"+XCC_URI_DB;

    private static final String PROPERTY_XCC_HTTPCOMPLIANT = "xcc.httpcompliant";
    private static final String PROPERTIES_FILE_NAME = "helloWorld.properties";
    private static final String PROPERTIES_FILE_DIR = "src/test/resources/";
    private static final String PROPERTIES_FILE_PATH = PROPERTIES_FILE_DIR + '/' + PROPERTIES_FILE_NAME;
    private static final String INVALID_FILE_PATH = "does/not/exist";
    private static final String SELECTOR_FILE_NAME = "selector.xqy";
    private static final String SELECTOR_FILE_PATH = "src/test/resources/" + SELECTOR_FILE_NAME;
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private String selectorAsText;

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String HOST = "localhost";
    private static final String PORT = "80";

    public static final String JASYPT_ALGORITHM = "jasypt.algorithm";
    public static final String JASYPT_PASSWORD = "jasypt.password";

    private static final PrintStream ERROR = System.err;

    private String originalHttpCompliantValue;

    @Before
    public void setUp() throws FileNotFoundException {
        ABSTRACT_MANAGER_LOG.addHandler(testLogger);
        clearSystemProperties();
        String text = TestUtils.readFile(SELECTOR_FILE_PATH);
        selectorAsText = text.trim();
        originalHttpCompliantValue = System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
        System.setErr(ERROR);
        if (originalHttpCompliantValue != null) {
            System.setProperty(PROPERTY_XCC_HTTPCOMPLIANT, originalHttpCompliantValue);
        } else {
            System.clearProperty(PROPERTY_XCC_HTTPCOMPLIANT);
        }
    }

    @Test
    public void testLoadPropertiesFileString() {
        try {
            Properties result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_PATH);
            assertNotNull(result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testLoadPropertiesFileEncoding() {
        Properties properties = new Properties();
        properties.setProperty(OPTIONS_FILE_ENCODING, "ascii");
        try {
            Properties result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_PATH, true, properties);
            assertNotEquals("Verify that UTF8 value gets mangled when read as ascii","コンニチハ", result.getProperty("URIS-MODULE.greeting"));
            properties.setProperty(OPTIONS_FILE_ENCODING, "utf8");
            result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_PATH, true, properties);
            assertEquals("Value is read correctly when read as UTF8 encoded","コンニチハ", result.getProperty("URIS-MODULE.greeting"));
        } catch(IOException ex) {
            fail();
        }
    }

    @Test
    public void testLoadPropertiesFileEncodingSystemProperty() {
        Properties properties = new Properties();
        System.setProperty(OPTIONS_FILE_ENCODING, "ascii");
        try {
            Properties result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_PATH, true, properties);
            assertNotEquals("Verify that UTF8 value gets mangled when read as ascii","コンニチハ", result.getProperty("URIS-MODULE.greeting"));
            System.setProperty(OPTIONS_FILE_ENCODING, "utf8");
            result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_PATH, true, properties);
            assertEquals("Value is read correctly when read as UTF8 encoded","コンニチハ", result.getProperty("URIS-MODULE.greeting"));
        } catch(IOException ex) {
            fail();
        }
        System.clearProperty(OPTIONS_FILE_ENCODING);
    }

    @Test
    public void testLoadPropertiesFileStringBoolean() {
        try {
            Properties result = AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, false);
            assertNotNull(result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFileStringBooleanMissingFileThrowsException() {
        try {
            AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, true);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testLoadPropertiesFile3args() {
        try {
            Properties props = new Properties();
            Properties result = AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, false, props);
            assertEquals(props, result);
            assertTrue(props.isEmpty());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testLoadPropertiesFile3argsLoadFromClasspath() {
        try {
            Properties props = new Properties();
            props.setProperty(KEY, VALUE);
            Properties result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_NAME, false, props);
            assertEquals(props, result);
            assertFalse(props.isEmpty());
            assertTrue(props.size() > 1);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testLoadPropertiesFile3argsExistingPropertiesAndBadPath() {
        try {
            Properties props = new Properties();
            props.setProperty(KEY, VALUE);
            Properties result = AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, false, props);
            assertEquals(props, result);
            assertFalse(props.isEmpty());
            assertTrue(props.size() == 1);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile3argsExistingPropertiesAndBadPathThrows() {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        try {
            AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, true, props);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testLoadPropertiesFile3argsExistingPropertiesAndBlankPath() {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        try {
            Properties result = AbstractManager.loadPropertiesFile("    ", true, props);
            assertEquals(props, result);
            assertFalse(props.isEmpty());
            assertTrue(props.size() == 1);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile3argsForDirectory() {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        try {
            AbstractManager.loadPropertiesFile(PROPERTIES_FILE_DIR, true, props);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testLogProperties() {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        Manager manager;
        try {
            manager = getMockManagerWithEmptyResults();
            manager.properties = props;
            manager.logProperties();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(props.size(), records.size());
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testLogPropertiesNullProperties() {
        try {
            Manager manager = getMockManagerWithEmptyResults();
            manager.logProperties();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(0, records.size());
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryNissingFile() {
        AbstractManager.getAdhocQuery(INVALID_FILE_PATH);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testGetAdhocQueryNull() {
        AbstractManager.getAdhocQuery(null);
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryEmptyString() {
        AbstractManager.getAdhocQuery("");
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryBlankString() {
        AbstractManager.getAdhocQuery("    ");
        fail();
    }

    @Test
    public void testGetAdhocQueryFromClassloader() {
        String result = AbstractManager.getAdhocQuery(SELECTOR_FILE_NAME);
        assertEquals(selectorAsText, result);
    }

    @Test
    public void testGetAdhocQueryFromFile() {
        String result = AbstractManager.getAdhocQuery(SELECTOR_FILE_PATH);
        assertEquals(selectorAsText, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryFromDir() {
        AbstractManager.getAdhocQuery(PROPERTIES_FILE_DIR);
        fail();
    }

    @Test
    public void testGetProperties() {
        AbstractManager manager = new AbstractManagerImpl();
        Properties result = manager.getProperties();
        assertNotNull(result);
    }

    @Test
    public void testGetOptions() {
        AbstractManager manager = new AbstractManagerImpl();
        TransformOptions result = manager.getOptions();
        assertNotNull(result);
    }

    @Test
    public void testInitPropertiesFromOptionsFile() {
        System.setProperty(Options.OPTIONS_FILE, PROPERTIES_FILE_NAME);
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.initPropertiesFromOptionsFile();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        Map properties = manager.getProperties();
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
    }

    @Test
    public void testInitStringArr() {
        try {
            String[] args = null;
            AbstractManager manager = new AbstractManagerImpl();
            manager.init(args);
            assertNull(manager.properties);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitProperties() {
        try {
            Properties props = new Properties();
            AbstractManager manager = new AbstractManagerImpl();
            manager.init(props);
            assertTrue(manager.properties.isEmpty());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitStringArrProperties() {
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.init(args, props);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        Map properties = manager.getProperties();
        assertTrue(properties.containsKey(KEY));
    }

    @Test
    public void testInitDecrypterNoDecrypterConfigured() {
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.initDecrypter();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNull(manager.decrypter);
    }

    @Test (expected=CorbException.class)
    public void testInitDecrypterBadClassname() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.DECRYPTER, "bogus");
        manager.initDecrypter();
    }

    @Test
    public void testInitDecrypterValidDecrypter() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.DECRYPTER, JasyptDecrypter.class.getName());
        try {
            manager.initDecrypter();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertTrue(manager.decrypter instanceof com.marklogic.developer.corb.JasyptDecrypter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitDecrypterInvalidDecrypter() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.DECRYPTER, String.class.getName());
        try {
            manager.initDecrypter();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testTryToDecryptUriInPartsClearText() {
        String origUri = "xcc://user:pass123@somehost:8000/FFE";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals(origUri, newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsClearTextWithEncoding() {
        String origUri = "xcc://user:pass#123@somehost:8000/FFE";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass%23123@somehost:8000/FFE", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsPasswordEncrypted() {
        String origUri = "xcc://user:zSSKlCXLWCaZp/LrbX0k0juz6+D5sUbr@somehost:8000/FFE";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass%23123@somehost:8000/FFE", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsPasswordEnocoded() {
        String origUri = "xcc://user:ENC(TThPQnCGGMjO7soUk/Kb8o1DQE7kUOVW)@somehost:8000/FFE";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("Since # is in password, auto should encode", "xcc://user:pass%23123@somehost:8000/FFE", newUri);
            newUri = manager.tryToDecryptUriInParts(origUri, "always");
            assertEquals("Same with always, encode it", "xcc://user:pass%23123@somehost:8000/FFE", newUri);
            newUri = manager.tryToDecryptUriInParts(origUri, "maybe");
            assertEquals("If not always or never, then auto","xcc://user:pass%23123@somehost:8000/FFE", newUri);
            newUri = manager.tryToDecryptUriInParts(origUri, "never");
            assertEquals("never means do not URLEncode even if it needs it", "xcc://user:pass#123@somehost:8000/FFE", newUri);
            newUri = manager.tryToDecryptUriInParts(origUri, "NEVER");
            assertEquals("Even case insensitive match on NEVER means do not URLEncode", "xcc://user:pass#123@somehost:8000/FFE", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsMultiPartEncryption() {
        String origUri = "xcc://ENC(v5b+VYiO4qpoZmVGkWieDg==):TThPQnCGGMjO7soUk/Kb8o1DQE7kUOVW@e+nz4XEN9SuuazGnbr6Ec5cyid1/l0qn:8000";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass%23123@somehost:8000", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsWithTrailingSlash() {
        String origUri = "xcc://user:pass@localhost:8000/";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsWithQuerystringParamNoValue() {
        String origUri = "xcc://user:pass@localhost:8000?apikey";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    public void testTryToDecryptUriInPartsWithUnsupportedQuerystringParams() {
        String origUri = "xcc://user:pass@localhost:8000?foo=bar";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    public void testTryToDecryptUriInPartsWithMarkLogicCloud() {
        String origUri = "xcc://user:pass@localhost:8000?basepath=/testpath&apikey=test123";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "never");
            assertEquals("xcc://user:pass@localhost:8000?apikey=test123&basepath=/testpath", newUri);
            newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000?apikey=test123&basepath=%2Ftestpath", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    public void testTryToDecryptUriInPartsWithMarkLogicCloudAllParams() {
        String origUri = "xcc://user:pass@localhost:8000?basepath=/testpath&apikey=test123&tokenduration=60&tokenendpoint=/token&grantType=apikey";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "never");
            assertEquals("xcc://user:pass@localhost:8000?apikey=test123&basepath=/testpath&granttype=apikey&tokenduration=60&tokenendpoint=/token", newUri);
            newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000?apikey=test123&basepath=%2Ftestpath&granttype=apikey&tokenduration=60&tokenendpoint=%2Ftoken", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsWithOauth() {
        String origUri = "xcc://user:pass@localhost:8000?oauthtoken=test%20123";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000?oauthtoken=test+123", newUri);

            origUri = "xcc://user:pass@localhost:8000?oauthtoken=test+123";
            newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000?oauthtoken=test+123", newUri);


            origUri = "xcc://user:pass@localhost:8000?oauthtoken=test 123";
            newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("xcc://user:pass@localhost:8000?oauthtoken=test+123", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testTryToDecryptUriInPartsIncomplete() {
        String origUri = "xccs:// : @ENC(,yY9wGdp6RxGPl7x/EZj5DGMMryhEw0T2):0";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri,"auto");
            assertEquals("If the regex doesn't match, values won't be decoded", origUri, newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    public void testTryToDecryptUriInPartsNoSchemeOrAuth() {
        String origUri = "localhost:8000";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri,"auto");
            assertEquals("If the regex doesn't match, values won't be decoded", origUri, newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    public void testTryToDecryptUriInPartsNoOauthValue() {
        String origUri = "xcc://localhost:8000?oauthtoken=";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri,"auto");
            assertEquals("If the regex doesn't match, values won't be decoded", "xcc://localhost:8000", newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }
    @Test
    public void testTryToDecryptUriInPartsWithTrailingSpace() {
        String origUri = "xcc://user:pass@localhost:8000 ";
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertEquals("Fails to match regex, bypassing some parsing", origUri, newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testTryToDecryptUriInPartsNullURI() {
        String origUri = null;
        try {
            AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
            String newUri = manager.tryToDecryptUriInParts(origUri, "auto");
            assertNull(newUri);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsWithXCCHTTPCompliantTrue() {
        String[] args = {};
        AbstractManager manager = AbstractManagerImpl.instanceWithXccHttpCompliantValue(Boolean.toString(true));
        try {
            manager.initOptions(args);
            assertEquals(Boolean.toString(true), System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsWithXCCHTTPCompliantFalse() {
        String[] args = {};
        AbstractManager manager = AbstractManagerImpl.instanceWithXccHttpCompliantValue(Boolean.toString(false));
        try {
            manager.initOptions(args);
            assertEquals(Boolean.toString(false), System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsWithXCCHTTPCompliantBlank() {
        String[] args = {};
        AbstractManager manager = AbstractManagerImpl.instanceWithXccHttpCompliantValue(AbstractManager.SPACE);
        try {
            manager.initOptions(args);
            if (originalHttpCompliantValue != null) {
                assertEquals(originalHttpCompliantValue, System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
            } else {
                assertEquals(Boolean.toString(true), System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitSSLConfig() {
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.initSSLConfig();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNotNull(manager.sslConfig);
    }

    @Test
    public void testInitSSLConfigCustomClass() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_CONFIG_CLASS, TwoWaySSLConfig.class.getName());
        try {
            manager.initSSLConfig();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNotNull(manager.sslConfig);
        assertTrue(manager.sslConfig instanceof TwoWaySSLConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitSSLConfigInvalidConfigClass() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_CONFIG_CLASS, String.class.getName());
        try {
            manager.initSSLConfig();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitSSLConfigBadClass() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_CONFIG_CLASS, "not a valid class");
        manager.initSSLConfig();
    }

    private void checkContentSource(AbstractManager instance, String user, String host, String port, String dbname) throws CorbException {
		ContentSource contentSource = instance.getContentSourcePool().get();
	    assertNotNull(contentSource);

	    assertEquals(host, contentSource.getConnectionProvider().getHostName());
	    if (port != null) {
            assertEquals(Integer.parseInt(port), contentSource.getConnectionProvider().getPort());
        }

	    String csToStr = contentSource.toString();
	    if (user != null) {
            assertTrue(csToStr.contains("user=" + user));
        }
	    if (dbname != null) {
            assertTrue(csToStr.contains("cb=" + dbname));
        } else {
	        assertTrue(csToStr.contains("cb={none}"));
        }
	}


    @Test
    public void testInitConnectionManager() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.initContentSourcePool(XCC_CONNECTION_URI);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, XCC_URI_USER, XCC_URI_HOST, XCC_URI_PORT, XCC_URI_DB);
    }

    @Test
    public void testInitURIArgsTakePrecedenceOverProperties() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            manager.initContentSourcePool(XCC_CONNECTION_URI);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager,XCC_URI_USER,XCC_URI_HOST,XCC_URI_PORT,XCC_URI_DB);
    }
    @Test
    public void testInitNoAuth() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.properties.setProperty(Options.XCC_DBNAME, XCC_URI_DB);
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, "{none}", XCC_URI_HOST, PORT, XCC_URI_DB);
    }
    @Test
    public void testInitKitchenSink() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_PROTOCOL, "xccs");
        manager.properties.setProperty(Options.XCC_USERNAME, "admin-user");
        manager.properties.setProperty(Options.XCC_PASSWORD, "secret");
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.properties.setProperty(Options.XCC_DBNAME, XCC_URI_DB);
        manager.properties.setProperty(Options.XCC_API_KEY, "myCustomKey");
        manager.properties.setProperty(Options.XCC_BASE_PATH, "base");
        manager.properties.setProperty(Options.XCC_GRANT_TYPE, "customGrantType");
        manager.properties.setProperty(Options.XCC_OAUTH_TOKEN, "oauth");
        manager.properties.setProperty(Options.XCC_TOKEN_DURATION, "61");
        manager.properties.setProperty(Options.XCC_TOKEN_ENDPOINT, "/token");
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        ContentSource contentSource = manager.getContentSourcePool().get();
        assertNotNull(contentSource);
        String contentSourceStr = contentSource.toString();
        ConnectionProvider provider = contentSource.getConnectionProvider();

        assertTrue(contentSourceStr.contains("SSLconn"));
        assertEquals(HOST, provider.getHostName());
        assertEquals(Integer.parseInt(PORT), provider.getPort());
        assertTrue(contentSourceStr.contains("cb=" + XCC_URI_DB));
        // when presenting username/password, MarkLogic Cloud, and OAuth credentials - no username/password, MarkLogic Cloud auth takes precendence
        assertTrue(contentSourceStr.contains("user={none}"));
        Credentials.MLCloudAuthConfig cloudAuth = contentSource.getUserCredentials().getMLCloudAuthConfig();
        assertEquals("myCustomKey", new String(cloudAuth.getApiKey()));
        assertEquals("customGrantType", cloudAuth.getGrantType());
        assertEquals(61, cloudAuth.getTokenDuration());
    }
    @Test
    public void testInitURIOauth() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.properties.setProperty(Options.XCC_OAUTH_TOKEN, "oauth");
        manager.properties.setProperty(Options.XCC_BASE_PATH, "oauthBase");
        //contentBase, basePath
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        ContentSource contentSource = manager.getContentSourcePool().get();
        assertNotNull(contentSource);
        String contentSourceStr = contentSource.toString();
        ConnectionProvider provider = contentSource.getConnectionProvider();
        assertTrue(contentSourceStr.contains("user={none}"));
        assertEquals("Bearer oauth", contentSource.getUserCredentials().toOAuth());
    }
    @Test
    public void testInitURIWithOauth() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.initContentSourcePool("xcc://localhost:8000?oauthtoken=foobar");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        ContentSource contentSource = manager.getContentSourcePool().get();
        assertNotNull(contentSource);
        String contentSourceStr = contentSource.toString();
        ConnectionProvider provider = contentSource.getConnectionProvider();
        assertTrue(contentSourceStr.contains("user={none}"));
        assertEquals("Bearer foobar", contentSource.getUserCredentials().toOAuth());
    }
    @Test
    public void testInitURIWithMarkLogicCloudAuth() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        try {
            manager.initContentSourcePool("xcc://localhost:8000?basepath=base&apikey=foobar&tokenendpoint=endpoint&tokenduration=5");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        ContentSource contentSource = manager.getContentSourcePool().get();
        assertNotNull(contentSource);
        String contentSourceStr = contentSource.toString();
        ConnectionProvider provider = contentSource.getConnectionProvider();
        assertTrue(contentSourceStr.contains("user={none}"));
        assertEquals("foobar", new String(contentSource.getUserCredentials().getMLCloudAuthConfig().getApiKey()));
        assertEquals("endpoint", new String(contentSource.getUserCredentials().getMLCloudAuthConfig().getTokenEndpoint()));
        assertEquals("endpoint", contentSource.getUserCredentials().getMLCloudAuthConfig().getTokenEndpoint());
        assertEquals(5, contentSource.getUserCredentials().getMLCloudAuthConfig().getTokenDuration());
    }
    @Test
    public void testInitURIAsSystemPropertyOnly() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        System.clearProperty(Options.XCC_CONNECTION_URI);
        checkContentSource(manager,XCC_URI_USER,XCC_URI_HOST,XCC_URI_PORT,XCC_URI_DB);
    }

    @Test(expected = CorbException.class)
    public void testInitURIInvalidXCCURI() throws CorbException {
        String uriArg = "www.marklogic.com";
        AbstractManager manager = new AbstractManagerImpl();
        manager.initContentSourcePool(uriArg);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURI() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.initContentSourcePool(null);
        fail();
    }

    @Test
    public void testInitURINullURIWithValues() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            manager.initContentSourcePool(null);
            checkContentSource(manager,USERNAME,HOST,PORT,null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitURINullURIWithUnencodedValues() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, "p@ssword:+!");
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, USERNAME, HOST, PORT,null);
    }

    @Test
    public void testInitURINullURIWithUnencodedValues2() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, "p@ssword:+");
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.properties.setProperty(Options.XCC_DBNAME, "documents database");
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, USERNAME, HOST, PORT,"documents+database");
    }

    @Test
    public void testInitURINullURIWithEncodedValues() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, "p%40assword%2B%3A");
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.properties.setProperty(Options.XCC_DBNAME, "documents%20database");
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, USERNAME, HOST, PORT,"documents database");
    }

    @Test
    public void testInitURINullURIWithEncodedValuesNoDatabase() throws CorbException {
        AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, USERNAME, HOST, PORT,null);
    }

    @Test
    public void testInitURIWithMultipleHosts() throws CorbException {
        AbstractManager manager = AbstractManagerImpl.instanceWithJasypt();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST + ",yY9wGdp6RxGPl7x/EZj5DGMMryhEw0T2, ENC(,yY9wGdp6RxGPl7x/EZj5DGMMryhEw0T2)");
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.properties.setProperty(Options.XCC_DBNAME, "documents%20database");
        try {
            manager.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(manager, USERNAME, HOST, PORT,"documents database");

        assertEquals(3, manager.csp.getAllContentSources().length);
        for (ContentSource contentSource : manager.csp.getAllContentSources()) {
            assertEquals("Verify that all hostname values are localhost", HOST, contentSource.getConnectionProvider().getHostName());
        }
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithPassword() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        manager.initContentSourcePool(null);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithPort() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_PORT, PORT);
        manager.initContentSourcePool(null);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithHostname() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        manager.initContentSourcePool(null);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithUsername() throws CorbException {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        manager.initContentSourcePool(null);
        fail();
    }

    @Test
    public void testGetOptionEmptyName() {
        String argVal = "";
        String propName = "";
        AbstractManager manager = new AbstractManagerImpl();
        assertNull(manager.getOption(argVal, propName));
    }

    @Test
    public void testGetOption() {
        String key = "option";
        String val = VALUE;
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(key, val);
        assertEquals(val, manager.getOption(key));
    }

    @Test
    public void testGetOptionWithEmptyString() {
        String key = "option";
        String val = "";
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(key, val);
        assertNull(manager.getOption(key));
    }

    @Test
    public void testGetOptionKebabWithSnake() {
        String key = "MAX_OPTS_FROM_MODULE";
        String val = VALUE;
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(key, val);
        assertEquals("Ensure that Snake_Case property is retrieved when looking for kebab", val, manager.getOption(Options.MAX_OPTS_FROM_MODULE));
    }

    @Test
    public void testGetOptionSnakeWithKebab() {
        String key = "MAX_OPTS_FROM_MODULE";
        String val = VALUE;
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.MAX_OPTS_FROM_MODULE, val);
        assertEquals("Ensure that Kebab-Case property is retrieved when looking for snake", val, manager.getOption(key));
    }

    @Test
    public void testGetOptionKebabWithBoth() {
        String key = "MAX_OPTS_FROM_MODULE";
        String val = VALUE;
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.MAX_OPTS_FROM_MODULE, val);
        manager.properties.setProperty(key, "wrong");
        assertEquals("Ensure that when both kebab and snake case properties set, and you ask for kebab, you get it", val, manager.getOption(Options.MAX_OPTS_FROM_MODULE));
    }

    @Test
    public void testGetOptionSnakeWithBoth() {
        String key = "MAX_OPTS_FROM_MODULE";
        String val = VALUE;
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.MAX_OPTS_FROM_MODULE, "wrong");
        manager.properties.setProperty(key, val);
        assertEquals("Ensure that when both kebab and snake case properties set, and you ask for snake, you get it", val, manager.getOption(key));
    }

    @Test
    public void testGetOptionMixedSnakeAndKebabProperty() {
        String key = "MAX-OPTS_FROM-MODULE";
        String val = VALUE;
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(key, val);
        assertNull("Although keys are normalized to snake and kebab case, if a property was specified with a mix, it won't be found",  manager.getOption(Options.MAX_OPTS_FROM_MODULE));
    }

    @Test
    public void testGetOptionMaxOptsFromModuleNotFound() {
        String key = "MAX_OPTS_FROM_MODULE";
        AbstractManager manager = new AbstractManagerImpl();
        assertNull("Not found", manager.getOption(key));
    }

    @Test
    public void testGetOptionPaddedValue() {
        String key = "option2";
        String val = "value2  ";
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(key, val);
        assertEquals(val.trim(), manager.getOption(key));
    }

    @Test
    public void testGetContentSourceManager() {
        AbstractManager manager = new AbstractManagerImpl();
        ContentSourcePool result = manager.getContentSourcePool();
        assertNull(result);
    }

    @Test
    public void testUsage() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outContent));
        AbstractManager manager = new AbstractManagerImpl();
        manager.usage();

        String usage = outContent.toString();
        assertNotNull(usage);
    }

    @Test
    public void testHasUsage() {
        assertTrue(Manager.hasUsage("-h"));
        assertTrue(Manager.hasUsage("--help"));
        assertTrue(Manager.hasUsage("--usage"));
        assertFalse(Manager.hasUsage("--nousage"));
        assertTrue(Manager.hasUsage("-foo", "--usage", "--bar"));
    }

    @Test
    public void testLogRuntimeArgs() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.logRuntimeArgs();

        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertTrue(records.get(0).getMessage().startsWith("runtime arguments = "));
    }

    @Test(expected = CorbException.class)
    public void testCreateContentSourceManagerBadClassname() throws CorbException{
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.CONTENT_SOURCE_POOL, "does-not-exist");
        manager.createContentSourcePool();
    }

    @Test(expected = CorbException.class)
    public void testCreateContentSourceManagerNotCSP() throws CorbException{
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.CONTENT_SOURCE_POOL, "com.marklogic.developer.corb.Manager");
        manager.createContentSourcePool();
    }

    @Test
    public void testCreateContentSourceManager() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.CONTENT_SOURCE_POOL, TestContentSourcePool.class.getName());
        try {
            ContentSourcePool csp = manager.createContentSourcePool();
            assertTrue(csp instanceof TestContentSourcePool);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void testTwoWaySSLConfigAutoConfig() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_KEYSTORE, "publicKey.pem");
        manager.properties.setProperty(Options.SSL_KEYSTORE_PASSWORD, "password");
        try {
            manager.initSSLConfig();
            assertEquals("Since both keystore and keystore password configured, and no explicit ",
                TwoWaySSLConfig.class, manager.sslConfig.getClass());
        } catch (CorbException ex){
            fail();
        }
    }
    @Test
    public void testTwoWaySSLConfigAutoConfigWithSSLKEYPASSWORD() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_KEYSTORE, "publicKey.pem");
        manager.properties.setProperty(Options.SSL_KEY_PASSWORD, "password");
        try {
            manager.initSSLConfig();
            assertEquals("Since both keystore and keystore password configured, and no explicit ",
                TwoWaySSLConfig.class, manager.sslConfig.getClass());
        } catch (CorbException ex){
            fail();
        }
    }
    @Test
    public void testTwoWaySSLConfigAutoConfigNotSet() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_KEYSTORE, "publicKey.pem");
        try {
            manager.initSSLConfig();
            assertEquals("Since no password configured, TwoWaySSLConfig not auto-configured",
                TrustAnyoneSSLConfig.class, manager.sslConfig.getClass());
        } catch (CorbException ex){
            fail();
        }
    }
    @Test
    public void testTwoWaySSLConfigAutoConfigNotOverridingExplicitConfig() {
        AbstractManager manager = new AbstractManagerImpl();
        manager.properties.setProperty(Options.SSL_KEYSTORE, "publicKey.pem");
        manager.properties.setProperty(Options.SSL_KEYSTORE_PASSWORD, "password");
        manager.properties.setProperty(Options.SSL_CONFIG_CLASS, TrustAnyoneSSLConfig.class.getCanonicalName());
        try {
            manager.initSSLConfig();
            assertEquals("Since SSL-CONFIG-CLASS explicitly configured, don't auto-configure TwoWaySSLConfig",
                TrustAnyoneSSLConfig.class, manager.sslConfig.getClass());
        } catch (CorbException ex){
            fail();
        }
    }

    public static class TestContentSourcePool extends DefaultContentSourcePool { }

    public static class AbstractManagerImpl extends AbstractManager {

        @Override
        public void init(String[] args, Properties props) throws CorbException {
            this.properties = props;
        }

        public static AbstractManager instanceWithXccHttpCompliantValue(String xccHttpCompliantValue) {
            System.clearProperty(PROPERTY_XCC_HTTPCOMPLIANT);

            Properties properties = new Properties();
            properties.setProperty(Options.XCC_HTTPCOMPLIANT, xccHttpCompliantValue);
            AbstractManager manager = new AbstractManagerImpl();
            manager.properties = properties;
            return manager;
        }

        public static AbstractManager instanceWithJasypt() throws CorbException {
            AbstractManager manager = new AbstractManagerImpl();
            manager.properties.setProperty(Options.DECRYPTER, JasyptDecrypter.class.getName());
            manager.properties.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
            manager.initDecrypter();
            return manager;
        }
    }

}
