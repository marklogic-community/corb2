/*
 * Copyright (c) 2004-2020 MarkLogic Corporation
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
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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

    private static final PrintStream ERROR = System.err;

    private String originalValue;

    @Before
    public void setUp() throws FileNotFoundException {
        ABSTRACT_MANAGER_LOG.addHandler(testLogger);
        clearSystemProperties();
        String text = TestUtils.readFile(SELECTOR_FILE_PATH);
        selectorAsText = text.trim();
        originalValue = System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
        System.setErr(ERROR);
        if (originalValue != null) {
            System.setProperty(PROPERTY_XCC_HTTPCOMPLIANT, originalValue);
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
        Manager instance;
        try {
            instance = getMockManagerWithEmptyResults();
            instance.properties = props;
            instance.logProperties();
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
            Manager instance = getMockManagerWithEmptyResults();
            instance.logProperties();
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
        AbstractManager instance = new AbstractManagerImpl();
        Properties result = instance.getProperties();
        assertNotNull(result);
    }

    @Test
    public void testGetOptions() {
        AbstractManager instance = new AbstractManagerImpl();
        TransformOptions result = instance.getOptions();
        assertNotNull(result);
    }

    @Test
    public void testInitPropertiesFromOptionsFile() {
        System.setProperty(Options.OPTIONS_FILE, PROPERTIES_FILE_NAME);
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.initPropertiesFromOptionsFile();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        Map properties = instance.getProperties();
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
    }

    @Test
    public void testInitStringArr() {
        try {
            String[] args = null;
            AbstractManager instance = new AbstractManagerImpl();
            instance.init(args);
            assertNull(instance.properties);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitProperties() {
        try {
            Properties props = new Properties();
            AbstractManager instance = new AbstractManagerImpl();
            instance.init(props);
            assertTrue(instance.properties.isEmpty());
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
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.init(args, props);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        Map properties = instance.getProperties();
        assertTrue(properties.containsKey(KEY));
    }

    @Test
    public void testInitDecrypterNoDecrypterConfigured() {
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.initDecrypter();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNull(instance.decrypter);
    }

    @Test (expected=CorbException.class)
    public void testInitDecrypterBadClassname() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.DECRYPTER, "bogus");
        instance.initDecrypter();
    }

    @Test
    public void testInitDecrypterValidDecrypter() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.DECRYPTER, JasyptDecrypter.class.getName());
        try {
            instance.initDecrypter();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertTrue(instance.decrypter instanceof com.marklogic.developer.corb.JasyptDecrypter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitDecrypterInvalidDecrypter() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.DECRYPTER, String.class.getName());
        try {
            instance.initDecrypter();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testInitOptionsWithXCCHTTPCompliantTrue() {
        String[] args = {};
        AbstractManager instance = AbstractManagerImpl.instanceWithXccHttpCompliantValue(Boolean.toString(true));
        try {
            instance.initOptions(args);
            assertEquals(Boolean.toString(true), System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsWithXCCHTTPCompliantFalse() {
        String[] args = {};
        AbstractManager instance = AbstractManagerImpl.instanceWithXccHttpCompliantValue(Boolean.toString(false));
        try {
            instance.initOptions(args);
            assertEquals(Boolean.toString(false), System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsWithXCCHTTPCompliantBlank() {
        String[] args = {};
        AbstractManager instance = AbstractManagerImpl.instanceWithXccHttpCompliantValue(AbstractManager.SPACE);
        try {
            instance.initOptions(args);
            assertEquals(originalValue, System.getProperty(PROPERTY_XCC_HTTPCOMPLIANT));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitSSLConfig() {
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.initSSLConfig();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNotNull(instance.sslConfig);
    }

    @Test
    public void testInitSSLConfigCustomClass() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.SSL_CONFIG_CLASS, TwoWaySSLConfig.class.getName());
        try {
            instance.initSSLConfig();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNotNull(instance.sslConfig);
        assertTrue(instance.sslConfig instanceof TwoWaySSLConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitSSLConfigInvalidConfigClass() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.SSL_CONFIG_CLASS, String.class.getName());
        try {
            instance.initSSLConfig();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitSSLConfigBadClass() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.SSL_CONFIG_CLASS, "not a valid class");
        instance.initSSLConfig();
    }

    private void checkContentSource(AbstractManager instance, String user, String host, String port, String dbname) throws CorbException{
		ContentSource contentSource = instance.getContentSourcePool().get();
	    assertNotNull(contentSource);

	    assertEquals(host, contentSource.getConnectionProvider().getHostName());
	    if (port != null) {
            assertEquals(Integer.parseInt(port), contentSource.getConnectionProvider().getPort());
        }

	    String csToStr = contentSource.toString();
	    if (user != null) {
            assertTrue(csToStr.contains("user="+user));
        }
	    if (dbname != null) {
            assertTrue(csToStr.contains("cb="+dbname));
        }
	}


    @Test
    public void testInitConnectionManager() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.initContentSourcePool(XCC_CONNECTION_URI);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(instance,XCC_URI_USER,XCC_URI_HOST,XCC_URI_PORT,XCC_URI_DB);
    }

    @Test
    public void testInitURIArgsTakePrecedenceOverProperties() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        instance.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        instance.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        instance.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            instance.initContentSourcePool(XCC_CONNECTION_URI);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(instance,XCC_URI_USER,XCC_URI_HOST,XCC_URI_PORT,XCC_URI_DB);
    }

    @Test
    public void testInitURIAsSystemPropertyOnly() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        try {
            instance.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        System.clearProperty(Options.XCC_CONNECTION_URI);
        checkContentSource(instance,XCC_URI_USER,XCC_URI_HOST,XCC_URI_PORT,XCC_URI_DB);
    }

    @Test(expected = CorbException.class)
    public void testInitURIInvalidXCCURI() throws CorbException {
        String uriArg = "www.marklogic.com";
        AbstractManager instance = new AbstractManagerImpl();
        instance.initContentSourcePool(uriArg);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURI() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initContentSourcePool(null);
        fail();
    }

    @Test
    public void testInitURINullURIWithValues() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        instance.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        instance.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        instance.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            instance.initContentSourcePool(null);
            checkContentSource(instance,USERNAME,HOST,PORT,null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitURINullURIWithUnencodedValues() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        instance.properties.setProperty(Options.XCC_PASSWORD, "p@ssword:+!");
        instance.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        instance.properties.setProperty(Options.XCC_PORT, PORT);
        try {
            instance.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(instance,USERNAME,HOST,PORT,null);
    }

    @Test
    public void testInitURINullURIWithUnencodedValues2() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        instance.properties.setProperty(Options.XCC_PASSWORD, "p@ssword:+");
        instance.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        instance.properties.setProperty(Options.XCC_PORT, PORT);
        instance.properties.setProperty(Options.XCC_DBNAME, "documents database");
        try {
            instance.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(instance,USERNAME,HOST,PORT,"documents+database");
    }

    @Test
    public void testInitURINullURIWithEncodedValues() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        instance.properties.setProperty(Options.XCC_PASSWORD, "p%40assword%2B%3A");
        instance.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        instance.properties.setProperty(Options.XCC_PORT, PORT);
        instance.properties.setProperty(Options.XCC_DBNAME, "documents%20database");
        try {
            instance.initContentSourcePool(null);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        checkContentSource(instance,USERNAME,HOST,PORT,"documents database");
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithPassword() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_PASSWORD, PASSWORD);
        instance.initContentSourcePool(null);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithPort() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_PORT, PORT);
        instance.initContentSourcePool(null);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithHostname() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_HOSTNAME, HOST);
        instance.initContentSourcePool(null);
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitURINullURIWithUsername() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, USERNAME);
        instance.initContentSourcePool(null);
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOptionEmptyName() {
        String argVal = "";
        String propName = "";
        AbstractManager instance = new AbstractManagerImpl();
        instance.getOption(argVal, propName);
        fail();
    }

    @Test
    public void testGetOption() {
        String key = "option";
        String val = VALUE;
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(key, val);
        assertEquals(val, instance.getOption(key));
    }

    @Test
    public void testGetOptionPaddedValue() {
        String key = "option2";
        String val = "value2  ";
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(key, val);
        assertEquals(val.trim(), instance.getOption(key));
    }

    @Test
    public void testGetContentSourceManager() {
        AbstractManager instance = new AbstractManagerImpl();
        ContentSourcePool result = instance.getContentSourcePool();
        assertNull(result);
    }

    @Test
    public void testUsage() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outContent));
        AbstractManager instance = new AbstractManagerImpl();
        instance.usage();

        String usage = outContent.toString();
        assertNotNull(usage);
    }

    @Test
    public void testLogRuntimeArgs() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.logRuntimeArgs();

        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertTrue(records.get(0).getMessage().startsWith("runtime arguments = "));
    }

    @Test(expected = CorbException.class)
    public void testCreateContentSourceManagerBadClassname() throws CorbException{
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.CONTENT_SOURCE_POOL, "does-not-exist");
        instance.createContentSourcePool();
    }

    @Test(expected = CorbException.class)
    public void testCreateContentSourceManagerNotCSP() throws CorbException{
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.CONTENT_SOURCE_POOL, "com.marklogic.developer.corb.Manager");
        instance.createContentSourcePool();
    }

    @Test
    public void testCreateContentSourceManager() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.CONTENT_SOURCE_POOL, TestContentSourcePool.class.getName());
        try {
            ContentSourcePool csp = instance.createContentSourcePool();
            assertTrue(csp instanceof TestContentSourcePool);
        } catch (CorbException ex) {
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
            AbstractManager instance = new AbstractManagerImpl();
            instance.properties = properties;
            return instance;
        }
    }

}
