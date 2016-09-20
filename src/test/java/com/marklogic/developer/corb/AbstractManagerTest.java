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
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
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
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractManagerTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private static final Logger LOG = Logger.getLogger(AbstractManager.class.getName());
    private final TestHandler testLogger = new TestHandler();
    private static final String XCC_CONNECTION_URI = "xcc://foo:bar@localhost:8008/baz";
    private static final String PROPERTIES_FILE_NAME = "helloWorld.properties";
    private static final String PROPERTIES_FILE_DIR = "src/test/resources/";
    private static final String PROPERTIES_FILE_PATH = PROPERTIES_FILE_DIR + "/" + PROPERTIES_FILE_NAME;
    private static final String INVALID_FILE_PATH = "does/not/exist";
    private static final String SELECTOR_FILE_NAME = "selector.xqy";
    private static final String SELECTOR_FILE_PATH = "src/test/resources/" + SELECTOR_FILE_NAME;
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private String selectorAsText;
    private String username = "username";
    private String password = "password";
    private String host = "localhost";
    private String port = "80";
    private PrintStream systemErr = System.err;

    @Before
    public void setUp() throws FileNotFoundException {
        LOG.addHandler(testLogger);
        clearSystemProperties();
        String text = TestUtils.readFile(SELECTOR_FILE_PATH);
        selectorAsText = text.trim();
    }

    @After
    public void tearDown() {
        clearSystemProperties();
        System.setErr(systemErr);
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFileString() throws IOException {
        Properties result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_PATH);
        assertNotNull(result);
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFileStringBoolean() throws IOException {
        Properties result = AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, false);
        assertNotNull(result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFileStringBooleanMissingFileThrowsException() throws Exception {
        AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, true);
        fail();
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFile3args() throws IOException {
        Properties props = new Properties();
        Properties result = AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, false, props);
        assertEquals(props, result);
        assertTrue(props.isEmpty());
    }

    @Test
    public void testLoadPropertiesFile3argsLoadFromClasspath() throws IOException {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        Properties result = AbstractManager.loadPropertiesFile(PROPERTIES_FILE_NAME, false, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() > 1);
    }

    @Test
    public void testLoadPropertiesFile3argsExistingPropertiesAndBadPath() throws IOException {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        Properties result = AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, false, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() == 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile3argsExistingPropertiesAndBadPathThrows() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        AbstractManager.loadPropertiesFile(INVALID_FILE_PATH, true, props);
        fail();
    }

    @Test
    public void testLoadPropertiesFile3argsExistingPropertiesAndBlankPath() throws IOException {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        Properties result = AbstractManager.loadPropertiesFile("    ", true, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() == 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile3argsForDirectory() throws Exception {
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        AbstractManager.loadPropertiesFile(PROPERTIES_FILE_DIR, true, props);
        fail();
    }

    /**
     * Test of getAdhocQuery method, of class AbstractManager.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryNissingFile() throws Exception {
        AbstractManager.getAdhocQuery(INVALID_FILE_PATH);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testGetAdhocQueryNull() throws Exception {
        AbstractManager.getAdhocQuery(null);
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryEmptyString() throws Exception {
        AbstractManager.getAdhocQuery("");
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQueryBlankString() throws Exception {
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

    /**
     * Test of getProperties method, of class AbstractManager.
     */
    @Test
    public void testGetProperties() {
        AbstractManager instance = new AbstractManagerImpl();
        Properties result = instance.getProperties();
        assertNotNull(result);
    }

    /**
     * Test of getOptions method, of class AbstractManager.
     */
    @Test
    public void testGetOptions() {
        AbstractManager instance = new AbstractManagerImpl();
        TransformOptions result = instance.getOptions();
        assertNotNull(result);
    }

    /**
     * Test of initPropertiesFromOptionsFile method, of class AbstractManager.
     */
    @Test
    public void testInitPropertiesFromOptionsFile() throws IOException {
        System.setProperty(Options.OPTIONS_FILE, PROPERTIES_FILE_NAME);
        AbstractManager instance = new AbstractManagerImpl();
        instance.initPropertiesFromOptionsFile();
        Properties properties = instance.getProperties();
        assertNotNull(properties);
        assertFalse(properties.isEmpty());
    }

    /**
     * Test of init method, of class AbstractManager.
     */
    @Test
    public void testInitStringArr() throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
        String[] args = null;
        AbstractManager instance = new AbstractManagerImpl();
        instance.init(args);
        assertNull(instance.properties);
    }

    /**
     * Test of init method, of class AbstractManager.
     */
    @Test
    public void testInitStringArrProperties()
            throws IOException, URISyntaxException, ClassNotFoundException,
            InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(KEY, VALUE);
        AbstractManager instance = new AbstractManagerImpl();
        instance.init(args, props);
        Properties properties = instance.getProperties();
        assertTrue(properties.containsKey(KEY));
    }

    /**
     * Test of initDecrypter method, of class AbstractManager.
     */
    @Test
    public void testInitDecrypterNoDecrypterConfigured() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initDecrypter();
        assertNull(instance.decrypter);
    }

    @Test
    public void testInitDecrypterValidDecrypter() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.DECRYPTER, JasyptDecrypter.class.getName());
        instance.initDecrypter();
        assertTrue(instance.decrypter instanceof com.marklogic.developer.corb.JasyptDecrypter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitDecrypterInvalidDecrypter() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.DECRYPTER, String.class.getName());
        instance.initDecrypter();
        fail();
    }

    /**
     * Test of initSSLConfig method, of class AbstractManager.
     */
    @Test
    public void testInitSSLConfig() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initSSLConfig();
        assertNotNull(instance.sslConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitSSLConfigInvalidConfigClass() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.SSL_CONFIG_CLASS, String.class.getName());
        instance.initSSLConfig();
        fail();
    }

    /**
     * Test of initURI method, of class AbstractManager.
     */
    @Test
    public void testInitURI() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(XCC_CONNECTION_URI);
        assertEquals(XCC_CONNECTION_URI, instance.connectionUri.toString());
    }

    @Test
    public void testInitURIArgsTakePrecedenceOverProperties() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, username);
        instance.properties.setProperty(Options.XCC_PASSWORD, password);
        instance.properties.setProperty(Options.XCC_HOSTNAME, host);
        instance.properties.setProperty(Options.XCC_PORT, port);
        instance.initURI(XCC_CONNECTION_URI);
        assertEquals(XCC_CONNECTION_URI, instance.connectionUri.toString());
    }

    @Test
    public void testInitURIAsSystemPropertyOnly() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        instance.initURI(null);
        System.clearProperty(Options.XCC_CONNECTION_URI);
        assertEquals(XCC_CONNECTION_URI, instance.connectionUri.toString());
    }

    @Test
    public void testInitURIInvalidXCCURI() throws InstantiationException, URISyntaxException {
        String uriArg = "www.marklogic.com";
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(uriArg);
        assertEquals(uriArg, instance.connectionUri.toString());
    }

    @Test(expected = InstantiationException.class)
    public void testInitURINullURI() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(null);
        fail();
    }

    @Test
    public void testInitURINullURIWithValues() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, username);
        instance.properties.setProperty(Options.XCC_PASSWORD, password);
        instance.properties.setProperty(Options.XCC_HOSTNAME, host);
        instance.properties.setProperty(Options.XCC_PORT, port);
        instance.initURI(null);
        assertEquals("xcc://username:password@localhost:80", instance.connectionUri.toString());
    }

    @Test
    public void testInitURINullURIWithUnencodedValues() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, username);
        instance.properties.setProperty(Options.XCC_PASSWORD, "p@ssword:+!");
        instance.properties.setProperty(Options.XCC_HOSTNAME, host);
        instance.properties.setProperty(Options.XCC_PORT, port);
        instance.initURI(null);
        assertEquals("xcc://username:p%40ssword%3A%2B%21@localhost:80", instance.connectionUri.toString());
    }

    @Test
    public void testInitURINullURIWithUnencodedValues2() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, username);
        instance.properties.setProperty(Options.XCC_PASSWORD, "p@ssword:+");
        instance.properties.setProperty(Options.XCC_HOSTNAME, host);
        instance.properties.setProperty(Options.XCC_PORT, port);
        instance.properties.setProperty(Options.XCC_DBNAME, "documents database");
        instance.initURI(null);
        assertEquals("xcc://username:p%40ssword%3A%2B@localhost:80/documents+database", instance.connectionUri.toString());
    }

    @Test
    public void testInitURINullURIWithEncodedValues() throws InstantiationException, URISyntaxException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, username);
        instance.properties.setProperty(Options.XCC_PASSWORD, "p%40assword%2B%3A");
        instance.properties.setProperty(Options.XCC_HOSTNAME, host);
        instance.properties.setProperty(Options.XCC_PORT, port);
        instance.properties.setProperty(Options.XCC_DBNAME, "documents%20database");
        instance.initURI(null);
        assertEquals("xcc://username:p%40assword%2B%3A@localhost:80/documents%20database", instance.connectionUri.toString());
    }

    @Test(expected = InstantiationException.class)
    public void testInitURINullURIWithPassword() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_PASSWORD, password);
        instance.initURI(null);
        fail();
    }

    @Test(expected = InstantiationException.class)
    public void testInitURINullURIWithPort() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_PORT, port);
        instance.initURI(null);
        fail();
    }

    @Test(expected = InstantiationException.class)
    public void testInitURINullURIWithHostname() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_HOSTNAME, host);
        instance.initURI(null);
        fail();
    }

    @Test(expected = InstantiationException.class)
    public void testInitURINullURIWithUsername() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(Options.XCC_USERNAME, username);
        instance.initURI(null);
        fail();
    }

    /**
     * Test of getOption method, of class AbstractManager.
     */
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
        assertEquals(0, instance.properties.size());
    }

    @Test
    public void testGetOptionPaddedValue() {
        String key = "option2";
        String val = "value2  ";
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(key, val);
        assertEquals(val.trim(), instance.getOption(key));
        assertEquals(0, instance.properties.size());
    }

    /**
     * Test of prepareContentSource method, of class AbstractManager.
     */
    @Test(expected = NullPointerException.class)
    public void testPrepareContentSourceNull() throws Exception {
        System.out.println("prepareContentSource");
        AbstractManager instance = new AbstractManagerImpl();
        instance.prepareContentSource();
        fail();
    }

    @Test
    public void testPrepareContentSourceSecureXCC() throws URISyntaxException, XccConfigException, GeneralSecurityException, RequestException {
        AbstractManager instance = new AbstractManagerImpl();
        instance.connectionUri = new URI("xccs://user:pass@localhost:8001");
        instance.sslConfig = mock(SSLConfig.class);
        instance.prepareContentSource();
        assertNotNull(instance.contentSource);
    }

    @Test(expected = XccConfigException.class)
    public void testPrepareContentSourceNoScheme() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.connectionUri = new URI("//user:pass@localhost:8001");
        instance.sslConfig = mock(SSLConfig.class);
        instance.prepareContentSource();
        fail();
    }

    /**
     * Test of getSecurityOptions method, of class AbstractManager.
     */
    @Test
    public void testGetSecurityOptions() throws KeyManagementException, NoSuchAlgorithmException {
        AbstractManager instance = new AbstractManagerImpl();
        TrustAnyoneSSLConfig sslConfig = new TrustAnyoneSSLConfig();
        instance.sslConfig = new TrustAnyoneSSLConfig();
        SecurityOptions result = instance.getSecurityOptions();
        SecurityOptions securityOptions = instance.getSecurityOptions();

        assertNotNull(securityOptions);
        assertArrayEquals(sslConfig.getSecurityOptions().getEnabledProtocols(), result.getEnabledProtocols());
    }

    @Test(expected = NullPointerException.class)
    public void testGetSecurityOptionsNullPointer() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.getSecurityOptions();
        fail();
    }

    /**
     * Test of getContentSource method, of class AbstractManager.
     */
    @Test
    public void testGetContentSource() {
        AbstractManager instance = new AbstractManagerImpl();
        ContentSource result = instance.getContentSource();
        assertNull(result);
    }

    /**
     * Test of usage method, of class AbstractManager.
     */
    @Test
    public void testUsage() {
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outContent));
        AbstractManager instance = new AbstractManagerImpl();
        instance.usage();

        String usage = outContent.toString();
        assertNotNull(usage);
    }

    /**
     * Test of logRuntimeArgs method, of class AbstractManager.
     */
    @Test
    public void testLogRuntimeArgs() {
        AbstractManager instance = new AbstractManagerImpl();
        instance.logRuntimeArgs();

        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("runtime arguments = {0}", records.get(0).getMessage());
    }

    public static class AbstractManagerImpl extends AbstractManager {

        @Override
        public void init(String[] args, Properties props) throws IOException, 
                URISyntaxException, ClassNotFoundException, InstantiationException, 
                IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
            this.properties = props;
        }

    }

}
