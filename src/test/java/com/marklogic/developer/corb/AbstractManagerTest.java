/*
 * Copyright 2005-2015 MarkLogic Corporation
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
    private final TestHandler testLogger = new TestHandler();
    private String propertiesFilename = "helloWorld.properties";
    private String propertiesFileDir = "src/test/resources/";
    private String propertiesFilePath = propertiesFileDir + "/" + propertiesFilename;
    private String invalidFilePath = "does/not/exist";
    private String selectorFilename = "selector.xqy";
    private String selectorFilePath = "src/test/resources/" + selectorFilename;
    private String selectorAsText;

    public AbstractManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws FileNotFoundException {
        Logger logger = Logger.getLogger(AbstractManager.class.getSimpleName());
        logger.addHandler(testLogger);
        clearSystemProperties();
        String text = TestUtils.readFile(selectorFilePath);
        selectorAsText = text.trim();
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFile_String() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties result = AbstractManager.loadPropertiesFile(propertiesFilePath);
        assertNotNull(result);
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFile_String_boolean() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, false);
        assertNotNull(result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_String_boolean_MissingFileThrowsException() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, true);
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFile_3args() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties props = new Properties();
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, false, props);
        assertEquals(props, result);
        assertTrue(props.isEmpty());
    }

    @Test
    public void testLoadPropertiesFile_3args_loadFromClasspath() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile(propertiesFilename, false, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() > 1);
    }

    @Test
    public void testLoadPropertiesFile_3args_existingPropertiesAndBadPath() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, false, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() == 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_3args_existingPropertiesAndBadPathThrows() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, true, props);
    }

    @Test
    public void testLoadPropertiesFile_3args_existingPropertiesAndBlankPath() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile("    ", true, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() == 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_3args_forDirectory() throws Exception {
        System.out.println("loadPropertiesFile");
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile(propertiesFileDir, true, props);
    }

    /**
     * Test of getAdhocQuery method, of class AbstractManager.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_missingFile() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery(invalidFilePath);
    }

    @Test(expected = NullPointerException.class)
    public void testGetAdhocQuery_null() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_emptyString() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery("");
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_blankString() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery("    ");
    }

    @Test
    public void testGetAdhocQuery_fromClassloader() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery(selectorFilename);
        assertEquals(selectorAsText, result);
    }

    @Test
    public void testGetAdhocQuery_fromFile() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery(selectorFilePath);
        assertEquals(selectorAsText, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_fromDir() {
        System.out.println("getAdhocQuery");
        String result = AbstractManager.getAdhocQuery(propertiesFileDir);
    }

    /**
     * Test of getProperties method, of class AbstractManager.
     */
    @Test
    public void testGetProperties() {
        System.out.println("getProperties");
        AbstractManager instance = new AbstractManagerImpl();
        Properties result = instance.getProperties();
        assertNotNull(result);
    }

    /**
     * Test of getOptions method, of class AbstractManager.
     */
    @Test
    public void testGetOptions() {
        System.out.println("getOptions");
        AbstractManager instance = new AbstractManagerImpl();
        TransformOptions result = instance.getOptions();
        assertNotNull(result);
    }

    /**
     * Test of initPropertiesFromOptionsFile method, of class AbstractManager.
     */
    @Test
    public void testInitPropertiesFromOptionsFile() throws Exception {
        System.out.println("initPropertiesFromOptionsFile");
        System.setProperty("OPTIONS-FILE", propertiesFilename);
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
    public void testInit_StringArr() throws Exception {
        System.out.println("init");
        String[] args = null;
        AbstractManager instance = new AbstractManagerImpl();
        instance.init(args);
    }

    /**
     * Test of init method, of class AbstractManager.
     */
    @Test
    public void testInit_StringArr_Properties() throws Exception {
        System.out.println("init");
        String[] args = null;
        Properties props = new Properties();
        props.setProperty("key", "value");
        AbstractManager instance = new AbstractManagerImpl();
        instance.init(args, props);
        Properties properties = instance.getProperties();
        assertTrue(properties.containsKey("key"));
    }

    /**
     * Test of initDecrypter method, of class AbstractManager.
     */
    @Test
    public void testInitDecrypter() throws Exception {
        System.out.println("initDecrypter");
        AbstractManager instance = new AbstractManagerImpl();
        instance.initDecrypter();

    }

    @Test
    public void testInitDecrypter_validDecrypter() throws Exception {
        System.out.println("initDecrypter");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("DECRYPTER", "com.marklogic.developer.corb.JasyptDecrypter");
        instance.initDecrypter();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitDecrypter_invalidDecrypter() throws Exception {
        System.out.println("initDecrypter");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("DECRYPTER", "java.lang.String");
        instance.initDecrypter();
    }

    /**
     * Test of initSSLConfig method, of class AbstractManager.
     */
    @Test
    public void testInitSSLConfig() throws Exception {
        System.out.println("initSSLConfig");
        AbstractManager instance = new AbstractManagerImpl();
        instance.initSSLConfig();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitSSLConfig_invalidConfigClass() throws Exception {
        System.out.println("initSSLConfig");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("SSL-CONFIG-CLASS", "java.lang.String");
        instance.initSSLConfig();
    }

    /**
     * Test of initURI method, of class AbstractManager.
     */
    @Test
    public void testInitURI() throws Exception {
        System.out.println("initURI");
        String uriArg = "xcc://foo:bar@localhost:8008/baz";
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(uriArg);
        assertEquals(uriArg, instance.connectionUri.toString());
    }

    @Test
    public void testInitURI_URI_withValues() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-USERNAME", "username");
        instance.properties.setProperty("XCC-PASSWORD", "password");
        instance.properties.setProperty("XCC-HOSTNAME", "localhost");
        instance.properties.setProperty("XCC-PORT", "80");
        instance.initURI("xcc://foo:bar@localhost:8008/baz");
    }

    @Test
    public void testInitURI_invalidURI() throws Exception {
        System.out.println("initURI");
        String uriArg = "http://www.marklogic.com";
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(uriArg);
    }

    @Test
    public void testInitURI_nullURI() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        exit.expectSystemExit();
        instance.initURI(null);
    }

    @Test
    public void testInitURI_nullURI_withValues() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-USERNAME", "username");
        instance.properties.setProperty("XCC-PASSWORD", "password");
        instance.properties.setProperty("XCC-HOSTNAME", "localhost");
        instance.properties.setProperty("XCC-PORT", "80");
        instance.initURI(null);
    }

    @Test
    public void testInitURI_nullURI_withPassword() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-PASSWORD", "password");
        exit.expectSystemExit();
        instance.initURI(null);
    }

    @Test
    public void testInitURI_nullURI_withPort() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-PORT", "80");
        exit.expectSystemExit();
        instance.initURI(null);
    }

    @Test
    public void testInitURI_nullURI_withHostname() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-HOSTNAME", "localhost");
        exit.expectSystemExit();
        instance.initURI(null);
    }

    @Test
    public void testInitURI_nullURI_withUsername() throws Exception {
        System.out.println("initURI");
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-USERNAME", "user");
        exit.expectSystemExit();
        instance.initURI(null);
    }

    /**
     * Test of getOption method, of class AbstractManager.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetOption() {
        System.out.println("getOption");
        String argVal = "";
        String propName = "";
        AbstractManager instance = new AbstractManagerImpl();
        String expResult = "";
        String result = instance.getOption(argVal, propName);

    }

    /**
     * Test of prepareContentSource method, of class AbstractManager.
     */
    @Test(expected = NullPointerException.class)
    public void testPrepareContentSource() throws Exception {
        System.out.println("prepareContentSource");
        AbstractManager instance = new AbstractManagerImpl();
        instance.prepareContentSource();
    }

    /**
     * Test of getSecurityOptions method, of class AbstractManager.
     */
    @Test
    public void testGetSecurityOptions() throws Exception {
        System.out.println("getSecurityOptions");
        AbstractManager instance = new AbstractManagerImpl();
        TrustAnyoneSSLConfig sslConfig = new TrustAnyoneSSLConfig();
        instance.sslConfig = new TrustAnyoneSSLConfig();
        SecurityOptions result = instance.getSecurityOptions();
        assertNotNull(result);
        Assert.assertArrayEquals(sslConfig.getSecurityOptions().getEnabledProtocols(), result.getEnabledProtocols());
    }

    @Test(expected = NullPointerException.class)
    public void testGetSecurityOptions_nullPointer() throws Exception {
        System.out.println("getSecurityOptions");
        AbstractManager instance = new AbstractManagerImpl();
        SecurityOptions expResult = null;
        SecurityOptions result = instance.getSecurityOptions();
        assertEquals(expResult, result);
    }

    /**
     * Test of getContentSource method, of class AbstractManager.
     */
    @Test
    public void testGetContentSource() {
        System.out.println("getContentSource");
        AbstractManager instance = new AbstractManagerImpl();
        ContentSource expResult = null;
        ContentSource result = instance.getContentSource();
        assertEquals(expResult, result);
    }

    /**
     * Test of usage method, of class AbstractManager.
     */
    @Test
    public void testUsage() {
        System.out.println("usage");
        AbstractManager instance = new AbstractManagerImpl();
        instance.usage();
    }

    /**
     * Test of logRuntimeArgs method, of class AbstractManager.
     */
    @Test
    public void testLogRuntimeArgs() {
        System.out.println("logRuntimeArgs");
        AbstractManager instance = new AbstractManagerImpl();
        instance.logRuntimeArgs();

        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("runtime arguments = {0}", records.get(0).getMessage());
    }

    public class AbstractManagerImpl extends AbstractManager {

        public void init(String[] args, Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
            this.properties = props;
        }

        public void usage() {

        }
    }

}
