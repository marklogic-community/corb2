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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
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
    private static final Logger logger = Logger.getLogger(AbstractManager.class.getName());
    private final TestHandler testLogger = new TestHandler();
    private final String xccConnectionUri = "xcc://foo:bar@localhost:8008/baz";
    private final String propertiesFilename = "helloWorld.properties";
    private final String propertiesFileDir = "src/test/resources/";
    private final String propertiesFilePath = propertiesFileDir + "/" + propertiesFilename;
    private final String invalidFilePath = "does/not/exist";
    private final String selectorFilename = "selector.xqy";
    private final String selectorFilePath = "src/test/resources/" + selectorFilename;
    private String selectorAsText;

    @Before
    public void setUp() throws FileNotFoundException {
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
        Properties result = AbstractManager.loadPropertiesFile(propertiesFilePath);
        assertNotNull(result);
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFile_String_boolean() throws Exception {
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, false);
        assertNotNull(result);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_String_boolean_MissingFileThrowsException() throws Exception {
        AbstractManager.loadPropertiesFile(invalidFilePath, true);
        fail();
    }

    /**
     * Test of loadPropertiesFile method, of class AbstractManager.
     */
    @Test
    public void testLoadPropertiesFile_3args() throws Exception {
        Properties props = new Properties();
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, false, props);
        assertEquals(props, result);
        assertTrue(props.isEmpty());
    }

    @Test
    public void testLoadPropertiesFile_3args_loadFromClasspath() throws Exception {
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile(propertiesFilename, false, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() > 1);
    }

    @Test
    public void testLoadPropertiesFile_3args_existingPropertiesAndBadPath() throws Exception {
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile(invalidFilePath, false, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() == 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_3args_existingPropertiesAndBadPathThrows() throws Exception {
        Properties props = new Properties();
        props.setProperty("key", "value");
        AbstractManager.loadPropertiesFile(invalidFilePath, true, props);
        fail();
    }

    @Test
    public void testLoadPropertiesFile_3args_existingPropertiesAndBlankPath() throws Exception {
        Properties props = new Properties();
        props.setProperty("key", "value");
        Properties result = AbstractManager.loadPropertiesFile("    ", true, props);
        assertEquals(props, result);
        assertFalse(props.isEmpty());
        assertTrue(props.size() == 1);
    }

    @Test(expected = IllegalStateException.class)
    public void testLoadPropertiesFile_3args_forDirectory() throws Exception {
        Properties props = new Properties();
        props.setProperty("key", "value");
        AbstractManager.loadPropertiesFile(propertiesFileDir, true, props);
        fail();
    }

    /**
     * Test of getAdhocQuery method, of class AbstractManager.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_missingFile() {
        System.out.println("getAdhocQuery");
        AbstractManager.getAdhocQuery(invalidFilePath);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testGetAdhocQuery_null() {
        AbstractManager.getAdhocQuery(null);
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_emptyString() {
        AbstractManager.getAdhocQuery("");
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_blankString() {
        AbstractManager.getAdhocQuery("    ");
        fail();
    }

    @Test
    public void testGetAdhocQuery_fromClassloader() {
        String result = AbstractManager.getAdhocQuery(selectorFilename);
        assertEquals(selectorAsText, result);
    }

    @Test
    public void testGetAdhocQuery_fromFile() {
        String result = AbstractManager.getAdhocQuery(selectorFilePath);
        assertEquals(selectorAsText, result);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetAdhocQuery_fromDir() {
        AbstractManager.getAdhocQuery(propertiesFileDir);
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
    public void testInitPropertiesFromOptionsFile() throws Exception {
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
        String[] args = null;
        AbstractManager instance = new AbstractManagerImpl();
        instance.init(args);
    }

    /**
     * Test of init method, of class AbstractManager.
     */
    @Test
    public void testInit_StringArr_Properties() throws Exception {
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
    public void testInitDecrypter_noDecrypterConfigured() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initDecrypter();
        assertNull(instance.decrypter);
    }

    @Test
    public void testInitDecrypter_validDecrypter() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("DECRYPTER", "com.marklogic.developer.corb.JasyptDecrypter");
        instance.initDecrypter();
        assertTrue(instance.decrypter instanceof com.marklogic.developer.corb.JasyptDecrypter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitDecrypter_invalidDecrypter() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("DECRYPTER", "java.lang.String");
        instance.initDecrypter();
        fail();
    }

    /**
     * Test of initSSLConfig method, of class AbstractManager.
     */
    @Test
    public void testInitSSLConfig() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initSSLConfig();
        assertNotNull(instance.sslConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitSSLConfig_invalidConfigClass() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("SSL-CONFIG-CLASS", "java.lang.String");
        instance.initSSLConfig();
        fail();
    }

    /**
     * Test of initURI method, of class AbstractManager.
     */
    @Test
    public void testInitURI() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(xccConnectionUri);
        assertEquals(xccConnectionUri, instance.connectionUri.toString());
    }

    @Test
    public void testInitURI_argsTakePrecedenceOverProperties() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-USERNAME", "username");
        instance.properties.setProperty("XCC-PASSWORD", "password");
        instance.properties.setProperty("XCC-HOSTNAME", "localhost");
        instance.properties.setProperty("XCC-PORT", "80");
        instance.initURI(xccConnectionUri);
        assertEquals(xccConnectionUri, instance.connectionUri.toString());
    }

    @Test
    public void testInitURI_asSystemPropertyOnly() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        System.setProperty("XCC-CONNECTION-URI", xccConnectionUri);
        instance.initURI(null);
        System.clearProperty("XCC-CONNECTION-URI");
        assertEquals(xccConnectionUri, instance.connectionUri.toString());
    }

    @Test
    public void testInitURI_invalidXCCURI() throws Exception {
        String uriArg = "www.marklogic.com";
        AbstractManager instance = new AbstractManagerImpl();
        instance.initURI(uriArg);
        assertEquals("www.marklogic.com", instance.connectionUri.toString());
    }

    @Test
    public void testInitURI_nullURI() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        exit.expectSystemExit();
        instance.initURI(null);
    }

    @Test
    public void testInitURI_nullURI_withValues() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-USERNAME", "username");
        instance.properties.setProperty("XCC-PASSWORD", "password");
        instance.properties.setProperty("XCC-HOSTNAME", "localhost");
        instance.properties.setProperty("XCC-PORT", "80");
        instance.initURI(null);
        assertEquals("xcc://username:password@localhost:80", instance.connectionUri.toString());
    }

    @Test
    public void testInitURI_nullURI_withPassword() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-PASSWORD", "password");
        exit.expectSystemExit();
        instance.initURI(null);
        fail();
    }

    @Test
    public void testInitURI_nullURI_withPort() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-PORT", "80");
        exit.expectSystemExit();
        instance.initURI(null);
        fail();
    }

    @Test
    public void testInitURI_nullURI_withHostname() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-HOSTNAME", "localhost");
        exit.expectSystemExit();
        instance.initURI(null);
        fail();
    }

    @Test
    public void testInitURI_nullURI_withUsername() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty("XCC-USERNAME", "user");
        exit.expectSystemExit();
        instance.initURI(null);
        fail();
    }

    /**
     * Test of getOption method, of class AbstractManager.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetOption_emptyName() {
        String argVal = "";
        String propName = "";
        AbstractManager instance = new AbstractManagerImpl();
        instance.getOption(argVal, propName);
        fail();
    }

    @Test
    public void testGetOption() {
        String key = "foo";
        String val = "bar";
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(key, val);
        assertEquals(val, instance.getOption(key));
        assertEquals(0, instance.properties.size());
    }

    @Test
    public void testGetOption_paddedValue() {
        String key = "foo";
        String val = "bar  ";
        AbstractManager instance = new AbstractManagerImpl();
        instance.properties.setProperty(key, val);
        assertEquals(val.trim(), instance.getOption(key));
        assertEquals(0, instance.properties.size());
    }

    /**
     * Test of prepareContentSource method, of class AbstractManager.
     */
    @Test(expected = NullPointerException.class)
    public void testPrepareContentSource_null() throws Exception {
        System.out.println("prepareContentSource");
        AbstractManager instance = new AbstractManagerImpl();
        instance.prepareContentSource();
        fail();
    }

    @Test
    public void testPrepareContentSource_SecureXCC() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        instance.connectionUri = new URI("xccs://user:pass@localhost:8001");
        instance.sslConfig = mock(SSLConfig.class);
        instance.prepareContentSource();
        assertNotNull(instance.contentSource);
    }

    @Test(expected = XccConfigException.class)
    public void testPrepareContentSource_noScheme() throws Exception {
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
    public void testGetSecurityOptions() throws Exception {
        AbstractManager instance = new AbstractManagerImpl();
        TrustAnyoneSSLConfig sslConfig = new TrustAnyoneSSLConfig();
        instance.sslConfig = new TrustAnyoneSSLConfig();
        SecurityOptions result = instance.getSecurityOptions();
        SecurityOptions securityOptions = instance.getSecurityOptions();
        
        assertNotNull(securityOptions);
        Assert.assertArrayEquals(sslConfig.getSecurityOptions().getEnabledProtocols(), result.getEnabledProtocols());
    }

    @Test(expected = NullPointerException.class)
    public void testGetSecurityOptions_nullPointer() throws Exception {
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
        AbstractManager instance = new AbstractManagerImpl();
        instance.usage();
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
        public void init(String[] args, Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
            this.properties = props;
        }

        @Override
        public void usage() {

        }
    }

}
