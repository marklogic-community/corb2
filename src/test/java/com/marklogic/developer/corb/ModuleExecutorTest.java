/*
 * Copyright (c) 2004-2015 MarkLogic Corporation
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.*;
import static org.junit.Assert.*;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.jndi.ContentSourceBean;
import static com.marklogic.developer.corb.TestUtils.clearFile;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
import java.util.logging.Logger;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The class <code>ModuleExecutorTest</code> contains tests for the class
 * <code>{@link ModuleExecutor}</code>.
 *
 * @generatedBy CodePro at 9/18/15 12:45 PM
 * @author matthew.heckel
 * @version $Revision: 1.0 $
 */
public class ModuleExecutorTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private final TestHandler testLogger = new TestHandler();
    private static final String XCC_CONNECTION_URI = "xcc://admin:admin@localhost:2223/FFE";
    private static final String OPTIONS_FILE = "src/test/resources/helloWorld.properties";
    private static final String EXPORT_FILE_NAME = "src/test/resources/helloWorld.txt";
    private static final String PROCESS_MODULE = "src/test/resources/transform2.xqy|ADHOC";

    /**
     * Run the ModuleExecutor(URI) constructor test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testModuleExecutor_1()
            throws Exception {
        clearSystemProperties();
        ModuleExecutor result = new ModuleExecutor();
        assertNotNull(result);
    }

    private Properties loadProperties(URL filePath) {
        InputStream input = null;
        Properties prop = new Properties();
        try {
            input = filePath.openStream();
            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return prop;
    }

    /**
     * Run the ContentSource getContentSource() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetContentSource_1()
            throws Exception {
        clearSystemProperties();
        System.setProperty("OPTIONS-FILE", OPTIONS_FILE);
        System.setProperty("EXPORT-FILE-NAME", EXPORT_FILE_NAME);
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        ContentSource result = executor.getContentSource();

        assertNotNull(result);
    }

    protected String getOption(String argVal, String propName, Properties properties) {
        if (StringUtils.isNotBlank(argVal)) {
            return argVal.trim();
        } else if (StringUtils.isNotBlank(System.getProperty(propName))) {
            return System.getProperty(propName).trim();
        } else if (StringUtils.isNotBlank(properties.getProperty(propName))) {
            String val = properties.getProperty(propName).trim();
            return val;
        }
        return null;
    }

    /**
     * Run the String getOption(String,String,Properties) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetOption_1()
            throws Exception {
        clearSystemProperties();
        String argVal = "";
        String propName = "URIS-MODULE";
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();

        String result = getOption(argVal, propName, executor.getProperties());

        assertNotNull(result);
    }

    /**
     * Run the String getOption(String,String,Properties) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetOption_2()
            throws Exception {
        clearSystemProperties();
        System.setProperty("URIS-MODULE", "helloWorld-selector.xqy");
        String argVal = "";
        String propName = "URIS-MODULE";
        Properties props = new Properties();

        String result = getOption(argVal, propName, props);

        assertNotNull(result);
    }

    /**
     * Run the String getOption(String,String,Properties) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetOption_3()
            throws Exception {
        clearSystemProperties();
        String argVal = "URIS-MODULE";
        String propName = "";
        Properties props = new Properties();

        String result = getOption(argVal, propName, props);

        assertNotNull(result);
    }

    /**
     * Run the TransformOptions getOptions() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetOptions_1()
            throws Exception {
        clearSystemProperties();
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        TransformOptions result = executor.getOptions();

        assertNotNull(result);
    }

    /**
     * Run the Properties getProperties() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetProperties_1()
            throws Exception {
        clearSystemProperties();
        System.setProperty("OPTIONS-FILE", OPTIONS_FILE);
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        Properties result = executor.getProperties();

        assertNotNull(result);
    }

    /**
     * Run the String getProperty(String) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetProperty_1()
            throws Exception {
        clearSystemProperties();
        System.setProperty("systemProperty", "hellowWorld");
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        executor.contentSource = new ContentSourceBean();
        executor.options = new TransformOptions();
        String key = "systemProperty";

        String result = executor.getProperty(key);

        assertNotNull(result);
    }

    /**
     * Run the String getProperty(String) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetProperty_2()
            throws Exception {
        clearSystemProperties();
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        executor.contentSource = new ContentSourceBean();
        executor.options = new TransformOptions();
        String key = "PROCESS-TASK";

        String result = executor.getProperty(key);

        assertNotNull(result);
    }

    /**
     * Run the byte[] getValueAsBytes(XdmItem) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testGetValueAsBytes_1()
            throws Exception {
        clearSystemProperties();
        System.setProperty("EXPORT-FILE-NAME", "src/test/resources/testGetValueAsBytes_1.txt");
        System.setProperty("OPTIONS-FILE", OPTIONS_FILE);
        System.setProperty("PROCESS-MODULE", "src/test/resources/transform2.xqy|ADHOC");
        Properties props = getProperties();
        String[] args = {props.getProperty("XCC-CONNECTION-URI")};
        ModuleExecutor executor = new ModuleExecutor();
        executor.init(args);
        ResultSequence resSeq = run(executor);
        byte[] report = executor.getValueAsBytes(resSeq.next().getItem());

        assertNotNull(report);
    }

    /**
     * Run the void main(String[]) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testMain_1()
            throws Exception {
        clearSystemProperties();
        System.setProperty("OPTIONS-FILE", OPTIONS_FILE);
        System.setProperty("PROCESS-MODULE", PROCESS_MODULE);
        System.setProperty("EXPORT-FILE-NAME", EXPORT_FILE_NAME);
        String[] args = new String[]{};

        ModuleExecutor executor = new ModuleExecutor();
        executor.init(args);
        executor.run();

        File report = new File(EXPORT_FILE_NAME);
        boolean fileExists = report.exists();
        clearFile(report);
        assertTrue(fileExists);
    }

    /**
     * Run the SecurityOptions newTrustAnyoneOptions() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testNewTrustAnyoneOptions_1()
            throws Exception {

        SecurityOptions result = new TrustAnyoneSSLConfig().getSecurityOptions();

        // add additional test code here
        assertNotNull(result);
        assertNull(result.getEnabledProtocols());
        assertNull(result.getEnabledCipherSuites());
    }

    /**
     * Run the void prepareContentSource() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testPrepareContentSource() throws Exception {
        clearSystemProperties();
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();

        executor.prepareContentSource();
        assertNotNull(executor.contentSource);

    }

    /**
     * Run the void run() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testRun_1()
            throws Exception {
        clearSystemProperties();
        System.setProperty("OPTIONS-FILE", OPTIONS_FILE);
        System.setProperty("PROCESS-MODULE", PROCESS_MODULE);
        System.setProperty("EXPORT-FILE-NAME", EXPORT_FILE_NAME);
        String[] args = {};
        ModuleExecutor executor = new ModuleExecutor();
        executor.init(args);
        executor.run();

        String reportPath = executor.getProperty("EXPORT-FILE-NAME");
        File report = new File(reportPath);
        boolean fileExists = report.exists();
        clearFile(report);
        assertTrue(fileExists);

    }

    /**
     * Run the void run() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testRun_2()
            throws Exception {
        clearSystemProperties();
        String[] args = {
            XCC_CONNECTION_URI,
            PROCESS_MODULE,
            "",
            "",
            "",
            EXPORT_FILE_NAME
        };
        ModuleExecutor executor = new ModuleExecutor();
        executor.init(args);
        executor.run();

        String reportPath = executor.getProperty("EXPORT-FILE-NAME");
        File report = new File(reportPath);
        boolean fileExists = report.exists();
        clearFile(report);
        assertTrue(fileExists);
    }

    /**
     * Run the void run() method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testRun_3()
            throws Exception {
        clearSystemProperties();
        String[] args = {};
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("PROCESS-MODULE", PROCESS_MODULE);
        System.setProperty("DECRYPTER", "com.marklogic.developer.corb.JasyptDecrypter");
        System.setProperty("JASYPT-PROPERTIES-FILE", "src/test/resources/jasypt.properties");
        System.setProperty("EXPORT-FILE-NAME", EXPORT_FILE_NAME);

        ModuleExecutor executor = new ModuleExecutor();
        executor.init(args);
        executor.run();

        String reportPath = executor.getProperty("EXPORT-FILE-NAME");
        File report = new File(reportPath);
        boolean fileExists = report.exists();
        clearFile(report);
        assertTrue(fileExists);
    }

    /**
     * Run the void setProperties(Properties) method test.
     *
     * @throws Exception
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Test
    public void testSetProperties_1()
            throws Exception {
        TestUtils.clearSystemProperties();
        System.setProperty("OPTIONS-FILE", OPTIONS_FILE);
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        Properties props = executor.getProperties();

        assertNotNull(props);
        assertFalse(props.isEmpty());
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    /**
     * Perform pre-test initialization.
     *
     * @throws Exception if the initialization fails for some reason
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @Before
    public void setUp()
            throws Exception {
        clearSystemProperties();
        Logger logger = Logger.getLogger(ModuleExecutor.class.getName());
        logger.addHandler(testLogger);
    }

    /**
     * Perform post-test clean-up.
     *
     * @throws Exception if the clean-up fails for some reason
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    @After
    public void tearDown()
            throws Exception {
        // Add additional tear down code here
        clearSystemProperties();
    }

    /**
     * Launch the test.
     *
     * @param args the command line arguments
     *
     * @generatedBy CodePro at 9/18/15 12:45 PM
     */
    public static void main(String[] args) {
        new org.junit.runner.JUnitCore().run(ModuleExecutorTest.class);
    }

    private Properties getProperties() {

        String propFileLocation = System.getProperty("OPTIONS-FILE");
        if (propFileLocation == null || propFileLocation.length() == 0) {
            propFileLocation = OPTIONS_FILE;
        }
        File propFile = new File(propFileLocation);
        URL url = null;
        try {
            url = propFile.toURI().toURL();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return loadProperties(url);
    }

    private ModuleExecutor buildModuleExecutorAndLoadProperties() throws Exception {
        ModuleExecutor executor = null;
        Properties props = getProperties();
        executor = new ModuleExecutor();
        executor.init(new String[0], props);
        return executor;

    }

    private ResultSequence run(ModuleExecutor executor) throws Exception {

        executor.prepareContentSource();
        Session session = null;
        ResultSequence res = null;
        try {
            RequestOptions opts = new RequestOptions();
            opts.setCacheResult(false);
            session = executor.contentSource.newSession();
            Request req = null;
            TransformOptions options = executor.getOptions();
            Properties properties = executor.getProperties();

            List<String> propertyNames = new ArrayList<String>(
                    properties.stringPropertyNames());
            propertyNames.addAll(System.getProperties().stringPropertyNames());

            String queryPath = options.getProcessModule().substring(0, options.getProcessModule().indexOf('|'));

            String adhocQuery = AbstractManager.getAdhocQuery(queryPath);
            if (adhocQuery == null || (adhocQuery.length() == 0)) {
                throw new IllegalStateException(
                        "Unable to read adhoc query " + queryPath
                        + " from classpath or filesystem");
            }
            req = session.newAdhocQuery(adhocQuery);
            for (String propName : propertyNames) {
                if (propName.startsWith("PROCESS-MODULE.")) {
                    String varName = propName.substring("PROCESS-MODULE.".length());
                    String value = properties.getProperty(propName);
                    if (value != null) {
                        req.setNewStringVariable(varName, value);
                    }
                }
            }
            req.setOptions(opts);
            res = session.submitRequest(req);

        } catch (RequestException exc) {
            throw new CorbException("While invoking XQuery Module", exc);
        } catch (Exception exd) {
            throw new CorbException("While invoking XCC...", exd);
        }
        return res;
    }

    /**
     * Test of main method, of class ModuleExecutor.
     */
    @Test
    public void testMain_nullArgs() throws Exception {
        System.out.println("main");
        String[] args = null;
        exit.expectSystemExit();
        ModuleExecutor.main(args);
    }

    @Test
    public void testMain_emptyArgs() throws Exception {
        System.out.println("main");
        String[] args = new String[]{};
        exit.expectSystemExit();
        ModuleExecutor.main(args);
    }

    /**
     * Test of init method, of class ModuleExecutor.
     */
    @Test
    public void testInit_StringArr_nullProperties() throws Exception {
        System.out.println("init");
        String[] args = null;
        Properties props = null;
        ModuleExecutor instance = new ModuleExecutor();
        exit.expectSystemExit();
        instance.init(args, props);
    }

    @Test
    public void testInit_StringArr_emptyProperties() throws Exception {
        System.out.println("init");
        String[] args = null;
        Properties props = new Properties();
        ModuleExecutor instance = new ModuleExecutor();
        exit.expectSystemExit();
        instance.init(args, props);
    }

    /**
     * Test of initOptions method, of class ModuleExecutor.
     */
    @Test(expected = NullPointerException.class)
    public void testInitOptions_missingPROCESS_MODULE() throws Exception {
        System.out.println("initOptions");
        String[] args = new String[]{};
        ModuleExecutor instance = new ModuleExecutor();
        instance.initOptions(args);
    }

    @Test
    public void testInitOptions() throws Exception {
        System.out.println("initOptions");
        String exportDir = TestUtils.createTempDirectory().toString();
        String[] args = new String[]{"foo", "processModule", "", "", exportDir};
        ModuleExecutor instance = new ModuleExecutor();
        instance.initOptions(args);
        assertEquals(exportDir, instance.properties.getProperty("EXPORT-FILE-DIR"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitOptions_exportDirDoesNotExist() throws Exception {
        System.out.println("initOptions");
        String exportDir = "does/not/exist";
        String[] args = new String[]{"foo", "processModule", "", "", exportDir};
        ModuleExecutor instance = new ModuleExecutor();
        instance.initOptions(args);
    }

    /**
     * Test of usage method, of class ModuleExecutor.
     */
    @Test
    public void testUsage() {
        System.out.println("usage");
        ModuleExecutor instance = new ModuleExecutor();
        instance.usage();
    }

    /**
     * Test of run method, of class ModuleExecutor.
     */
    @Test(expected = NullPointerException.class)
    public void testRun_nullContentSource() throws Exception {
        System.out.println("run");
        ModuleExecutor instance = new ModuleExecutor();
        instance.run();
    }

    /**
     * Test of getProperty method, of class ModuleExecutor.
     */
    @Test
    public void testGetProperty() {
        System.out.println("getProperty");
        String key = "foo";
        ModuleExecutor instance = new ModuleExecutor();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    /**
     * Test of getValueAsBytes method, of class ModuleExecutor.
     */
    @Test
    public void testGetValueAsBytes_withXdmBinary() {
        System.out.println("getValueAsBytes");
        XdmBinary item = mock(XdmBinary.class);
        byte[] expected = {'a', 'b', 'c'};
        when(item.asBinaryData()).thenReturn(expected);
        ModuleExecutor instance = new ModuleExecutor();
        byte[] result = instance.getValueAsBytes(item);
        Assert.assertArrayEquals(expected, result);
    }

    @Test
    public void testGetValueAsBytes() {
        System.out.println("getValueAsBytes");
        XdmItem item = mock(XdmItem.class);
        String expected = "foo";
        when(item.asString()).thenReturn(expected);
        ModuleExecutor instance = new ModuleExecutor();
        byte[] result = instance.getValueAsBytes(item);
        assertArrayEquals(expected.getBytes(), result);
    }
    
    @Test
    public void testGetValueAsBytes_null() {
        System.out.println("getValueAsBytes");
        ModuleExecutor instance = new ModuleExecutor();
        byte[] result = instance.getValueAsBytes(null);
        assertArrayEquals(new byte[]{}, result);
    }
}
