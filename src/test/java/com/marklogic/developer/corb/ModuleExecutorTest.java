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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmItem;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The class <code>ModuleExecutorTest</code> contains tests for the class
 * <code>{@link ModuleExecutor}</code>.
 *
 * @author matthew.heckel
 */
public class ModuleExecutorTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private final TestHandler testLogger = new TestHandler();
    private static final Logger LOG = Logger.getLogger(ModuleExecutor.class.getName());
    public static final String XCC_CONNECTION_URI = "xcc://marklogic-corb-admin:marklogic-corb-admin-password@localhost:8223/marklogic-corb-content";
    public static final String OPTIONS_FILE = "src/test/resources/helloWorld.properties";
    public static final String EXPORT_FILE_NAME = "src/test/resources/helloWorld.txt";
    public static final String PROCESS_MODULE = "src/test/resources/transform2.xqy|ADHOC";
    private static final String FOO = "foo";
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
        // Add additional tear down code here
        clearSystemProperties();
        System.setOut(systemOut);
        System.setErr(systemErr);
    }

    @Test
    public void testModuleExecutor1() {
        clearSystemProperties();
        ModuleExecutor result = new ModuleExecutor();
        assertNotNull(result);
    }

    private Properties loadProperties(URL filePath) throws IOException {
        Properties prop = new Properties();
        try (InputStream input = filePath.openStream();) {
            // load a properties file
            prop.load(input);
        }
        return prop;
    }

    @Test
    public void testGetContentSource1() {
        clearSystemProperties();
        System.setProperty(Options.OPTIONS_FILE, OPTIONS_FILE);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
        ContentSource result = executor.getContentSource();

        assertNotNull(result);
    }

    @Test
    public void testLogOptions() {
        try {
            ModuleExecutor instance = getMockModuleExecutorWithEmptyResults();
            instance.logOptions();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(3, records.size());
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
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

    @Test
    public void testGetOption1() {
        clearSystemProperties();
        String argVal = "";
        String propName = Options.URIS_MODULE;
        ModuleExecutor executor;
        try {
            executor = this.buildModuleExecutorAndLoadProperties();
            String result = getOption(argVal, propName, executor.getProperties());
            assertNotNull(result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetOption2() {
        clearSystemProperties();
        System.setProperty(Options.URIS_MODULE, "helloWorld-selector.xqy");
        String argVal = "";
        String propName = Options.URIS_MODULE;
        Properties props = new Properties();

        String result = getOption(argVal, propName, props);

        assertNotNull(result);
    }

    @Test
    public void testGetOption3() {
        clearSystemProperties();
        String argVal = Options.URIS_MODULE;
        String propName = "";
        Properties props = new Properties();

        String result = getOption(argVal, propName, props);

        assertNotNull(result);
    }

    @Test
    public void testGetOptions1() {
        try {
            clearSystemProperties();
            ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
            TransformOptions result = executor.getOptions();

            assertNotNull(result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetProperties1() {
        try {
            clearSystemProperties();
            System.setProperty(Options.OPTIONS_FILE, OPTIONS_FILE);
            ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
            Properties result = executor.getProperties();

            assertNotNull(result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetProperty1() {
        try {
            clearSystemProperties();
            String key = "systemProperty";
            System.setProperty(key, "hellowWorld");
            ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
            executor.contentSource = new ContentSourceBean();
            executor.options = new TransformOptions();
            String result = executor.getProperty(key);

            assertNotNull(result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetProperty2() {
        try {
            clearSystemProperties();
            ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
            executor.contentSource = new ContentSourceBean();
            executor.options = new TransformOptions();
            String key = Options.PROCESS_TASK;

            String result = executor.getProperty(key);

            assertNotNull(result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGetValueAsBytes1() {
        clearSystemProperties();
        System.setProperty(Options.EXPORT_FILE_NAME, "src/test/resources/testGetValueAsBytes_1.txt");
        System.setProperty(Options.OPTIONS_FILE, OPTIONS_FILE);
        System.setProperty(Options.PROCESS_MODULE, "src/test/resources/transform2.xqy|ADHOC");
        Properties props = getProperties();
        String[] args = {props.getProperty(Options.XCC_CONNECTION_URI)};

        try {
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            ResultSequence resSeq = run(executor);
            byte[] report = AbstractTask.getValueAsBytes(resSeq.next().getItem());

            assertNotNull(report);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testMain1() {

        clearSystemProperties();
        System.setProperty(Options.OPTIONS_FILE, OPTIONS_FILE);
        System.setProperty(Options.PROCESS_MODULE, PROCESS_MODULE);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        String[] args = new String[]{};
        try {
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            executor.run();

            File report = new File(EXPORT_FILE_NAME);
            boolean fileExists = report.exists();
            clearFile(report);
            assertTrue(fileExists);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testNewTrustAnyoneOptions1() {
        try {
            SecurityOptions result = new TrustAnyoneSSLConfig().getSecurityOptions();

            // add additional test code here
            assertNotNull(result);
            assertNull(result.getEnabledProtocols());
            assertNull(result.getEnabledCipherSuites());
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testPrepareContentSource() {
        clearSystemProperties();
        try {
            ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
            executor.prepareContentSource();
            assertNotNull(executor.contentSource);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = CorbException.class)
    public void testProcessResult() throws CorbException {
        clearSystemProperties();
        ModuleExecutor executor = new MockModuleExecutorCannotWrite();
        ResultSequence seq = null;
        executor.processResult(seq);
    }

    @Test
    public void testRun1() {
        clearSystemProperties();
        System.setProperty(Options.OPTIONS_FILE, OPTIONS_FILE);
        System.setProperty(Options.PROCESS_MODULE, PROCESS_MODULE);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        String[] args = {};
        try {
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            executor.run();

            String reportPath = executor.getProperty(Options.EXPORT_FILE_NAME);
            File report = new File(reportPath);
            boolean fileExists = report.exists();
            clearFile(report);
            assertTrue(fileExists);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testRun2() {
        clearSystemProperties();
        String[] args = {
            XCC_CONNECTION_URI,
            PROCESS_MODULE,
            "",
            "",
            "",
            EXPORT_FILE_NAME
        };
        try {
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            executor.run();

            String reportPath = executor.getProperty(Options.EXPORT_FILE_NAME);
            File report = new File(reportPath);
            boolean fileExists = report.exists();
            clearFile(report);
            assertTrue(fileExists);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testRun3() {
        clearSystemProperties();
        String[] args = {};
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, PROCESS_MODULE);
        System.setProperty(Options.DECRYPTER, "com.marklogic.developer.corb.JasyptDecrypter");
        System.setProperty(Options.JASYPT_PROPERTIES_FILE, "src/test/resources/jasypt.properties");
        System.setProperty("PROCESS-MODULE.foo", "bar");
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        try {
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            executor.run();

            String reportPath = executor.getProperty(Options.EXPORT_FILE_NAME);
            File report = new File(reportPath);
            boolean fileExists = report.exists();
            clearFile(report);
            assertTrue(fileExists);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testRunInlineEmpty() {
        clearSystemProperties();
        String[] args = {};
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, "INLINE-XQUERY|");
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        try {
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            executor.run();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            if (ex instanceof IllegalStateException) {
                throw (IllegalStateException) ex;
            }
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testRunAdhocIsEmpty() {
        try {
            File emptyModule = File.createTempFile("emptyModule", "txt");
            emptyModule.createNewFile();
            emptyModule.deleteOnExit();
            clearSystemProperties();
            String[] args = {};
            System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
            System.setProperty(Options.PROCESS_MODULE, emptyModule.getAbsolutePath() + "|ADHOC");
            System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
            ModuleExecutor executor = getMockModuleExecutorWithEmptyResults();
            executor.init(args);
            executor.run();

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            if (ex instanceof IllegalStateException) {
                throw (IllegalStateException) ex;
            }
        }
        fail();
    }

    @Test
    public void testSetProperties1() {
        try {
            clearSystemProperties();
            System.setProperty(Options.OPTIONS_FILE, OPTIONS_FILE);
            ModuleExecutor executor = this.buildModuleExecutorAndLoadProperties();
            Properties props = executor.getProperties();

            assertNotNull(props);
            assertFalse(props.isEmpty());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    private Properties getProperties() {
        Properties properties = null;
        String propFileLocation = System.getProperty(Options.OPTIONS_FILE);
        if (propFileLocation == null || propFileLocation.length() == 0) {
            propFileLocation = OPTIONS_FILE;
        }
        File propFile = new File(propFileLocation);
        try {
            URL url = propFile.toURI().toURL();
            properties = loadProperties(url);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        return properties;
    }

    private ModuleExecutor buildModuleExecutorAndLoadProperties() {
        ModuleExecutor executor = null;
        Properties props = getProperties();
        try {
            executor = getMockModuleExecutorWithEmptyResults();
            executor.init(new String[0], props);
            return executor;
        } catch (RequestException | IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        return executor;
    }

    private ResultSequence run(ModuleExecutor executor) throws CorbException {
        ResultSequence res = null;
        try {
            executor.prepareContentSource();

            RequestOptions opts = new RequestOptions();
            opts.setCacheResult(false);
            Session session = executor.contentSource.newSession();
            Request req = null;
            TransformOptions options = executor.getOptions();
            Properties properties = executor.getProperties();

            List<String> propertyNames = new ArrayList<>(
                    properties.stringPropertyNames());
            propertyNames.addAll(System.getProperties().stringPropertyNames());

            String queryPath = options.getProcessModule().substring(0, options.getProcessModule().indexOf('|'));

            String adhocQuery = AbstractManager.getAdhocQuery(queryPath);
            if (StringUtils.isEmpty(adhocQuery)) {
                throw new IllegalStateException(
                        "Unable to read adhoc query " + queryPath
                        + " from classpath or filesystem");
            }
            req = session.newAdhocQuery(adhocQuery);
            for (String propName : propertyNames) {
                if (propName.startsWith(Options.PROCESS_MODULE + ".")) {
                    String varName = propName.substring((Options.PROCESS_MODULE + ".").length());
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
        } catch (XccConfigException | GeneralSecurityException | IllegalStateException exd) {
            throw new CorbException("While invoking XCC...", exd);
        }
        return res;
    }

    @Test
    public void testMainNullArgs() {
        String[] args = null;
        exit.expectSystemExit();
        ModuleExecutor.main(args);
        fail();
    }

    @Test
    public void testMainEmptyArgs() {
        String[] args = new String[]{};
        exit.expectSystemExit();
        ModuleExecutor.main(args);
        fail();
    }

    @Test(expected = InstantiationException.class)
    public void testInitStringArrNullProperties() throws InstantiationException {
        String[] args = null;
        Properties props = null;
        ModuleExecutor instance = new ModuleExecutor();
        try {
            instance.init(args, props);
        } catch (IOException | URISyntaxException | ClassNotFoundException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = InstantiationException.class)
    public void testInitStringArrEmptyProperties() throws InstantiationException {
        String[] args = null;
        Properties props = new Properties();
        ModuleExecutor instance = new ModuleExecutor();
        try {
            instance.init(args, props);
        } catch (IOException | URISyntaxException | ClassNotFoundException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptionsMissingPROCESSMODULE() {
        String[] args = new String[]{};
        ModuleExecutor instance = new ModuleExecutor();
        instance.initOptions(args);
        fail();
    }

    @Test
    public void testInitOptions() {
        try {
            String exportDir = TestUtils.createTempDirectory().toString();
            String[] args = new String[]{FOO, "processModule", "", "", exportDir};
            ModuleExecutor instance = new ModuleExecutor();
            instance.initOptions(args);
            assertEquals(exportDir, instance.properties.getProperty(Options.EXPORT_FILE_DIR));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitOptionsExportDirDoesNotExist() {
        String exportDir = "does/not/exist";
        String[] args = new String[]{FOO, "processModule", "", "", exportDir};
        ModuleExecutor instance = new ModuleExecutor();
        instance.initOptions(args);
        fail();
    }

    @Test
    public void testUsage() {
        AbstractManager aManager = new AbstractManagerTest.AbstractManagerImpl();
        aManager.usage();
        String aManagerUsage = outContent.toString();
        outContent.reset();

        ModuleExecutor instance = new ModuleExecutor();
        instance.usage();

        assertNotNull(errContent.toString());
        assertTrue(outContent.toString().contains(aManagerUsage));
    }

    @Test(expected = NullPointerException.class)
    public void testRunNullContentSource() {
        ModuleExecutor instance = new ModuleExecutor();
        try {
            instance.run();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            if (ex instanceof NullPointerException) {
                throw (NullPointerException) ex;
            }
        }
        fail();
    }

    @Test
    public void testGetProperty() {
        String key = "getPropertyKeyDoesNotExist";
        ModuleExecutor instance = new ModuleExecutor();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    @Test
    public void testCustomProcessResults() {
        try {
            String[] args = {"xcc://user:pass@localhost:8000", "module.xqy"};
            ModuleExecutor instance = getMockModuleExecutorCustomProcessResults();
            instance.init(args);
            instance.run();
            MockModuleExecutorResults moduleExecutor = (MockModuleExecutorResults) instance;
            assertFalse(moduleExecutor.results.isEmpty());
            assertTrue(moduleExecutor.results.size() == 2);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    public static ModuleExecutor getMockModuleExecutorCustomProcessResults() throws RequestException {
        MockModuleExecutorResults manager = new MockModuleExecutorResults();
        manager.contentSource = getMockContentSource();
        return manager;
    }

    public static ModuleExecutor getMockModuleExecutorWithEmptyResults() throws RequestException {
        ModuleExecutor manager = new MockModuleExecutor();
        manager.contentSource = getMockContentSource();
        return manager;
    }

    public static ContentSource getMockContentSource() throws RequestException {
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke moduleInvoke = mock(ModuleInvoke.class);
        AdhocQuery adhocQuery = mock(AdhocQuery.class);

        ResultSequence res = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        ResultItem uriCountResult = mock(ResultItem.class);
        XdmItem batchRefItem = mock(XdmItem.class);
        XdmItem exampleValue = mock(XdmItem.class);
        XdmItem uriCount = mock(XdmItem.class);

        when(contentSource.newSession()).thenReturn(session);
        when(contentSource.newSession((String) any())).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(moduleInvoke).thenReturn(moduleInvoke);
        when(session.newAdhocQuery(anyString())).thenReturn(adhocQuery);

        when(session.submitRequest((Request) any())).thenReturn(res);
        //First, return false when registerInfo() is calling.
        when(res.hasNext()).thenReturn(Boolean.FALSE).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);
        when(res.next()).thenReturn(resultItem).thenReturn(uriCountResult).thenReturn(resultItem).thenReturn(null);

        when(resultItem.getItem()).thenReturn(batchRefItem).thenReturn(exampleValue);
        when(uriCountResult.getItem()).thenReturn(uriCount);

        when(batchRefItem.asString()).thenReturn("batchRefVal");
        when(exampleValue.asString()).thenReturn(FOO);
        when(uriCount.asString()).thenReturn("1");

        return contentSource;
    }

    private static class MockModuleExecutor extends ModuleExecutor {

        //Want to retain the mock contentSource that we set in our tests
        @Override
        protected void prepareContentSource() throws XccConfigException, GeneralSecurityException {
        }
    }

    private static class MockModuleExecutorCannotWrite extends ModuleExecutor {

        @Override
        protected void writeToFile(ResultSequence seq) throws IOException {
            super.writeToFile(seq);
            throw new IOException("Throwing exception in order to test branch coverage");
        }
    }

    private static class MockModuleExecutorResults extends MockModuleExecutor {

        public List<String> results = new ArrayList();

        @Override
        protected void writeToFile(ResultSequence res) {
            while (res.hasNext()) {
                results.add(res.next().getItem().asString());
            }
        }
    }

}
