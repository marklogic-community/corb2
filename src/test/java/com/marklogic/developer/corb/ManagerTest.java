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
import org.junit.*;
import static org.junit.Assert.*;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static com.marklogic.developer.corb.TestUtils.containsLogRecord;
import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmItem;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.exceptions.base.MockitoException;

/**
 * The class <code>ManagerTest</code> contains tests for the class
 * <code>{@link Manager}</code>.
 *
 * @author matthew.heckel
 */
public class ManagerTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private final TestHandler testLogger = new TestHandler();
    private static final Logger LOG = Logger.getLogger(Manager.class.getName());
    private PrintStream systemErr = System.err;
    public static final String XCC_CONNECTION_URI = "xcc://admin:admin@localhost:2223/FFE";
    public static final String COLLECTION_NAME = "StringPassedToTheURIsModule";
    public static final String XQUERY_MODULE = "src/test/resources/transform.xqy|ADHOC";
    public static final String THREAD_COUNT = "2";
    public static final String PROCESS_TASK = "com.marklogic.developer.corb.ExportBatchToFileTask";
    public static final String MODULES_ROOT = "/";
    public static final String MODULES_DATABASE = "Documents";
    public static final String PRE_BATCH_MODULE = "src/test/resources/preBatchModule.xqy|ADHOC";
    public static final String PRE_BATCH_TASK = "com.marklogic.developer.corb.PreBatchUpdateFileTask";
    public static final String POST_BATCH_MODULE = "src/test/resources/postBatchModule.xqy|ADHOC";
    public static final String POST_BATCH_TASK = "com.marklogic.developer.corb.PostBatchUpdateFileTask";
    public static String EXPORT_FILE_DIR = null;
    public static final String EXPORT_FILE_NAME = "exportFile.out";
    public static final String URIS_FILE = "src/test/resources/uris-file.txt";
    public static final String XQUERY_MODULE_FOO = "process-bar";
    public static final String POST_BATCH_XQUERY_MODULE_FOO = "post-bar";
    public static final String PRE_BATCH_XQUERY_MODULE_FOO = "pre-bar";
    public static final String PROCESS_MODULE = "src/test/resources/transform2.xqy|ADHOC";

    /**
     * Perform pre-test initialization.
     *
     * @throws Exception if the initialization fails for some reason
     *
     * @generatedBy CodePro at 9/18/15 10:51 AM
     */
    @Before
    public void setUp()
            throws Exception {
        clearSystemProperties();
        LOG.addHandler(testLogger);
        File tempDir = TestUtils.createTempDirectory();
        EXPORT_FILE_DIR = tempDir.toString();
    }

    /**
     * Perform post-test clean-up.
     *
     * @throws Exception if the clean-up fails for some reason
     *
     * @generatedBy CodePro at 9/18/15 10:51 AM
     */
    @After
    public void tearDown() throws Exception {
        FileUtils.deleteFile(ManagerTest.EXPORT_FILE_DIR);
        clearSystemProperties();
        System.setErr(systemErr);
    }

    @Test(expected = NullPointerException.class)
    public void testRejectedExecution_npe() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
        fail();
    }

    @Test
    public void testRejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);
        @SuppressWarnings("unchecked")
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        when(threadPool.getQueue()).thenReturn(queue).thenReturn(null);

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        
        cbp.rejectedExecution(r, threadPool);
        assertNull(threadPool.getQueue());
    }

    @Test(expected = MockitoException.class)
    public void testRejectedExecution_rejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        when(threadPool.getQueue()).thenThrow(new InterruptedException());
        threadPool.getQueue();
        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
        fail();
    }

    @Test
    public void testRejectedExecution_warningIsTrueAndQueIsNotNull() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);
        @SuppressWarnings("unchecked")
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        when(threadPool.getQueue()).thenReturn(queue).thenThrow(new NullPointerException());

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
        cbp.rejectedExecution(r, threadPool);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("queue is full: size = {0} (will only appear once)", records.get(0).getMessage());
        assertEquals(1, records.size());
    }

    /**
     * Test of init method, of class Manager.
     */
    @Test (expected = InstantiationException.class)
    public void testInit_nullArgs_properties() throws Exception {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE,Integer.toString(5));
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
    }

    @Test
    public void testInit_blankCollection() throws Exception {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        //no "COLLECTION-NAME" specified
        props.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals("", instance.collection);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInit_urisFileDoesNoteExist() throws Exception {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        props.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");
        props.setProperty(Options.URIS_FILE, "does/not/exist");

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        fail();
    }

    @Test (expected = InstantiationException.class)
    public void testInit_nullArgs_emptyProperties() throws Exception {
        String[] args = null;
        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
    }

    /**
     * Test of initOptions method, of class Manager.
     */
    @Test(expected = NullPointerException.class)
    public void testInitOptions_nullArgs() throws Exception {
        String[] args = null;
        Manager instance = getMockManagerWithEmptyResults();
        instance.initOptions(args);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions() throws Exception {
        String[] args = {};
        Manager instance = new Manager();
        instance.initOptions(args);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions_withEmptyProperties() throws Exception {
        String[] args = null;
        Manager instance = getMockManagerWithEmptyResults();
        instance.properties = new Properties();
        instance.initOptions(args);
        fail();
    }

    @Test
    public void testInitOptions_urisFileIsBlank() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = "      ";

        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertNull(instance.options.getUrisFile());
    }

    @Test
    public void testInitOptions_urisFileIsNull() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = null;

        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertNull(instance.options.getUrisFile());
    }

    @Test
    public void testInitOptions_setXQUERY_MODULE_property() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null;

        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
    }

    @Test
    public void testInitOptions_setPROCESS_MODULE_property() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";//process-module

        Properties props = new Properties();
        props.setProperty(Options.PROCESS_MODULE, PROCESS_MODULE);
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
    }

    @Test
    public void testInitOptions_setInstall_property_true() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = Boolean.toString(true);//install
        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertTrue(instance.options.isDoInstall());
    }

    @Test
    public void testInitOptions_setInstall_property_one() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = "1";//install
        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertTrue(instance.options.isDoInstall());
    }

    @Test
    public void testInitOptions_setInstall_property_maybe() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = "maybe";//install
        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertFalse(instance.options.isDoInstall());
    }

    @Test
    public void testInitOptions_setDISK_QUEUE_MAX_IN_MEMORY_SIZE_property() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();

        Properties props = new Properties();
        props.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, "10");
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(10, instance.options.getDiskQueueMaxInMemorySize());
    }

    @Test(expected = NumberFormatException.class)
    public void testInitOptions_setDISK_QUEUE_MAX_IN_MEMORY_SIZE_property_NaN() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();

        Properties props = new Properties();
        props.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, "ten");
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        fail();
    }

    @Test
    public void testInitOptions_missingPROCESS_MODULE() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";

        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertNull(instance.options.getProcessModule());
    }

    @Test
    public void testInitOptions_normalizeLegacySystemProperties() throws Exception {
        clearSystemProperties();
        System.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        System.setProperty("XQUERY-MODULE.foo", XQUERY_MODULE_FOO);
        System.setProperty(Options.PRE_BATCH_XQUERY_MODULE, PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-XQUERY-MODULE.foo", PRE_BATCH_XQUERY_MODULE_FOO);
        System.setProperty(Options.POST_BATCH_XQUERY_MODULE, POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-XQUERY-MODULE.foo", POST_BATCH_XQUERY_MODULE_FOO);

        String[] args = getDefaultArgs();
        args[2] = null; //process-module
        args[11] = null; //pre-batch-module
        args[13] = null; //post-batch-module

        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);

        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        assertEquals(XQUERY_MODULE_FOO, System.getProperty("PROCESS-MODULE.foo"));
        assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
        assertEquals(PRE_BATCH_XQUERY_MODULE_FOO, System.getProperty("PRE-BATCH-MODULE.foo"));
        assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
        assertEquals(POST_BATCH_XQUERY_MODULE_FOO, System.getProperty("POST-BATCH-MODULE.foo"));
    }

    @Test
    public void testInitOptions_normalizeLegacyProperties() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null; //process-module
        args[11] = null; //pre-batch-module
        args[13] = null; //post-batch-module
        
        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        props.setProperty("XQUERY-MODULE.foo", XQUERY_MODULE_FOO);
        props.setProperty(Options.PRE_BATCH_XQUERY_MODULE, PRE_BATCH_MODULE);
        props.setProperty("PRE-BATCH-XQUERY-MODULE.foo", PRE_BATCH_XQUERY_MODULE_FOO);
        props.setProperty(Options.POST_BATCH_XQUERY_MODULE, POST_BATCH_MODULE);
        props.setProperty("POST-BATCH-XQUERY-MODULE.foo", POST_BATCH_XQUERY_MODULE_FOO);

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        assertEquals(XQUERY_MODULE_FOO, instance.properties.getProperty("PROCESS-MODULE.foo"));
        assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
        assertEquals(PRE_BATCH_XQUERY_MODULE_FOO, instance.properties.getProperty("PRE-BATCH-MODULE.foo"));
        assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
        assertEquals(POST_BATCH_XQUERY_MODULE_FOO, instance.properties.getProperty("POST-BATCH-MODULE.foo"));
    }

    @Test(expected = NumberFormatException.class)
    public void testInitOptions_batchSize_parseError() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, "one");
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        fail();
    }

    @Test
    public void testInitOptions_batchSize() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, Integer.toString(5));
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(5, instance.options.getBatchSize());
    }

    @Test
    public void testInit_failOnError_falseCaseInsensitive() throws Exception {
        clearSystemProperties();
        System.setProperty(Options.FAIL_ON_ERROR, "False");
        String[] args = getDefaultArgs();
        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertFalse(instance.options.isFailOnError());
    }

    @Test
    public void testInit_failOnErrorInvalidValue() throws Exception {
        clearSystemProperties();
        System.setProperty(Options.FAIL_ON_ERROR, "No");
        String[] args = getDefaultArgs();
        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertTrue(instance.options.isFailOnError());
    }

    @Test
    public void testInitOptions_ensurePropertiesAreSet() throws Exception {
        clearSystemProperties();
        System.setProperty(Options.ERROR_FILE_NAME, EXPORT_FILE_DIR + "/out");
        System.setProperty(Options.EXPORT_FILE_PART_EXT, "pt");
        String[] args = getDefaultArgs();
        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(1, instance.options.getBatchSize());
        assertEquals(EXPORT_FILE_DIR, instance.properties.getProperty("EXPORT-FILE-DIR"));
        assertEquals(EXPORT_FILE_NAME, instance.properties.getProperty("EXPORT-FILE-NAME"));
        assertEquals(EXPORT_FILE_DIR + "/out", instance.properties.getProperty("ERROR-FILE-NAME"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitOptions_exportDirNotExists() throws Exception {
        clearSystemProperties();

        String[] args = getDefaultArgs();
        args[13] = "/does/not/exist";

        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        fail();
    }

    @Test
    public void testInitOptions_exportFileAndErrorFileExists() throws Exception {
        clearSystemProperties();
        String errorFilename = "error.txt";
        File errorFile = new File(EXPORT_FILE_DIR, errorFilename);
        errorFile.createNewFile();
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
        exportFile.createNewFile();
        String[] args = getDefaultArgs();

        Properties props = new Properties();
        props.setProperty(Options.ERROR_FILE_NAME, errorFilename);
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertFalse(errorFile.exists());
        assertFalse(exportFile.exists());
    }

    @Test
    public void testInitOptions_clearExportFilePartExt() throws Exception {
        clearSystemProperties();

        System.setProperty(Options.EXPORT_FILE_PART_EXT, "exp");
        String[] args = getDefaultArgs();
        args[12] = null;
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "expt");
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertNull(instance.properties.getProperty(Options.EXPORT_FILE_PART_EXT));
        assertNull(System.getProperty(Options.EXPORT_FILE_PART_EXT));
    }

    @Test
    public void testInitOptions_defaultOptions() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        //args[2] = null;
        args[3] = null;
        args[4] = null;
        args[5] = null;
        args[6] = null;
        args[7] = null;
        args[8] = null;
        args[9] = null;
        args[10] = null;
        args[11] = null;
        args[12] = null;
        args[13] = null;
        args[14] = null;
        args[15] = null;
        Properties props = new Properties();

        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);

        assertNull(instance.properties.getProperty(Options.PROCESS_TASK));
        assertFalse(instance.options.isDoInstall());
        assertNull(instance.options.getUrisModule());
        assertEquals("Modules", instance.options.getModulesDatabase());
        assertNull(instance.options.getUrisFile());
        assertEquals(1, instance.options.getBatchSize());
        assertTrue(instance.options.isFailOnError());
        assertNull(instance.properties.getProperty(Options.EXPORT_FILE_DIR));
        assertNull(instance.properties.getProperty(Options.EXPORT_FILE_NAME));
        assertNull(instance.properties.getProperty(Options.ERROR_FILE_NAME));
        assertNull(instance.options.getInitModule());
        assertNull(instance.options.getInitTaskClass());
        assertNull(instance.options.getPreBatchModule());
        assertNull(instance.options.getPreBatchTaskClass());
        assertNull(instance.options.getPostBatchModule());
        assertNull(instance.options.getPostBatchTaskClass());
        assertNull(instance.properties.getProperty(Options.EXPORT_FILE_PART_EXT));
        assertNull(System.getProperty(Options.EXPORT_FILE_PART_EXT));
    }

    @Test
    public void testInitOptions_initModule() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.INIT_MODULE, "initModule");
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals("initModule", instance.options.getInitModule());
    }

    @Test
    public void testInitOptions_processTaskClass() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.INIT_TASK, PROCESS_TASK);
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(PROCESS_TASK, instance.options.getProcessTaskClass().getName());
    }

    @Test
    public void testInitOptions_customUrisLoader() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        String loader = FileUrisLoader.class.getName();
        Properties props = new Properties();
        props.setProperty(Options.URIS_LOADER, loader);
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        assertEquals(loader, instance.options.getUrisLoaderClass().getName());
    }

    @Test
    public void testInitOptions_InstallWithBlankModules() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/selector.xqy";
        args[6] = "";
        args[7] = "true";

        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.options.setModulesDatabase("");
        instance.init(args, props);
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, new LogRecord(Level.WARNING, "XCC configured for the filesystem: please install modules manually")));
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions_InstallWithMissingModule() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/doesNotExist.xqy";
        args[7] = Boolean.toString(true);
        Properties props = new Properties();
        Manager instance = getMockManagerWithEmptyResults();
        instance.init(args, props);
        fail();
    }

    @Test
    public void testNormalizeLegacyProperties_whenPropertiesIsNull() throws RequestException {
        Manager manager = getMockManagerWithEmptyResults();
        manager.properties = null;
        manager.normalizeLegacyProperties();
        assertNull(manager.properties);
    }

    @Test
    public void testNormalizeLegacyProperties() throws RequestException {
        String legacyValue1 = "legacyVal1";
        String legacyValue2 = "legacyVal2";
        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, legacyValue1);
        props.setProperty("XQUERY-MODULE.bar", legacyValue2);
        Manager manager = getMockManagerWithEmptyResults();
        manager.properties = props;
        manager.normalizeLegacyProperties();

        assertEquals(legacyValue1, manager.properties.getProperty(Options.PROCESS_MODULE));
        assertEquals(legacyValue2, manager.properties.getProperty("PROCESS-MODULE.bar"));
    }

    @Test
    public void testNormalizeLegacyProperties_precedenceChecks() throws RequestException {
        Properties props = new Properties();
        String processVal = "legacyValue";
        props.setProperty("PROCESS-MODULE.bar", processVal);
        props.setProperty("XQUERY-MODULE.bar", "asdf");
        Manager manager = getMockManagerWithEmptyResults();
        manager.properties = props;
        manager.normalizeLegacyProperties();

        assertEquals(processVal, manager.properties.getProperty("PROCESS-MODULE.bar"));
    }

    /**
     * Test of getTaskCls method, of class Manager.
     */
    @Test
    public void testGetTaskCls() throws Exception {
        String type = "";
        String className = Transform.class.getName();
        Manager instance = new Manager();
        Class<? extends Task> expResult = Transform.class;
        Class<? extends Task> result = instance.getTaskCls(type, className);
        assertEquals(expResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTaskCls_notTaskClass() throws Exception {
        String type = "";
        Manager instance = new Manager();
        Class<? extends Task> expResult = Transform.class;
        Class<? extends Task> result = instance.getTaskCls(type, String.class.getName());
        assertEquals(expResult, result);
        fail();
    }

    /**
     * Test of getUrisLoaderCls method, of class Manager.
     */
    @Test
    public void testGetUrisLoaderCls() throws Exception {
        String className = FileUrisLoader.class.getName();
        Manager instance = new Manager();
        Class<? extends UrisLoader> expResult = FileUrisLoader.class;
        Class<? extends UrisLoader> result = instance.getUrisLoaderCls(className);
        assertEquals(expResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUrisLoaderCls_notUrisClass() throws Exception {
        Manager instance = new Manager();
        Class<? extends UrisLoader> expResult = FileUrisLoader.class;
        Class<? extends UrisLoader> result = instance.getUrisLoaderCls(String.class.getName());
        assertEquals(expResult, result);
        fail();
    }

    @Test(expected = ClassNotFoundException.class)
    public void testGetUrisLoaderCls_badClassname() throws Exception {
        String className = "does.not.Exist";
        Manager instance = new Manager();
        instance.getUrisLoaderCls(className);
        fail();
    }

    /**
     * Test of usage method, of class Manager.
     */
    @Test
    public void testUsage() {
        Manager instance = new Manager();
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outContent));
        
        AbstractManager aManager = new AbstractManagerTest.AbstractManagerImpl();
        aManager.usage();
        String aManagerUsage = outContent.toString();
        outContent.reset();
        instance.usage();
        System.out.println(aManagerUsage);
        assertTrue(outContent.toString().contains(aManagerUsage));
    }

    /**
     * Test of run method, of class Manager.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRun_missingURIS_MODULE_FILE_AND_LOADER() throws Exception {
        Manager instance = new Manager();
        instance.run();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testRun_getURILoader_withURIS_MODULE_noContentSource() throws Exception {
        Manager instance = new Manager();
        instance.options.setUrisModule("someFile1.xqy");
        instance.run();
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRun_getURILoader_withURIS_MODULE_invalidCollection() throws Exception {
        Manager instance = new Manager();
        instance.contentSource = ContentSourceFactory.newContentSource(new URI(XCC_CONNECTION_URI));
        instance.options.setUrisModule("someFile2.xqy");
        instance.run();
        fail();
    }

    @Test
    public void testRun_getURILoader_withURIS_MODULE() throws Exception {
        Manager instance = getMockManagerWithEmptyResults();
        instance.collection = "URILoader_Modules";
        instance.options.setUrisModule("someFile3.xqy");
        int count = instance.run();
        assertEquals(0, count);
    }

    /**
     * Test of registerStatusInfo method, of class Manager.
     */
    @Test
    public void testRegisterStatusInfo() throws RequestException {
        String xccRootValue = "xccRootValue";

        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        AdhocQuery adhocQuery = mock(AdhocQuery.class);
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem first = mock(ResultItem.class);
        XdmItem firstXdmItem = mock(XdmItem.class);
        ResultItem second = mock(ResultItem.class);
        XdmItem secondXdmItem = mock(XdmItem.class);

        when(contentSource.newSession()).thenReturn(session);
        when(session.newAdhocQuery(anyString())).thenReturn(adhocQuery);
        when(resultSequence.hasNext()).thenReturn(true, true, false);
        when(firstXdmItem.asString()).thenReturn(Integer.toString(0));
        when(first.getItem()).thenReturn(firstXdmItem);
        when(first.getIndex()).thenReturn(0);
        when(secondXdmItem.asString()).thenReturn(xccRootValue);
        when(second.getItem()).thenReturn(secondXdmItem);
        when(second.getIndex()).thenReturn(1);
        when(resultSequence.next()).thenReturn(first, second);
        when(session.submitRequest(any(Request.class))).thenReturn(resultSequence);

        Manager instance = getMockManagerWithEmptyResults();
        instance.contentSource = contentSource;
        instance.registerStatusInfo();

        assertEquals(xccRootValue, instance.options.getXDBC_ROOT());
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(19, records.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterStatusInfo_nullContentSource() {
        Manager instance = new Manager();
        instance.registerStatusInfo();
        fail();
    }

    /**
     * Test of logProperties method, of class Manager.
     */
    @Test
    public void testLogProperties() throws RequestException {
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        Manager instance = getMockManagerWithEmptyResults();
        instance.properties = props;
        instance.logProperties();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(props.size(), records.size());
    }

    @Test
    public void testLogProperties_nullProperties() throws RequestException {
        Manager instance = getMockManagerWithEmptyResults();
        instance.logProperties();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(0, records.size());
    }

    /**
     * Test of stop method, of class Manager.
     */
    @Test
    public void testStop_0args() {
        Manager instance = new Manager();
        instance.stop();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("cleaning up", records.get(0).getMessage());
    }

    /**
     * Test of stop method, of class Manager.
     */
    @Test
    public void testStop_ExecutionException() {
        ExecutionException e = new ExecutionException("test", new Error());
        Manager instance = new Manager();
        instance.stop(e);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("fatal error", records.get(0).getMessage());
    }

    @Test
    public void testSetThreadCount() {
        Manager instance = new Manager();
        instance.setThreadCount(2);
        assertEquals(2, instance.options.getThreadCount());
    }

    @Test
    public void testSetThreadCountTwice() {
        Manager instance = new Manager();
        instance.setThreadCount(2);
        instance.setThreadCount(2);
        assertEquals(2, instance.options.getThreadCount());
    }

    @Test
    public void testSetThreadCount_invalidValue() {
        Manager instance = new Manager();
        instance.setThreadCount(-5);
        assertEquals(1, instance.options.getThreadCount());
        instance.setThreadCount(0);
        assertEquals(1, instance.options.getThreadCount());
    }

    public static String[] getDefaultArgs() {
        String[] args = {XCC_CONNECTION_URI,
            COLLECTION_NAME,
            XQUERY_MODULE,
            THREAD_COUNT,
            "src/test/resources/selector.xqy|ADHOC",
            MODULES_ROOT,
            MODULES_DATABASE,
            "false",
            PROCESS_TASK,
            PRE_BATCH_MODULE,
            PRE_BATCH_TASK,
            POST_BATCH_MODULE,
            POST_BATCH_TASK,
            EXPORT_FILE_DIR,
            EXPORT_FILE_NAME,
            URIS_FILE};
        return args;
    }

    public static Manager getMockManagerWithEmptyResults() throws RequestException {
        Manager manager = new MockManager();

        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke moduleInvoke = mock(ModuleInvoke.class);
        ResultSequence res = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        ResultItem uriCountResult = mock(ResultItem.class);
        XdmItem batchRefItem = mock(XdmItem.class);
        XdmItem uriCount = mock(XdmItem.class);

        when(contentSource.newSession()).thenReturn(session);
        when(contentSource.newSession((String) any())).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(moduleInvoke);
        when(session.submitRequest((Request) any())).thenReturn(res);
        when(res.next()).thenReturn(resultItem).thenReturn(uriCountResult).thenReturn(null);
        when(resultItem.getItem()).thenReturn(batchRefItem);
        when(uriCountResult.getItem()).thenReturn(uriCount);
        when(batchRefItem.asString()).thenReturn("batchRefVal");
        when(uriCount.asString()).thenReturn(Integer.toString(0));

        manager.contentSource = contentSource;
        return manager;
    }

    private static class MockManager extends Manager {
 
        @Override
        protected void prepareContentSource() throws XccConfigException, GeneralSecurityException {
            //Want to retain the mock contentSoure that we set in our tests
        }
    }
}
