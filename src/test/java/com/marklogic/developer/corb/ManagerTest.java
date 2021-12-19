/*
 * Copyright (c) 2004-2021 MarkLogic Corporation
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

import static com.marklogic.developer.corb.TestUtils.containsLogRecord;
import static org.mockito.Mockito.*;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.XdmItem;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.mockito.exceptions.base.MockitoException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * The class {@code ManagerTest} contains tests for the class
 * <code>{@link Manager}</code>.
 *
 * @author matthew.heckel
 */
public class ManagerTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private final TestHandler testLogger = new TestHandler();
    private static final Logger MANAGER_LOGGER = Logger.getLogger(Manager.class.getName());
    private static final Logger LOG = Logger.getLogger(ManagerTest.class.getName());
    private PrintStream systemErr = System.err;
    public static final String XCC_CONNECTION_URI = "xcc://marklogic-corb-admin:marklogic-corb-admin-password@localhost:8223/marklogic-corb-content";
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
    public static final String LOG_LEVEL_INFO = "info";
    public static String EXPORT_FILE_DIR = null;
    public static final String EXPORT_FILE_NAME = "exportFile.out";
    public static final String URIS_FILE = "src/test/resources/uris-file.txt";
    public static final String XQUERY_MODULE_FOO = "process-bar";
    public static final String POST_BATCH_XQUERY_MODULE_FOO = "post-bar";
    public static final String PRE_BATCH_XQUERY_MODULE_FOO = "pre-bar";
    public static final String PROCESS_MODULE = "src/test/resources/transform2.xqy|ADHOC";
    public static final String SLOW_RECEIVE_MESSAGE = "Slow receive! Consider increasing max heap size and using -XX:+UseConcMarkSweepGC";

    private void clearSystemProperties() {
    		TestUtils.clearSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "0");
        System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
    }

    @Before
    public void setUp() throws IOException {
        clearSystemProperties();
        MANAGER_LOGGER.addHandler(testLogger);
        File tempDir = TestUtils.createTempDirectory();
        EXPORT_FILE_DIR = tempDir.toString();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteFile(ManagerTest.EXPORT_FILE_DIR);
        clearSystemProperties();
        System.setErr(systemErr);
    }

    @Test
    public void testMainWithoutOptionsInitFailure() {
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_INIT_ERROR);
        Manager.main();
    }

    @Test(expected = NullPointerException.class)
    public void testRejectedExecutionNpe() {
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
    public void testRejectedExecutionRejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        when(threadPool.getQueue()).thenThrow(new InterruptedException());
        threadPool.getQueue();
        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
        fail();
    }

    @Test
    public void testRejectedExecutionWarningIsTrueAndQueIsNotNull() {
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
        assertEquals("queue is full: size = 0 (will only appear once)", records.get(0).getMessage());
        assertEquals(1, records.size());
    }

    @Test(expected = CorbException.class)
    public void testInitNullArgsProperties() throws CorbException {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, Integer.toString(5));
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testInitBlankCollection() {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        //no "COLLECTION-NAME" specified
        props.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals("", instance.collection);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitPropertiesEmptyProperties() {
        clearSystemProperties();
        Properties properties = new Properties();
        Manager manager = new Manager();
        try {
            manager.initProperties(properties);
            assertEquals(2, manager.properties.size());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void testInitPropertiesNullProperties() {
        clearSystemProperties();
        Properties properties = null;
        Manager manager = new Manager();
        try {
            manager.initProperties(properties);
            assertNotNull(manager.properties);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void testInitProperties() {
        clearSystemProperties();
        System.setProperty(Options.BATCH_SIZE, "3");
        System.setProperty(Options.COMMAND, "pause");

        Properties properties = new Properties();
        properties.setProperty(Options.COMMAND, "resume");
        Manager manager = new Manager();
        try {
            manager.initProperties(properties);
            assertNotNull(manager.properties);
            assertEquals("resume", manager.properties.getProperty(Options.COMMAND));
            assertEquals("3", manager.properties.getProperty(Options.BATCH_SIZE));
        } catch (CorbException ex) {
            fail();
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void testInitUrisFileDoesNoteExist() {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        props.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");
        props.setProperty(Options.URIS_FILE, "does/not/exist");

        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitNullArgsEmptyProperties() throws CorbException {
        String[] args = null;
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptionsNullArgs() {
        String[] args = null;
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.initOptions(args);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testInitOptions() throws CorbException {
        String[] args = {};
        Manager instance = new Manager();
        instance.initOptions(args);
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptionsWithEmptyProperties() {
        String[] args = null;
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.properties = new Properties();
            instance.initOptions(args);

        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testInitOptionsUrisFileIsBlank() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = "      ";
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertNull(instance.options.getUrisFile());

        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsUrisFileIsNull() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = null;

        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertNull(instance.options.getUrisFile());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsSetXQUERYMODULEProperty() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null;

        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsSetPROCESSMODULEProperty() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";//process-module

        Properties props = new Properties();
        props.setProperty(Options.PROCESS_MODULE, PROCESS_MODULE);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsSetInstallPropertyTrue() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = Boolean.toString(true);//install
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertTrue(instance.options.isDoInstall());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsSetInstallPropertyOne() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = "1";//install
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertTrue(instance.options.isDoInstall());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsSetInstallPropertyMaybe() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = "maybe";//install
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertFalse(instance.options.isDoInstall());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsSetDISKQUEUEMAXINMEMORYSIZEProperty() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, "10");
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(10, instance.options.getDiskQueueMaxInMemorySize());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NumberFormatException.class)
    public void testInitOptionsSetDISKQUEUEMAXINMEMORYSIZEPropertyNaN() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, "ten");
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testInitOptionsMissingPROCESSMODULE() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertNull(instance.options.getProcessModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsNormalizeLegacySystemProperties() {
        clearSystemProperties();
        String propertySuffix = ".foo";
        System.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        System.setProperty(Options.XQUERY_MODULE + propertySuffix, XQUERY_MODULE_FOO);
        System.setProperty(Options.PRE_BATCH_XQUERY_MODULE, PRE_BATCH_MODULE);
        System.setProperty(Options.PRE_BATCH_XQUERY_MODULE + propertySuffix, PRE_BATCH_XQUERY_MODULE_FOO);
        System.setProperty(Options.POST_BATCH_XQUERY_MODULE, POST_BATCH_MODULE);
        System.setProperty(Options.POST_BATCH_XQUERY_MODULE + propertySuffix, POST_BATCH_XQUERY_MODULE_FOO);

        String[] args = getDefaultArgs();
        args[2] = null; //process-module
        args[11] = null; //pre-batch-module
        args[13] = null; //post-batch-module

        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);

            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
            assertEquals(XQUERY_MODULE_FOO, System.getProperty(Options.PROCESS_MODULE + propertySuffix));
            assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
            assertEquals(PRE_BATCH_XQUERY_MODULE_FOO, System.getProperty(Options.PRE_BATCH_MODULE + propertySuffix));
            assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
            assertEquals(POST_BATCH_XQUERY_MODULE_FOO, System.getProperty(Options.POST_BATCH_MODULE + propertySuffix));
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsNormalizeLegacyProperties() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null; //process-module
        args[11] = null; //pre-batch-module
        args[13] = null; //post-batch-module
        String propertySuffix = ".foo";
        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        props.setProperty(Options.XQUERY_MODULE + propertySuffix, XQUERY_MODULE_FOO);
        props.setProperty(Options.PRE_BATCH_XQUERY_MODULE, PRE_BATCH_MODULE);
        props.setProperty(Options.PRE_BATCH_XQUERY_MODULE + propertySuffix, PRE_BATCH_XQUERY_MODULE_FOO);
        props.setProperty(Options.POST_BATCH_XQUERY_MODULE, POST_BATCH_MODULE);
        props.setProperty(Options.POST_BATCH_XQUERY_MODULE + propertySuffix, POST_BATCH_XQUERY_MODULE_FOO);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
            assertEquals(XQUERY_MODULE_FOO, instance.properties.getProperty(Options.PROCESS_MODULE + propertySuffix));
            assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
            assertEquals(PRE_BATCH_XQUERY_MODULE_FOO, instance.properties.getProperty(Options.PRE_BATCH_MODULE + propertySuffix));
            assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
            assertEquals(POST_BATCH_XQUERY_MODULE_FOO, instance.properties.getProperty(Options.POST_BATCH_MODULE + propertySuffix));
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NumberFormatException.class)
    public void testInitOptionsBatchSizeParseError() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, "one");
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testInitOptionsBatchSize() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, Integer.toString(5));
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(5, instance.options.getBatchSize());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitFailOnErrorFalseCaseInsensitive() {
        clearSystemProperties();
        System.setProperty(Options.FAIL_ON_ERROR, "False");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertFalse(instance.options.isFailOnError());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitFailOnErrorInvalidValue() {
        clearSystemProperties();
        System.setProperty(Options.FAIL_ON_ERROR, "No");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertTrue(instance.options.isFailOnError());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsEnsurePropertiesAreSet() {
        clearSystemProperties();
        System.setProperty(Options.ERROR_FILE_NAME, "out");
        System.setProperty(Options.EXPORT_FILE_PART_EXT, "pt");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(1, instance.options.getBatchSize());
            assertEquals(EXPORT_FILE_DIR, instance.properties.getProperty("EXPORT-FILE-DIR"));
            assertEquals(EXPORT_FILE_NAME, instance.properties.getProperty("EXPORT-FILE-NAME"));
            assertEquals("out", instance.properties.getProperty("ERROR-FILE-NAME"));
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitOptionsExportDirNotExists() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[13] = "/does/not/exist";
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testInitOptionsExportFileAndErrorFileExists() {
        clearSystemProperties();
        String errorFilename = "error.txt";
        File errorFile = new File(EXPORT_FILE_DIR, errorFilename);
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);

        String[] args = getDefaultArgs();

        Properties props = new Properties();
        props.setProperty(Options.ERROR_FILE_NAME, errorFilename);
        try {
            if (errorFile.createNewFile() && exportFile.createNewFile()) {
                Manager instance = getMockManagerWithEmptyResults();
                instance.init(args, props);
            }
            assertFalse(errorFile.exists());
            assertFalse(exportFile.exists());
        } catch (CorbException | IOException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsClearExportFilePartExt() {
        clearSystemProperties();

        System.setProperty(Options.EXPORT_FILE_PART_EXT, "exp");
        String[] args = getDefaultArgs();
        args[12] = null;
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "expt");
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertNull(instance.properties.getProperty(Options.EXPORT_FILE_PART_EXT));
            assertNull(System.getProperty(Options.EXPORT_FILE_PART_EXT));
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsDefaultOptions() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
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

        Manager instance = null;
        try {
            instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

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
    public void testInitOptionsInitModule() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.INIT_MODULE, "initModule");
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals("initModule", instance.options.getInitModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsProcessTaskClass() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.INIT_TASK, PROCESS_TASK);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(PROCESS_TASK, instance.options.getProcessTaskClass().getName());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsCustomUrisLoader() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        String loader = FileUrisLoader.class.getName();
        Properties props = new Properties();
        props.setProperty(Options.URIS_LOADER, loader);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(loader, instance.options.getUrisLoaderClass().getName());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsInstallWithBlankModules() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/selector.xqy";
        args[6] = "";
        args[7] = "true";

        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.options.setModulesDatabase("");
            instance.init(args, props);
            List<LogRecord> records = testLogger.getLogRecords();
            assertTrue(containsLogRecord(records, new LogRecord(Level.WARNING, "XCC configured for the filesystem: please install modules manually")));
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptionsInstallWithMissingModule() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/doesNotExist.xqy";
        args[7] = Boolean.toString(true);
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testNormalizeLegacyPropertiesWhenPropertiesIsNull() {
        try {
            Manager manager = getMockManagerWithEmptyResults();
            manager.properties = null;
            manager.normalizeLegacyProperties();
            assertNull(manager.properties);
        } catch (RequestException|CorbException ex ) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testNormalizeLegacyProperties() {
        String legacyValue1 = "legacyVal1";
        String legacyValue2 = "legacyVal2";
        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, legacyValue1);
        props.setProperty(Options.XQUERY_MODULE + ".bar", legacyValue2);

        try {
            Manager manager = getMockManagerWithEmptyResults();
            manager.properties = props;
            manager.normalizeLegacyProperties();

            assertEquals(legacyValue1, manager.properties.getProperty(Options.PROCESS_MODULE));
            assertEquals(legacyValue2, manager.properties.getProperty(Options.PROCESS_MODULE + ".bar"));
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testNormalizeLegacyPropertiesPrecedenceChecks() {
        Properties props = new Properties();
        String processVal = "legacyValue";
        props.setProperty("PROCESS-MODULE.bar", processVal);
        props.setProperty("XQUERY-MODULE.bar", "asdf");
        try {
            Manager manager = getMockManagerWithEmptyResults();
            manager.properties = props;
            manager.normalizeLegacyProperties();

            assertEquals(processVal, manager.properties.getProperty("PROCESS-MODULE.bar"));
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testClose() {
        ContentSourcePool csp = mock(ContentSourcePool.class);
        ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
        Manager manager = new Manager();
        manager.scheduledExecutor = scheduledExecutor;
        manager.csp = csp;
        try {
            manager.close();
            verify(scheduledExecutor).shutdown();
            verify(csp).close();
        } catch (IOException ex) {
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeFile() {
        File file = FileUtils.getFile("helloWorld.properties");
        Manager manager = new Manager();
        Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
        fileWatcher.onChange(file);
        assertFalse(manager.isPaused());
    }

    @Test
    public void testCommandFileWatcherRun() {
        try {
            File file = createTempFile("THREAD-COUNT=100");
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.run();
            assertEquals(100, manager.options.getThreadCount());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherRunFileDoesNotExist() {
        try {
            File file = new File("doesnotexist");
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.run();
            assertEquals(1, manager.options.getThreadCount());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeFileIsPaused() {
        try {
            File file = createTempFile("COMMAND=PAUSE");
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertTrue(testLogger.getLogRecords().isEmpty());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeFileIsStop() {
        try {
            File file = createTempFile("COMMAND=STOP");
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertTrue(manager.stopCommand);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeThreadCount() {
        commandFileWatcherOnChangeThreadCount(11, 11);
    }

    @Test
    public void testCommandFileWatcherOnChangeThreadCountIsZero() {
        commandFileWatcherOnChangeThreadCount(0, 1);
    }

    public void commandFileWatcherOnChangeThreadCount(int threads, long expectedThreadCount) {
        try {
            File file = createTempFile("THREAD-COUNT=" + Integer.toString(threads));
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertEquals(expectedThreadCount, manager.options.getThreadCount());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeFileDoesNotExist() {
        try {
            File file = new File("does-not-exist");
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.WARNING, records.get(0).getLevel());
        assertEquals("Unable to load COMMAND-FILE", records.get(0).getMessage());
        assertEquals(1, records.size());
    }

    @Test(expected = NullPointerException.class)
    public void testCommandFileWatcherOnChangeFileIsNull() {
        File file = null;
        Manager manager = new Manager();
        Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
        fileWatcher.onChange(file);
    }

    @Test
    public void testInsertModule() {
        Session session = mock(Session.class);
        Manager manager = new Manager();
        manager.options.setDoInstall(true);
        try {
            manager.insertModule(session, "src/test/resources/transform.xqy");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInsertModuleLoadedFromClassloader() {
        Session session = mock(Session.class);
        Manager manager = new Manager();
        manager.options.setDoInstall(true);
        try {
            manager.insertModule(session, "transform.xqy");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testInsertModuleLoadedFromClassloaderNotFound() {
        Session session = mock(Session.class);
        Manager manager = new Manager();
        manager.options.setDoInstall(true);
        try {
            manager.insertModule(session, "transformDoesNotExist.xqy");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = CorbException.class)
    public void testInsertModuleAndThrowException() throws CorbException {
        Session session = mock(Session.class);
        try {
            doThrow(RequestException.class).when(session).insertContent(any(Content.class));
            Manager manager = new Manager();
            manager.options.setDoInstall(true);
            manager.insertModule(session, "src/test/resources/transform.xqy");
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

    }

    @Test
    public void testSetPoolSizeGrow() {
        int size = 5;
        ThreadPoolExecutor threadPool = testSetPoolSize(5, size);
        assertEquals(size, threadPool.getMaximumPoolSize());
        assertEquals(size, threadPool.getCorePoolSize());
    }

    @Test
    public void testSetPoolSizeShrink() {
        int size = 1;
        ThreadPoolExecutor threadPool = testSetPoolSize(5, size);
        assertEquals(size, threadPool.getMaximumPoolSize());
        assertEquals(size, threadPool.getCorePoolSize());
    }

    @Test
    public void testSetPoolSizeShrinkNegative() {
        int initialSize = 5;
        ThreadPoolExecutor threadPool = testSetPoolSize(initialSize, -1);
        assertEquals(initialSize, threadPool.getMaximumPoolSize());
        assertEquals(initialSize, threadPool.getCorePoolSize());
    }

    public ThreadPoolExecutor testSetPoolSize(int initialSize, int size) {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(initialSize, initialSize, 100, TimeUnit.MILLISECONDS, mock(BlockingQueue.class));
        Manager instance = new Manager();
        instance.setPoolSize(threadPool, size);
        return threadPool;
    }

    @Test
    public void testGetTaskCls() {
        try {
            String type = "";
            String className = Transform.class.getName();
            Manager instance = new Manager();
            Class<? extends Task> expResult = Transform.class;
            Class<? extends Task> result = instance.getTaskCls(type, className);
            assertEquals(expResult, result);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTaskClsNotTaskClass() {
        String type = "";
        Manager instance = new Manager();
        Class<? extends Task> expResult = Transform.class;
        try {
            Class<? extends Task> result = instance.getTaskCls(type, String.class.getName());
            assertEquals(expResult, result);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testGetUrisLoaderCls() {
        String className = FileUrisLoader.class.getName();
        Manager instance = new Manager();
        Class<? extends UrisLoader> expResult = FileUrisLoader.class;
        try {
            Class<? extends UrisLoader> result = instance.getUrisLoaderCls(className);
            assertEquals(expResult, result);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUrisLoaderClsNotUrisClass() {
        Manager instance = new Manager();
        try {
            instance.getUrisLoaderCls(String.class.getName());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = ClassNotFoundException.class)
    public void testGetUrisLoaderClsBadClassname() throws ClassNotFoundException {
        String className = "does.not.Exist";
        Manager instance = new Manager();
        try {
            instance.getUrisLoaderCls(className);
        } catch (InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

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
        assertTrue(outContent.toString().contains(aManagerUsage));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRunMissingURISMODULEFILEANDLOADER() {
        Manager instance = new Manager();
        try {
            instance.run();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            if (ex instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) ex;
            }
        }
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testRunGetURILoaderWithURISMODULENoContentSource() {
        Manager instance = new Manager();
        instance.options.setUrisModule("someFile1.xqy");
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

    @Test(expected = IllegalArgumentException.class)
    public void testRunGetURILoaderWithURISMODULEInvalidCollection() {
        Manager instance = new Manager();
        instance.options.setUrisModule("someFile2.xqy");
        try {
            instance.initContentSourcePool(XCC_CONNECTION_URI);

            instance.run();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            if (ex instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) ex;
            }
        }
        fail();
    }

    @Test
    public void testRunGetURILoaderWithURISMODULE() {
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.initContentSourcePool(XCC_CONNECTION_URI);
            instance.collection = "URILoader_Modules";
            instance.options.setUrisModule("someFile3.xqy");
            long count = instance.run();
            assertEquals(0L, count);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testRegisterStatusInfo() throws CorbException{
        String xccRootValue = "xccRootValue";

        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        AdhocQuery adhocQuery = mock(AdhocQuery.class);
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem first = mock(ResultItem.class);
        XdmItem firstXdmItem = mock(XdmItem.class);
        ResultItem second = mock(ResultItem.class);
        XdmItem secondXdmItem = mock(XdmItem.class);

        when(contentSourcePool.get()).thenReturn(contentSource);
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

        try {
            when(session.submitRequest(any(Request.class))).thenReturn(resultSequence);

            Manager instance = getMockManagerWithEmptyResults();
            instance.csp = contentSourcePool;
            instance.registerStatusInfo();

            assertEquals(xccRootValue, instance.options.getXDBC_ROOT());
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(19, records.size());
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterStatusInfoNullContentSource() throws CorbException{
        Manager instance = new Manager();
        instance.registerStatusInfo();
        fail();
    }

    @Test
    public void testLogOptions() {
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.logOptions();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(19, records.size());
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testStop0args() {
        Manager instance = new Manager();
        instance.stop();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("cleaning up", records.get(0).getMessage());
    }

    @Test
    public void testStopExecutionException() {
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
    public void testSetThreadCountWithInvalidValue() {
        Manager instance = new Manager();
        instance.setThreadCount(-5);
        assertEquals(1, instance.options.getThreadCount());
        instance.setThreadCount(0);
        assertEquals(1, instance.options.getThreadCount());
    }

    @Test
    public void testNoResultsPrePostBatchAlwaysExecuteTrue() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = null;
        args[9] = null;
        args[11] = null;
        args[15] = null;
        Properties props = new Properties();
        props.setProperty(Options.PRE_POST_BATCH_ALWAYS_EXECUTE, Boolean.toString(true));
        props.setProperty(Options.URIS_LOADER, MockEmptyFileUrisLoader.class.getName());
        props.setProperty(Options.PRE_BATCH_MINIMUM_COUNT, Integer.toString(0));
        props.setProperty(Options.POST_BATCH_MINIMUM_COUNT, Integer.toString(0));
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, "top content");
        props.setProperty(Options.PRE_BATCH_TASK, PRE_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "bottom content");
        props.setProperty(Options.POST_BATCH_TASK, POST_BATCH_TASK);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            instance.run();

            File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
            assertTrue(exportFile.exists());
            assertEquals(2, FileUtils.getLineCount(exportFile));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testNoResultsDefaultPrePostExecution() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = null;
        args[9] = null;
        args[11] = null;
        args[15] = null;
        Properties props = new Properties();
        props.setProperty(Options.URIS_LOADER, MockEmptyFileUrisLoader.class.getName());
        props.setProperty(Options.PRE_BATCH_TASK, PRE_BATCH_TASK);
        props.setProperty(Options.POST_BATCH_TASK, POST_BATCH_TASK);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            instance.run();

            File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
            assertFalse(exportFile.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }


    @Test
    public void testNoResultsPrePostBatchAlwaysExecuteFalseMinCountGreater() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = null;
        args[9] = null;
        args[11] = null;
        args[15] = null;
        Properties props = new Properties();
        props.setProperty(Options.PRE_POST_BATCH_ALWAYS_EXECUTE, Boolean.toString(false));
        props.setProperty(Options.URIS_LOADER, MockEmptyFileUrisLoader.class.getName());
        props.setProperty(Options.PRE_BATCH_MINIMUM_COUNT, Integer.toString(10));
        props.setProperty(Options.POST_BATCH_MINIMUM_COUNT, Integer.toString(10));
        props.setProperty(Options.PRE_BATCH_TASK, PRE_BATCH_TASK);
        props.setProperty(Options.POST_BATCH_TASK, POST_BATCH_TASK);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            instance.run();

            File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
            assertFalse(exportFile.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testResultsPrePostBatchAlwaysExecuteFalseMinCountGreater() throws IOException {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = null;
        args[9] = null;
        args[11] = null;
        args[15] = null;
        Properties props = new Properties();
        props.setProperty(Options.PRE_POST_BATCH_ALWAYS_EXECUTE, Boolean.toString(false));
        props.setProperty(Options.URIS_LOADER, MockEmptyFileUrisLoader.class.getName());
        props.setProperty(Options.PRE_BATCH_MINIMUM_COUNT, Integer.toString(10));
        props.setProperty(Options.POST_BATCH_MINIMUM_COUNT, Integer.toString(10));
        props.setProperty(Options.PRE_BATCH_TASK, PRE_BATCH_TASK);
        props.setProperty(Options.POST_BATCH_TASK, POST_BATCH_TASK);

        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            instance.run();

            File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
            assertFalse(exportFile.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testSubmitUriTasksWithUrisRedacted() {
        UrisLoader urisLoader = mock(UrisLoader.class);
        try {
            Manager manager = getMockManagerWithEmptyResults();
            CompletionService completionService = mock(CompletionService.class);
            manager.completionService = completionService;
            manager.pool = mock(PausableThreadPoolExecutor.class);
            when(urisLoader.hasNext()).thenAnswer(new Answer() {
                private int count = 0;
                public Object answer(InvocationOnMock invocation) {
                    if (count++ == 50000) {
                        return false;
                    }
                    return true;
                }
            });
            when(urisLoader.next()).thenAnswer(new Answer() {
                private int count = 0;
                public Object answer(InvocationOnMock invocation) {
                    if (count++ == 50000) {
                        return null;
                    }
                    return "uri";
                }
            });
            Properties properties = ManagerTest.getDefaultProperties();
            properties.setProperty(Options.URIS_REDACTED, Boolean.TRUE.toString());
            properties.setProperty(Options.FAIL_ON_ERROR, Boolean.FALSE.toString());

            manager.init(properties);

            long processedCount = manager.submitUriTasks(urisLoader, mock(TaskFactory.class), 50000);
            assertEquals(50000, processedCount);
            List<LogRecord> records = testLogger.getLogRecords();
            assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "received 50,000/50,000")));

        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testSubmitUriTasksWithUrisNotRedacted() {
        UrisLoader urisLoader = mock(UrisLoader.class);
        try {
            Manager manager = getMockManagerWithEmptyResults();
            CompletionService completionService = mock(CompletionService.class);
            manager.completionService = completionService;
            manager.pool = mock(PausableThreadPoolExecutor.class);
            when(urisLoader.hasNext()).thenAnswer(new Answer() {
                private int count = 0;
                public Object answer(InvocationOnMock invocation) {
                    if (count++ == 50000) {
                        return false;
                    }
                    return true;
                }
            });
            when(urisLoader.next()).thenAnswer(new Answer() {
                private int count = 0;
                public Object answer(InvocationOnMock invocation) {
                    if (count++ == 50000) {
                        return null;
                    }
                    return "uri";
                }
            });
            Properties properties = ManagerTest.getDefaultProperties();
            properties.setProperty(Options.FAIL_ON_ERROR, Boolean.FALSE.toString());

            manager.init(properties);

            long processedCount = manager.submitUriTasks(urisLoader, mock(TaskFactory.class), 50000);
            assertEquals(50000, processedCount);
            List<LogRecord> records = testLogger.getLogRecords();
            assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "received 50,000/50,000: uri")));

        } catch (Exception ex) {
            fail();
        }
    }

    public static String[] getDefaultArgs() {
        return new String[]{XCC_CONNECTION_URI,
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
    }

    public static Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(Options.XCC_CONNECTION_URI, ManagerTest.XCC_CONNECTION_URI);
        properties.setProperty(Options.COLLECTION_NAME, ManagerTest.COLLECTION_NAME);
        properties.setProperty(Options.PROCESS_MODULE, ManagerTest.XQUERY_MODULE);
        properties.setProperty(Options.THREAD_COUNT, ManagerTest.THREAD_COUNT);
        properties.setProperty(Options.MODULE_ROOT, ManagerTest.MODULES_ROOT);
        properties.setProperty(Options.MODULES_DATABASE, ManagerTest.MODULES_DATABASE);
        properties.setProperty(Options.INSTALL, Boolean.toString(false));
        properties.setProperty(Options.PROCESS_TASK, ManagerTest.PROCESS_TASK);
        properties.setProperty(Options.PRE_BATCH_MODULE, ManagerTest.PRE_BATCH_MODULE);
        properties.setProperty(Options.PRE_BATCH_TASK, ManagerTest.PRE_BATCH_TASK);
        properties.setProperty(Options.POST_BATCH_MODULE, ManagerTest.POST_BATCH_MODULE);
        properties.setProperty(Options.POST_BATCH_TASK, ManagerTest.POST_BATCH_TASK);
        properties.setProperty(Options.METRICS_LOG_LEVEL, ManagerTest.LOG_LEVEL_INFO);


        if (ManagerTest.EXPORT_FILE_DIR != null) {
        	properties.setProperty(Options.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_DIR);
        }
        properties.setProperty(Options.URIS_FILE, ManagerTest.URIS_FILE);
        return properties;
    }

    public static void setDefaultSystemProperties() {
        System.getProperties().putAll(getDefaultProperties());
    }

    public File createTempFile(String content) throws IOException {
        List<String> lines = Collections.singletonList(content);
        return createTempFile(lines);
    }

    public File createTempFile(List<String> lines) throws IOException {
        Path path = Files.createTempFile("tmp", "txt");
        File file = path.toFile();
        file.deleteOnExit();
        Files.write(path, lines, Charset.forName("UTF-8"));
        return file;
    }

    public static Manager getMockManagerWithEmptyResults() throws RequestException, CorbException{
    		Manager manager = spy(new Manager());
    		ContentSourcePool contentSourcePool = getMockContentSourceManagerWithEmptyResults();
    		when(manager.createContentSourcePool()).thenReturn(contentSourcePool);
    		return manager;
    }


    public static ContentSourcePool getMockContentSourceManagerWithEmptyResults() throws RequestException, CorbException{
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke moduleInvoke = mock(ModuleInvoke.class);
        ResultSequence res = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        ResultItem uriCountResult = mock(ResultItem.class);
        XdmItem batchRefItem = mock(XdmItem.class);
        XdmItem uriCount = mock(XdmItem.class);

        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSourcePool.available()).thenReturn(true);
        when(contentSource.newSession()).thenReturn(session);
        when(contentSource.newSession(any())).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(moduleInvoke);
        when(session.submitRequest(any())).thenReturn(res);
        when(res.next()).thenReturn(resultItem).thenReturn(uriCountResult).thenReturn(null);
        when(resultItem.getItem()).thenReturn(batchRefItem);
        when(uriCountResult.getItem()).thenReturn(uriCount);
        when(batchRefItem.asString()).thenReturn("batchRefVal");
        when(uriCount.asString()).thenReturn(Integer.toString(0));
        return contentSourcePool;
    }

    @Test
    public void testLogIfNotLowMemory() {
        Manager manager = new Manager();
        manager.logIfLowMemory(Runtime.getRuntime().totalMemory());
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(0, records.size());
    }

    @Test
    public void testLogLowMemory() {
        Manager manager = new Manager();
        manager.logIfLowMemory(Runtime.getRuntime().freeMemory() * 6);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(2, records.size());
        assertEquals(Level.WARNING, records.get(0).getLevel());
        assertEquals(Level.WARNING, records.get(1).getLevel());
    }

    @Test
    public void testPause() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isRunning()).thenReturn(Boolean.TRUE);
        JobStats jobStats = mock(JobStats.class);
        Manager manager = new Manager();
        manager.pool = pool;
        manager.jobStats = jobStats;

        manager.pause();
        verify(pool).pause();
    }

    @Test
    public void testPauseNotRunning() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isRunning()).thenReturn(Boolean.FALSE);
        JobStats jobStats = mock(JobStats.class);
        Manager manager = new Manager();
        manager.pool = pool;
        manager.jobStats = jobStats;

        manager.pause();
        verify(pool, never()).pause();
    }

    @Test
    public void testResume() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isPaused()).thenReturn(Boolean.TRUE);
        when(pool.isRunning()).thenReturn(Boolean.TRUE);
        JobStats jobStats = mock(JobStats.class);
        Manager manager = new Manager();
        manager.pool = pool;
        manager.jobStats = jobStats;

        manager.resume();
        verify(pool).resume();
    }

    @Test
    public void testResumeNotPaused() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isPaused()).thenReturn(Boolean.FALSE);
        when(pool.isRunning()).thenReturn(Boolean.TRUE);
        JobStats jobStats = mock(JobStats.class);
        Manager manager = new Manager();
        manager.pool = pool;
        manager.jobStats = jobStats;

        manager.resume();
        verify(pool, never()).resume();
    }

    public static class MockEmptyFileUrisLoader extends FileUrisLoader {

        @Override
        public void open() throws CorbException {
            this.setTotalCount(0);
        }
    }
}
