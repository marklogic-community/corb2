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
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Arrays;
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
    public static String EXPORT_FILE_DIR = null;
    public static final String EXPORT_FILE_NAME = "exportFile.out";
    public static final String URIS_FILE = "src/test/resources/uris-file.txt";
    public static final String XQUERY_MODULE_FOO = "process-bar";
    public static final String POST_BATCH_XQUERY_MODULE_FOO = "post-bar";
    public static final String PRE_BATCH_XQUERY_MODULE_FOO = "pre-bar";
    public static final String PROCESS_MODULE = "src/test/resources/transform2.xqy|ADHOC";

    @Before
    public void setUp()
            throws Exception {
        clearSystemProperties();
        MANAGER_LOGGER.addHandler(testLogger);
        File tempDir = TestUtils.createTempDirectory();
        EXPORT_FILE_DIR = tempDir.toString();
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteFile(ManagerTest.EXPORT_FILE_DIR);
        clearSystemProperties();
        System.setErr(systemErr);
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
        assertEquals("queue is full: size = {0} (will only appear once)", records.get(0).getMessage());
        assertEquals(1, records.size());
    }

    @Test(expected = InstantiationException.class)
    public void testInitNullArgsProperties() throws InstantiationException {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, Integer.toString(5));
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (IOException | URISyntaxException | ClassNotFoundException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = InstantiationException.class)
    public void testInitNullArgsEmptyProperties() throws InstantiationException {
        String[] args = null;
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (IOException | URISyntaxException | ClassNotFoundException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions() {
        try {
            String[] args = {};
            Manager instance = new Manager();
            instance.initOptions(args);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptionsWithEmptyProperties() {
        String[] args = null;
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.properties = new Properties();
            instance.initOptions(args);

        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RequestException ex) {
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

        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (RequestException | IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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

        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        props.setProperty("XQUERY-MODULE.foo", XQUERY_MODULE_FOO);
        props.setProperty(Options.PRE_BATCH_XQUERY_MODULE, PRE_BATCH_MODULE);
        props.setProperty("PRE-BATCH-XQUERY-MODULE.foo", PRE_BATCH_XQUERY_MODULE_FOO);
        props.setProperty(Options.POST_BATCH_XQUERY_MODULE, POST_BATCH_MODULE);
        props.setProperty("POST-BATCH-XQUERY-MODULE.foo", POST_BATCH_XQUERY_MODULE_FOO);
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
            assertEquals(XQUERY_MODULE_FOO, instance.properties.getProperty("PROCESS-MODULE.foo"));
            assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
            assertEquals(PRE_BATCH_XQUERY_MODULE_FOO, instance.properties.getProperty("PRE-BATCH-MODULE.foo"));
            assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
            assertEquals(POST_BATCH_XQUERY_MODULE_FOO, instance.properties.getProperty("POST-BATCH-MODULE.foo"));
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsEnsurePropertiesAreSet() {
        clearSystemProperties();
        System.setProperty(Options.ERROR_FILE_NAME, EXPORT_FILE_DIR + "/out");
        System.setProperty(Options.EXPORT_FILE_PART_EXT, "pt");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertEquals(1, instance.options.getBatchSize());
            assertEquals(EXPORT_FILE_DIR, instance.properties.getProperty("EXPORT-FILE-DIR"));
            assertEquals(EXPORT_FILE_NAME, instance.properties.getProperty("EXPORT-FILE-NAME"));
            assertEquals(EXPORT_FILE_DIR + "/out", instance.properties.getProperty("ERROR-FILE-NAME"));
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
            errorFile.createNewFile();
            exportFile.createNewFile();
            Manager instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
            assertFalse(errorFile.exists());
            assertFalse(exportFile.exists());
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (RequestException | IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInitOptionsDefaultOptions() {
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

        Manager instance = null;
        try {
            instance = getMockManagerWithEmptyResults();
            instance.init(args, props);
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (RequestException | IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
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
        } catch (IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException | RequestException ex) {
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
        } catch (RequestException | IOException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | XccConfigException | GeneralSecurityException ex) {
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
        } catch (RequestException ex) {
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
        props.setProperty("XQUERY-MODULE.bar", legacyValue2);

        try {
            Manager manager = getMockManagerWithEmptyResults();
            manager.properties = props;
            manager.normalizeLegacyProperties();

            assertEquals(legacyValue1, manager.properties.getProperty(Options.PROCESS_MODULE));
            assertEquals(legacyValue2, manager.properties.getProperty("PROCESS-MODULE.bar"));
        } catch (RequestException ex) {
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
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
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
            List<String> lines = Arrays.asList("THREAD-COUNT=100");
            File file = createTempFile(lines);
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.run();
            assertEquals(100, manager.options.getThreadCount());
        } catch (Exception ex) {
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
            List<String> lines = Arrays.asList("COMMAND=PAUSE");
            File file = createTempFile(lines);
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertTrue(testLogger.getLogRecords().isEmpty());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeFileIsStop() {
        try {
            List<String> lines = Arrays.asList("COMMAND=STOP");
            File file = createTempFile(lines);
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertTrue(manager.stopCommand);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeThreadCount() {
        try {
            List<String> lines = Arrays.asList("THREAD-COUNT=11");
            File file = createTempFile(lines);
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertEquals(11, manager.options.getThreadCount());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCommandFileWatcherOnChangeThreadCountIsZero() {
        try {
            List<String> lines = Arrays.asList("THREAD-COUNT=0");
            File file = createTempFile(lines);
            Manager manager = new Manager();
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertEquals(1, manager.options.getThreadCount());
        } catch (Exception ex) {
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
            instance.contentSource = ContentSourceFactory.newContentSource(new URI(XCC_CONNECTION_URI));
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
            instance.collection = "URILoader_Modules";
            instance.options.setUrisModule("someFile3.xqy");
            int count = instance.run();
            assertEquals(0, count);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testRegisterStatusInfo() {
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

        try {
            when(session.submitRequest(any(Request.class))).thenReturn(resultSequence);

            Manager instance = getMockManagerWithEmptyResults();
            instance.contentSource = contentSource;
            instance.registerStatusInfo();

            assertEquals(xccRootValue, instance.options.getXDBC_ROOT());
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(19, records.size());
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterStatusInfoNullContentSource() {
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
        } catch (RequestException ex) {
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
    public void testSetThreadCount_InvalidValue() {
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

    public File createTempFile(List<String> lines) throws IOException {
        Path path = Files.createTempFile("tmp", "txt");
        File file = path.toFile();
        file.deleteOnExit();
        Files.write(path, lines, Charset.forName("UTF-8"));
        return file;
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
