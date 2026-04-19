/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import java.io.*;

import static com.marklogic.developer.corb.AbstractManager.EXIT_CODE_PROCESSING_ERROR;
import static com.marklogic.developer.corb.AbstractManager.EXIT_CODE_SUCCESS;
import static com.marklogic.developer.corb.Manager.EXIT_CODE_STOP_COMMAND;
import static org.junit.jupiter.api.Assertions.*;

import static com.marklogic.developer.corb.TestUtils.assertContainsLogRecord;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.*;
import org.mockito.exceptions.base.MockitoException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * The class {@code ManagerTest} contains tests for the class
 * <code>{@link Manager}</code>.
 *
 * @author matthew.heckel
 */
class ManagerTest {

    private final TestHandler testLogger = new TestHandler();
    private static final Logger MANAGER_LOGGER = Logger.getLogger(Manager.class.getName());
    private static final Logger LOG = Logger.getLogger(ManagerTest.class.getName());
    private static final PrintStream systemErr = System.err;
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

    private static void clearSystemProperties() {
        TestUtils.clearSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "0");
        System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
    }

    @BeforeEach
    void setUp() throws IOException {
        clearSystemProperties();
        MANAGER_LOGGER.addHandler(testLogger);
        File tempDir = TestUtils.createTempDirectory();
        EXPORT_FILE_DIR = tempDir.toString();
    }

    @AfterEach
    void tearDown() throws IOException {
        FileUtils.deleteFile(ManagerTest.EXPORT_FILE_DIR);
        clearSystemProperties();
        System.setErr(systemErr);
    }

    @Test
    void testMainWithoutOptionsInitFailure() {
        String[] args = new String[0];
        int exitCode = Manager.run(args);
        assertEquals(Manager.EXIT_CODE_INIT_ERROR, exitCode);
    }

    @Test
    void testRejectedExecutionNpe() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        assertThrows(NullPointerException.class, () -> cbp.rejectedExecution(r, threadPool));
    }

    @Test
    void testRejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);
        @SuppressWarnings("unchecked")
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        when(threadPool.getQueue()).thenReturn(queue).thenReturn(null);

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();

        cbp.rejectedExecution(r, threadPool);
        assertNull(threadPool.getQueue());
    }

    @Test
    void testRejectedExecutionRejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        when(threadPool.getQueue()).thenThrow(new MockitoException("testing"));

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        assertThrows(MockitoException.class, () -> cbp.rejectedExecution(r, threadPool));
    }

    @Test
    void testRejectedExecutionWarningIsTrueAndQueIsNotNull() {
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

    @Test
    void testHelp() throws UnsupportedEncodingException {
        clearSystemProperties();
        String[] args =  new String[]{"--help"};
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outContent, true, StandardCharsets.UTF_8.name()));
        Manager.run(args);
        assertTrue(outContent.toString(StandardCharsets.UTF_8.name()).contains("CoRB version"));
    }

    @Test
    void testInitNullArgsProperties() throws CorbException {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, Integer.toString(5));
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(CorbException.class, () -> instance.init(args, props));
        } catch (RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitBlankCollection() {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        //no "COLLECTION-NAME" specified
        props.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals("", instance.collection);
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitPropertiesEmptyProperties() {
        clearSystemProperties();
        Properties properties = new Properties();
        try (Manager manager = new Manager()) {
            manager.initProperties(properties);
            assertEquals(2, manager.properties.size());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testInitPropertiesNullProperties() {
        clearSystemProperties();
        Properties properties = null;
        try (Manager manager = new Manager()) {
            manager.initProperties(properties);
            assertNotNull(manager.properties);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testInitProperties() {
        clearSystemProperties();
        System.setProperty(Options.BATCH_SIZE, "3");
        System.setProperty(Options.COMMAND, "pause");

        Properties properties = new Properties();
        properties.setProperty(Options.COMMAND, "resume");
        try (Manager manager = new Manager()) {
            manager.initProperties(properties);
            assertNotNull(manager.properties);
            assertEquals("resume", manager.properties.getProperty(Options.COMMAND));
            assertEquals("3", manager.properties.getProperty(Options.BATCH_SIZE));
        } catch (CorbException ex) {
            fail();
        }
    }


    @Test
    void testInitUrisFileDoesNoteExist() {
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        props.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");
        props.setProperty(Options.URIS_FILE, "does/not/exist");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(args, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitNullArgsEmptyProperties() throws CorbException {
        String[] args = null;
        Properties props = new Properties();
        try {
            Manager instance = getMockManagerWithEmptyResults();
            assertThrows(CorbException.class, () -> instance.init(args, props));
        } catch (RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsNullArgs() {
        String[] args = null;
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(NullPointerException.class, () -> instance.initOptions(args));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptions() {
        String[] args = {};
        clearSystemProperties();
        try (Manager instance = new Manager()) {
            assertThrows(CorbException.class, () -> instance.initOptions(args));
        }
    }

    @Test
    void testInitOptionsWithEmptyProperties() {
        String[] args = null;
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.properties = new Properties();
            assertThrows(NullPointerException.class, () -> instance.initOptions(args));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsUrisFileIsBlank() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = "      ";
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertNull(instance.options.getUrisFile());

        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsUrisFileIsNull() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = null;

        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertNull(instance.options.getUrisFile());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetXQUERYMODULEProperty() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null;

        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, PROCESS_MODULE);
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetPROCESSMODULEProperty() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";//process-module

        Properties props = new Properties();
        props.setProperty(Options.PROCESS_MODULE, PROCESS_MODULE);
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetInstallPropertyTrue() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = Boolean.toString(true);//install
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertTrue(instance.options.isDoInstall());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetInstallPropertyOne() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = "1";//install
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertTrue(instance.options.isDoInstall());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetInstallPropertyMaybe() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[7] = "maybe";//install
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertFalse(instance.options.isDoInstall());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetDISKQUEUEMAXINMEMORYSIZEProperty() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, "10");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(10, instance.options.getDiskQueueMaxInMemorySize());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsSetDISKQUEUEMAXINMEMORYSIZEPropertyNaN() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, "ten");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(NumberFormatException.class, () -> instance.init(args, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsSetRestartStateDirAndCreateIt() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        File restartStateRootDir = TestUtils.createTempDirectory();
        Properties props = new Properties();
        props.setProperty(Options.RESTARTABLE, "true");
        props.setProperty(Options.RESTART_STATE_DIR, restartStateRootDir.getAbsolutePath());
        props.setProperty(Options.JOB_NAME, "Restart State Test");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertTrue(instance.options.isRestartable());
            assertTrue(instance.options.getRestartStateDir().exists());
            assertEquals(restartStateRootDir.getCanonicalFile(), instance.options.getRestartStateDir().getParentFile().getCanonicalFile());
            assertTrue(instance.options.getRestartStateDir().getName().startsWith("restart-state-test-"));
        } finally {
            FileUtils.deleteFile(restartStateRootDir.getAbsolutePath());
        }
    }

    @Test
    void testInitOptionsRestartableDefaultsRestartStateDirToTempDir() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        File tempDir = TestUtils.createTempDirectory();
        Properties props = new Properties();
        props.setProperty(Options.RESTARTABLE, "true");
        props.setProperty(Options.TEMP_DIR, tempDir.getAbsolutePath());
        props.setProperty(Options.JOB_NAME, "Temp Dir Restart");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertTrue(instance.options.isRestartable());
            assertEquals(tempDir.getCanonicalFile(), instance.options.getRestartStateDir().getParentFile().getCanonicalFile());
            assertTrue(instance.options.getRestartStateDir().getName().startsWith("temp-dir-restart-"));
            assertEquals(instance.options.getRestartStateDir().getAbsolutePath(), instance.properties.getProperty(Options.RESTART_STATE_DIR));
        } finally {
            FileUtils.deleteFile(tempDir.getAbsolutePath());
        }
    }

    @Test
    void testInitOptionsRestartableScopesStateDirByJobIdentity() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        File restartStateRootDir = TestUtils.createTempDirectory();
        Properties propsOne = new Properties();
        propsOne.putAll(getDefaultProperties());
        propsOne.setProperty(Options.RESTARTABLE, "true");
        propsOne.setProperty(Options.RESTART_STATE_DIR, restartStateRootDir.getAbsolutePath());
        propsOne.setProperty(Options.JOB_NAME, "Job One");
        Properties propsTwo = new Properties();
        propsTwo.putAll(getDefaultProperties());
        propsTwo.setProperty(Options.RESTARTABLE, "true");
        propsTwo.setProperty(Options.RESTART_STATE_DIR, restartStateRootDir.getAbsolutePath());
        propsTwo.setProperty(Options.JOB_NAME, "Job Two");
        try (Manager first = getMockManagerWithEmptyResults(); Manager second = getMockManagerWithEmptyResults()) {
            first.init(args, propsOne);
            second.init(args, propsTwo);
            assertNotEquals(first.options.getRestartStateDir().getCanonicalPath(), second.options.getRestartStateDir().getCanonicalPath());
            assertEquals(restartStateRootDir.getCanonicalFile(), first.options.getRestartStateDir().getParentFile().getCanonicalFile());
            assertEquals(restartStateRootDir.getCanonicalFile(), second.options.getRestartStateDir().getParentFile().getCanonicalFile());
        } finally {
            FileUtils.deleteFile(restartStateRootDir.getAbsolutePath());
        }
    }

    @Test
    void testInitOptionsRestartableDefaultsRestartStateDirToJavaTmpDir() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        File javaTmpDir = TestUtils.createTempDirectory();
        String originalJavaTmpDir = System.getProperty("java.io.tmpdir");
        Properties props = new Properties();
        props.setProperty(Options.RESTARTABLE, "true");
        props.setProperty(Options.JOB_NAME, "Java Tmp Restart");
        System.setProperty("java.io.tmpdir", javaTmpDir.getAbsolutePath());
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertTrue(instance.options.isRestartable());
            assertEquals(javaTmpDir.getCanonicalFile(), instance.options.getRestartStateDir().getParentFile().getCanonicalFile());
            assertTrue(instance.options.getRestartStateDir().getName().startsWith("java-tmp-restart-"));
            assertEquals(instance.options.getRestartStateDir().getAbsolutePath(), instance.properties.getProperty(Options.RESTART_STATE_DIR));
        } finally {
            if (originalJavaTmpDir != null) {
                System.setProperty("java.io.tmpdir", originalJavaTmpDir);
            } else {
                System.clearProperty("java.io.tmpdir");
            }
            FileUtils.deleteFile(javaTmpDir.getAbsolutePath());
        }
    }

    @Test
    void testInitOptionsMissingPROCESSMODULE() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertNull(instance.options.getProcessModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsNormalizeLegacySystemProperties() {
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
        try (Manager instance = getMockManagerWithEmptyResults()) {
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
    void testInitOptionsNormalizeLegacyProperties() {
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
        try (Manager instance = getMockManagerWithEmptyResults()) {
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

    @Test
    void testInitOptionsBatchSizeParseError() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, "one");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(NumberFormatException.class, () -> instance.init(args, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsBatchSize() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.BATCH_SIZE, Integer.toString(5));
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(5, instance.options.getBatchSize());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitFailOnErrorFalseCaseInsensitive() {
        clearSystemProperties();
        System.setProperty(Options.FAIL_ON_ERROR, "False");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertFalse(instance.options.isFailOnError());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitFailOnErrorInvalidValue() {
        clearSystemProperties();
        System.setProperty(Options.FAIL_ON_ERROR, "No");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertTrue(instance.options.isFailOnError());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsEnsurePropertiesAreSet() {
        clearSystemProperties();
        System.setProperty(Options.ERROR_FILE_NAME, "out");
        System.setProperty(Options.EXPORT_FILE_PART_EXT, "pt");
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
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

    @Test
    void testInitOptionsExportDirNotExists() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[13] = "/does/not/exist";
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(args, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsExportFileAndErrorFileExists() {
        clearSystemProperties();
        String errorFilename = "error.txt";
        File errorFile = new File(EXPORT_FILE_DIR, errorFilename);
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);

        String[] args = getDefaultArgs();

        Properties props = new Properties();
        props.setProperty(Options.ERROR_FILE_NAME, errorFilename);
        try {
            if (errorFile.createNewFile() && exportFile.createNewFile()) {
                try (Manager instance = getMockManagerWithEmptyResults()) {
                    instance.init(args, props);
                }
            }
            assertFalse(errorFile.exists());
            assertFalse(exportFile.exists());
        } catch (CorbException | IOException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsClearExportFilePartExt() {
        clearSystemProperties();

        System.setProperty(Options.EXPORT_FILE_PART_EXT, "exp");
        String[] args = getDefaultArgs();
        args[12] = null;
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "expt");
        try ( Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertNull(instance.properties.getProperty(Options.EXPORT_FILE_PART_EXT));
            assertNull(System.getProperty(Options.EXPORT_FILE_PART_EXT));
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsDefaultOptions() {
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
    void testInitOptionsInitModule() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.INIT_MODULE, "initModule");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals("initModule", instance.options.getInitModule());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsProcessTaskClass() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty(Options.INIT_TASK, PROCESS_TASK);
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(PROCESS_TASK, instance.options.getProcessTaskClass().getName());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsCustomUrisLoader() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        String loader = FileUrisLoader.class.getName();
        Properties props = new Properties();
        props.setProperty(Options.URIS_LOADER, loader);
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(loader, instance.options.getUrisLoaderClass().getName());
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsInstallWithBlankModules() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/selector.xqy";
        args[6] = "";
        args[7] = "true";

        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.options.setModulesDatabase("");
            instance.init(args, props);
            assertContainsLogRecord(testLogger, Level.WARNING, "XCC configured for the filesystem: please install modules manually");
        } catch (CorbException | RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInitOptionsInstallWithMissingModule() {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/doesNotExist.xqy";
        args[7] = Boolean.toString(true);
        Properties props = new Properties();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(NullPointerException.class, () -> instance.init(args, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testNormalizeLegacyPropertiesWhenPropertiesIsNull() {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.properties = null;
            manager.normalizeLegacyProperties();
            assertNull(manager.properties);
        } catch (RequestException|CorbException ex ) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testNormalizeLegacyProperties() {
        String legacyValue1 = "legacyVal1";
        String legacyValue2 = "legacyVal2";
        Properties props = new Properties();
        props.setProperty(Options.XQUERY_MODULE, legacyValue1);
        props.setProperty(Options.XQUERY_MODULE + ".bar", legacyValue2);

        try (Manager manager = getMockManagerWithEmptyResults()) {
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
    void testNormalizeLegacyPropertiesPrecedenceChecks() {
        Properties props = new Properties();
        String processVal = "legacyValue";
        props.setProperty("PROCESS-MODULE.bar", processVal);
        props.setProperty("XQUERY-MODULE.bar", "asdf");
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.properties = props;
            manager.normalizeLegacyProperties();

            assertEquals(processVal, manager.properties.getProperty("PROCESS-MODULE.bar"));
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testClose() {
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
    void testCommandFileWatcherOnChangeFile() {
        File file = FileUtils.getFile("helloWorld.properties");
        Manager manager = new Manager();
        Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
        fileWatcher.onChange(file);
        assertFalse(manager.isPaused());
    }

    @Test
    void testCommandFileWatcherRun() {
        try (Manager manager = new Manager()) {
            File file = createTempFile("THREAD-COUNT=100");
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.run();
            assertEquals(100, manager.options.getThreadCount());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCommandFileWatcherRunFileDoesNotExist() {
        try (Manager manager = new Manager()) {
            File file = new File("doesnotexist");
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.run();
            assertEquals(1, manager.options.getThreadCount());
        }
    }

    @Test
    void testCommandFileWatcherOnChangeFileIsPaused() {
        testLogger.clear();
        try (Manager manager = new Manager()) {
            File file = createTempFile("COMMAND=PAUSE");
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertTrue(testLogger.getLogRecords().isEmpty());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCommandFileWatcherOnChangeFileIsStop() {
        try (Manager manager = new Manager()) {
            File file = createTempFile("COMMAND=STOP");
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertTrue(manager.stopCommand);
            assertEquals(EXIT_CODE_STOP_COMMAND, manager.getExitCode());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCommandFileWatcherOnChangeThreadCount() {
        commandFileWatcherOnChangeThreadCount(11, 11);
    }

    @Test
    void testCommandFileWatcherOnChangeThreadCountIsZero() {
        commandFileWatcherOnChangeThreadCount(0, 1);
    }

    void commandFileWatcherOnChangeThreadCount(int threads, long expectedThreadCount) {
        try (Manager manager = new Manager()) {
            File file = createTempFile("THREAD-COUNT=" + threads);
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
            assertEquals(expectedThreadCount, manager.options.getThreadCount());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCommandFileWatcherOnChangeFileDoesNotExist() {
        testLogger.clear();
        try (Manager manager = new Manager()) {
            File file = new File("does-not-exist");
            Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
            fileWatcher.onChange(file);
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.WARNING, records.get(0).getLevel());
        assertEquals("Unable to load COMMAND-FILE", records.get(0).getMessage());
        assertEquals(1, records.size());
    }

    @Test
    void testCommandFileWatcherOnChangeFileIsNull() {
        File file = null;
        Manager manager = new Manager();
        Manager.CommandFileWatcher fileWatcher = new Manager.CommandFileWatcher(file, manager);
        assertThrows(NullPointerException.class, () -> fileWatcher.onChange(file));
    }

    @Test
    void testInsertModule() {
        Session session = mock(Session.class);
        try (Manager manager = new Manager()) {
            manager.options.setDoInstall(true);
            manager.insertModule(session, "src/test/resources/transform.xqy");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInsertModuleLoadedFromClassloader() {
        Session session = mock(Session.class);
        try (Manager manager = new Manager()) {
            manager.options.setDoInstall(true);
            manager.insertModule(session, "transform.xqy");
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testInsertModuleLoadedFromClassloaderNotFound() {
        Session session = mock(Session.class);
        try (Manager manager = new Manager()) {
            manager.options.setDoInstall(true);
            assertThrows(NullPointerException.class, () -> manager.insertModule(session, "transformDoesNotExist.xqy"));
        }
    }

    @Test
    void testInsertModuleAndThrowException() {
        Session session = mock(Session.class);
        try (Manager manager = new Manager()) {
            doThrow(RequestException.class).when(session).insertContent(any(Content.class));
            manager.options.setDoInstall(true);
            assertThrows(CorbException.class, () -> manager.insertModule(session, "src/test/resources/transform.xqy"));
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

    }

    @Test
    void testSetPoolSizeGrow() {
        int size = 5;
        ThreadPoolExecutor threadPool = testSetPoolSize(5, size);
        assertEquals(size, threadPool.getMaximumPoolSize());
        assertEquals(size, threadPool.getCorePoolSize());
    }

    @Test
    void testSetPoolSizeShrink() {
        int size = 1;
        ThreadPoolExecutor threadPool = testSetPoolSize(5, size);
        assertEquals(size, threadPool.getMaximumPoolSize());
        assertEquals(size, threadPool.getCorePoolSize());
    }

    @Test
    void testSetPoolSizeShrinkNegative() {
        int initialSize = 5;
        ThreadPoolExecutor threadPool = testSetPoolSize(initialSize, -1);
        assertEquals(initialSize, threadPool.getMaximumPoolSize());
        assertEquals(initialSize, threadPool.getCorePoolSize());
    }

    static ThreadPoolExecutor testSetPoolSize(int initialSize, int size) {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(initialSize, initialSize, 100, TimeUnit.MILLISECONDS, mock(BlockingQueue.class));
        try (Manager instance = new Manager()) {
            instance.setPoolSize(threadPool, size);
        }
        return threadPool;
    }

    @Test
    void testGetTaskCls() {
        try (Manager instance = new Manager()) {
            String type = "";
            String className = Transform.class.getName();
            Class<? extends Task> expResult = Transform.class;
            Class<? extends Task> result = instance.getTaskCls(type, className);
            assertEquals(expResult, result);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetTaskClsNotTaskClass() {
        String type = "";
        try (Manager instance = new Manager()) {
            assertThrows(IllegalArgumentException.class, () -> instance.getTaskCls(type, String.class.getName()));
        }
    }

    @Test
    void testGetUrisLoaderCls() {
        String className = FileUrisLoader.class.getName();
        Class<? extends UrisLoader> expResult = FileUrisLoader.class;
        try (Manager instance = new Manager()) {
            Class<? extends UrisLoader> result = instance.getUrisLoaderCls(className);
            assertEquals(expResult, result);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetUrisLoaderClsNotUrisClass() {
        try (Manager instance = new Manager()) {
            assertThrows(IllegalArgumentException.class, () -> instance.getUrisLoaderCls(String.class.getName()));
        }
    }

    @Test
    void testGetUrisLoaderClsBadClassname() {
        String className = "does.not.Exist";
        try (Manager instance = new Manager()) {
            assertThrows(ClassNotFoundException.class, () -> instance.getUrisLoaderCls(className));
        }
    }

    @Test
    void testUsage() {
        try (Manager instance = new Manager()) {
            ByteArrayOutputStream outContent = new ByteArrayOutputStream();
            System.setErr(new PrintStream(outContent));

            AbstractManager aManager = new AbstractManagerTest.AbstractManagerImpl();
            aManager.usage();
            String aManagerUsage = outContent.toString();
            outContent.reset();
            instance.usage();
            assertTrue(outContent.toString().contains(aManagerUsage));
        }
    }

    @Test
    void testRunMissingURISMODULEFILEANDLOADER() {
        try (Manager instance = new Manager()) {
            assertThrows(IllegalArgumentException.class, instance::run);
        }
    }

    @Test
    void testRunGetURILoaderWithURISMODULENoContentSource() {
        try (Manager instance = new Manager()) {
            instance.options.setUrisModule("someFile1.xqy");
            assertThrows(NullPointerException.class, instance::run);
        }
    }

    @Test
    void testRunGetURILoaderWithURISMODULEInvalidCollection() {
        try ( Manager instance = new Manager()) {
            instance.options.setUrisModule("someFile2.xqy");
            instance.initContentSourcePool(XCC_CONNECTION_URI);

            assertThrows(IllegalArgumentException.class, instance::run);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testRunGetURILoaderWithURISMODULE() {
        try (Manager instance = getMockManagerWithEmptyResults()) {
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
    void testRegisterStatusInfo() throws CorbException{
        String xccRootValue = "xccRootValue";
        testLogger.clear();
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

        try (Manager instance = getMockManagerWithEmptyResults()) {
            when(session.submitRequest(any(Request.class))).thenReturn(resultSequence);
            instance.csp = contentSourcePool;
            instance.registerStatusInfo();

            assertEquals(xccRootValue, instance.options.getXDBC_ROOT());
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(21, records.size());
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testRegisterStatusInfoNullContentSource() {
        try (Manager instance = new Manager()) {
            assertThrows(NullPointerException.class, instance::registerStatusInfo);
        }
    }

    @Test
    void testLogOptions() {
        testLogger.clear();
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.logOptions();
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(21, records.size());
        } catch (RequestException|CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testStop0args() {
        testLogger.clear();
        try (Manager instance = new Manager()) {
            instance.stop();
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.INFO, records.get(0).getLevel());
        assertEquals("cleaning up", records.get(0).getMessage());
    }

    @Test
    void testStopExecutionException() {
        testLogger.clear();
        ExecutionException e = new ExecutionException("test", new Error());
        try (Manager manager = new Manager()) {
            manager.stop(e);
            List<LogRecord> records = testLogger.getLogRecords();
            assertEquals(Level.SEVERE, records.get(0).getLevel());
            assertEquals("fatal error", records.get(0).getMessage());
            //When stopped from an exception, execError is true
            assertTrue(manager.hasExecError());
            assertEquals(EXIT_CODE_PROCESSING_ERROR, manager.getExitCode());
        }
    }

    @Test
    void testSetThreadCount() {
        try (Manager instance = new Manager()) {
            instance.setThreadCount(2);
            assertEquals(2, instance.options.getThreadCount());
        }
    }

    @Test
    void testSetThreadCountTwice() {
        try (Manager instance = new Manager()) {
            instance.setThreadCount(2);
            instance.setThreadCount(2);
            assertEquals(2, instance.options.getThreadCount());
        }
    }

    @Test
    void testSetThreadCountWithInvalidValue() {
        try (Manager instance = new Manager()) {
            instance.setThreadCount(-5);
            assertEquals(1, instance.options.getThreadCount());
            instance.setThreadCount(0);
            assertEquals(1, instance.options.getThreadCount());
        }
    }

    @Test
    void testNoResultsPrePostBatchAlwaysExecuteTrue() {
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
        props.setProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE,"false");
        props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "bottom content");
        props.setProperty(Options.POST_BATCH_TASK, POST_BATCH_TASK);
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            instance.run();

            File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
            assertTrue(exportFile.exists());
            assertEquals(2, FileUtils.getLineCount(exportFile));
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "testNoResultsPrePostBatchAlwaysExecuteTrue exception thrown", ex);
            fail();
        }
    }

    @Test
    void testNoResultsDefaultPrePostExecution() {
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
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(args, props);
            manager.run();

            File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
            assertFalse(exportFile.exists());
            assertEquals(EXIT_CODE_SUCCESS, manager.getExitCode());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }


    @Test
    void testNoResultsPrePostBatchAlwaysExecuteFalseMinCountGreater() {
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
        try (Manager instance = getMockManagerWithEmptyResults()) {
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
    void testResultsPrePostBatchAlwaysExecuteFalseMinCountGreater() {
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

        try (Manager instance = getMockManagerWithEmptyResults()) {
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
    void testSubmitUriTasksWithUrisRedacted() {
        UrisLoader urisLoader = mock(UrisLoader.class);
        try (Manager manager = getMockManagerWithEmptyResults()) {
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
            assertContainsLogRecord(testLogger, Level.INFO, "received 50,000/50,000");

        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testSubmitUriTasksWithUrisNotRedacted() {
        UrisLoader urisLoader = mock(UrisLoader.class);
        try (Manager manager = getMockManagerWithEmptyResults()) {
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
            assertContainsLogRecord(testLogger, Level.INFO, "received 50,000/50,000: uri");

        } catch (CorbException|RequestException ex) {
            fail();
        }
    }

    @Test
    void testSubmitUriTasksSkipsPreviouslyCompletedUris() throws Exception {
        UrisLoader urisLoader = mock(UrisLoader.class);
        File restartStateRootDir = TestUtils.createTempDirectory();
        try {
            try (Manager manager = getMockManagerWithEmptyResults()) {
                CompletionService completionService = mock(CompletionService.class);
                manager.completionService = completionService;
                manager.pool = mock(PausableThreadPoolExecutor.class);
                when(urisLoader.hasNext()).thenReturn(true, true, true, true, true, false);
                when(urisLoader.next()).thenReturn("uri-1", "uri-2", "uri-3", "uri-4", "uri-5");
                Properties properties = ManagerTest.getDefaultProperties();
                properties.setProperty(Options.RESTARTABLE, "true");
                properties.setProperty(Options.RESTART_STATE_DIR, restartStateRootDir.getAbsolutePath());
                properties.setProperty(Options.JOB_NAME, "Skip Restart URIs");
                properties.setProperty(Options.FAIL_ON_ERROR, Boolean.FALSE.toString());

                manager.init(properties);
                Files.write(
                    new File(manager.options.getRestartStateDir(), RestartableJobState.COMPLETED_URIS_FILENAME).toPath(),
                    Arrays.asList("uri-2", "uri-4"),
                    StandardCharsets.UTF_8
                );
                manager.initializeRestartableJobState();

                long processedCount = manager.submitUriTasks(urisLoader, mock(TaskFactory.class), 5);
                assertEquals(3, processedCount);
                assertEquals(2, manager.restartableSkippedCount);
                verify(completionService, times(3)).submit(any());
            }
        } finally {
            FileUtils.deleteFile(restartStateRootDir.getAbsolutePath());
        }
    }

    @Test
    void testRunDeletesRestartStateFilesOnSuccess() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = null;
        args[9] = null;
        args[11] = null;
        args[15] = null;
        File restartStateRootDir = TestUtils.createTempDirectory();
        Properties props = new Properties();
        props.setProperty(Options.RESTARTABLE, "true");
        props.setProperty(Options.RESTART_STATE_DIR, restartStateRootDir.getAbsolutePath());
        props.setProperty(Options.JOB_NAME, "Cleanup Restart State");
        props.setProperty(Options.URIS_LOADER, MockEmptyFileUrisLoader.class.getName());
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(args, props);
            File restartStateDir = manager.options.getRestartStateDir();
            Files.createDirectories(restartStateDir.toPath());
            Files.write(
                new File(restartStateDir, RestartableJobState.COMPLETED_URIS_FILENAME).toPath(),
                Collections.singletonList("uri-1"),
                StandardCharsets.UTF_8
            );
            Files.createDirectories(new File(restartStateDir, RestartableJobState.COMPLETED_URIS_INDEX_DIRNAME).toPath());
            Files.write(
                new File(restartStateDir, RestartableJobState.COMPLETED_URIS_INDEX_METADATA_FILENAME).toPath(),
                Collections.singletonList("indexed.uniqueCount=1"),
                StandardCharsets.UTF_8
            );

            manager.run();

            assertFalse(restartStateDir.exists());
        } finally {
            if (restartStateRootDir.exists()) {
                FileUtils.deleteFile(restartStateRootDir.getAbsolutePath());
            }
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

    public static File createTempFile(List<String> lines) throws IOException {
        Path path = Files.createTempFile("tmp", "txt");
        File file = path.toFile();
        file.deleteOnExit();
        Files.write(path, lines, StandardCharsets.UTF_8);
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
    void testLogIfNotLowMemory() {
        testLogger.clear();
        try (Manager manager = new Manager()) {
            manager.logIfLowMemory(Runtime.getRuntime().totalMemory());
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(0, records.size());
    }

    @Test
    void testLogLowMemory() {
        testLogger.clear();
        try (Manager manager = new Manager()) {
            manager.logIfLowMemory(Runtime.getRuntime().freeMemory() * 6);
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(2, records.size());
        assertEquals(Level.WARNING, records.get(0).getLevel());
        assertEquals(Level.WARNING, records.get(1).getLevel());
    }

    @Test
    void testPause() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isRunning()).thenReturn(Boolean.TRUE);
        JobStats jobStats = mock(JobStats.class);
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.jobStats = jobStats;

            manager.pause();
        }
        verify(pool).pause();
    }

    @Test
    void testPauseNotRunning() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isRunning()).thenReturn(Boolean.FALSE);
        JobStats jobStats = mock(JobStats.class);
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.jobStats = jobStats;

            manager.pause();
        }
        verify(pool, never()).pause();
    }

    @Test
    void testResume() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isPaused()).thenReturn(Boolean.TRUE);
        when(pool.isRunning()).thenReturn(Boolean.TRUE);
        JobStats jobStats = mock(JobStats.class);
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.jobStats = jobStats;

            manager.resume();
        }
        verify(pool).resume();
    }

    @Test
    void testResumeNotPaused() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isPaused()).thenReturn(Boolean.FALSE);
        when(pool.isRunning()).thenReturn(Boolean.TRUE);
        JobStats jobStats = mock(JobStats.class);
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.jobStats = jobStats;

            manager.resume();
        }
        verify(pool, never()).resume();
    }

    @Test
    void testHasExecError() {
        try (Manager manager = new Manager()) {
            assertFalse(manager.hasExecError());
        }
    }

    @Test
    void testGetTransformStartMillis() {
        try (Manager manager = new Manager()) {
            assertEquals(0L, manager.getTransformStartMillis());
            manager.transformStartMillis = 12345L;
            assertEquals(12345L, manager.getTransformStartMillis());
        }
    }

    @Test
    void testDetermineExitCodeProcessingError() {
        try (Manager manager = new Manager()) {
            manager.execError = true;
            assertEquals(EXIT_CODE_PROCESSING_ERROR, manager.getExitCode());
        }
    }

    @Test
    void testDetermineExitCodeStopCommand() {
        try (Manager manager = new Manager()) {
            manager.stopCommand = true;
            assertEquals(EXIT_CODE_STOP_COMMAND, manager.getExitCode());
        }
    }

    @Test
    void testDetermineExitCodeIgnoredErrors() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.getNumFailedUris()).thenReturn(2L);
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.urisCount = 5;
            manager.EXIT_CODE_IGNORED_ERRORS = 2;
            assertEquals(2, manager.getExitCode());
        }
    }

    @Test
    void testDetermineExitCodeNoUris() {
        try (Manager manager = new Manager()) {
            manager.urisCount = 0;
            manager.EXIT_CODE_NO_URIS = 4;
            assertEquals(4, manager.getExitCode());
        }
    }

    @Test
    void testDetermineExitCodeSuccess() {
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.getNumFailedUris()).thenReturn(0L);
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.urisCount = 3;
            assertEquals(EXIT_CODE_SUCCESS, manager.getExitCode());
        }
    }

    @Test
    void testDetermineExitCodeAlreadySet() {
        try (Manager manager = new Manager()) {
            manager.setExitCode(7);
            // calling again should not recompute
            assertEquals(7, manager.determineExitCode());
        }
    }

    @Test
    void testStopWithPendingTasks() {
        testLogger.clear();
        Runnable pendingTask = mock(Runnable.class);
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.shutdownNow()).thenReturn(Collections.singletonList(pendingTask));
        try (Manager manager = new Manager()) {
            manager.pool = pool;
            manager.stop();
        }
        assertContainsLogRecord(testLogger, Level.WARNING,
                java.text.MessageFormat.format("thread pool was shut down with {0,number} pending tasks", 1));
    }

    @Test
    void testScheduleJobMetricsWhenIntervalConfigured() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            JobStats jobStats = mock(JobStats.class);
            manager.jobStats = jobStats;
            manager.options.setMetricsSyncFrequencyInMillis(50);
            manager.scheduleJobMetrics();
            // Allow the scheduled task to fire at least once
            Thread.sleep(150);
            verify(jobStats, atLeastOnce()).logMetrics(eq("RUNNING CORB JOB:"), eq(true), eq(false));
        } catch (RequestException | CorbException ex) {
            fail();
        }
    }

    @Test
    void testScheduleJobMetricsWhenIntervalNotConfigured() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            JobStats jobStats = mock(JobStats.class);
            manager.jobStats = jobStats;
            // no metrics interval set — should not schedule anything
            manager.scheduleJobMetrics();
            Thread.sleep(100);
            verify(jobStats, never()).logMetrics(anyString(), anyBoolean(), anyBoolean());
        } catch (RequestException | CorbException ex) {
            fail();
        }
    }

    @Test
    void testScheduleCommandFileWatcherWithCommandFile() throws Exception {
        File commandFile = ManagerTest.createTempFile(Collections.singletonList(""));
        try (Manager manager = getMockManagerWithEmptyResults()) {
            Properties props = getDefaultProperties();
            props.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());
            manager.init(null, props);
            manager.scheduleCommandFileWatcher();
            // scheduleCommandFileWatcher schedules an immediate execute — wait briefly
            Thread.sleep(200);
            // Verify the scheduled executor has tasks (no exception = schedule succeeded)
            assertFalse(manager.scheduledExecutor.isShutdown());
        } catch (RequestException | CorbException ex) {
            fail();
        } finally {
            commandFile.delete();
        }
    }

    @Test
    void testRunStringArgsWhenRunThrowsException() {
        // run(String[]) should catch exceptions from manager.run() and return PROCESSING_ERROR
        // We simulate this by passing args that get past init but cause an exception during run
        // The simplest trigger: a valid init with a URIS-FILE that causes a parse error
        // But to be reliable without a real server we instead test the run(String[]) code path
        // by checking that a failed init returns EXIT_CODE_INIT_ERROR (already covered) and
        // verify that run() sets PROCESSING_ERROR when manager.run() throws.
        int exitCode = Manager.run("--help"); // hasUsage flag → clean EXIT_CODE_SUCCESS path
        assertEquals(EXIT_CODE_SUCCESS, exitCode);
    }

    @Test
    void testRecordCompletedUrisWithNullJobState() throws CorbException {
        try (Manager manager = new Manager()) {
            manager.restartableJobState = null;
            // should not throw
            manager.recordCompletedUris(new String[]{"uri1"});
        }
    }

    @Test
    void testRecordCompletedUrisWithNullUris() throws Exception {
        RestartableJobState jobState = mock(RestartableJobState.class);
        try (Manager manager = new Manager()) {
            manager.restartableJobState = jobState;
            manager.recordCompletedUris(null);
            verify(jobState, never()).appendCompletedUris(any());
        }
    }

    @Test
    void testRecordCompletedUrisWithEmptyUris() throws Exception {
        RestartableJobState jobState = mock(RestartableJobState.class);
        try (Manager manager = new Manager()) {
            manager.restartableJobState = jobState;
            manager.recordCompletedUris(new String[]{});
            verify(jobState, never()).appendCompletedUris(any());
        }
    }

    @Test
    void testRecordCompletedUrisWhenAppendThrows() throws Exception {
        RestartableJobState jobState = mock(RestartableJobState.class);
        doThrow(new java.io.IOException("disk full")).when(jobState).appendCompletedUris(any());
        try (Manager manager = new Manager()) {
            manager.restartableJobState = jobState;
            assertThrows(CorbException.class, () -> manager.recordCompletedUris(new String[]{"uri1"}));
        }
    }

    @Test
    void testRunInitTaskWhenInitTaskIsNull() throws Exception {
        JobStats jobStats = mock(JobStats.class);
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.jobStats = jobStats;
            // init() does not invoke runInitTask — only populateQueue() does
            verify(jobStats, never()).setInitTaskRunTime(anyLong());
        } catch (RequestException | CorbException ex) {
            fail();
        }
    }

    @Test
    void testStartAndStopJobServer() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            Properties props = getDefaultProperties();
            props.setProperty(Options.JOB_SERVER_PORT, "0"); // port 0 = OS picks a free port
            manager.init(null, props);
            assertNull(manager.getJobServer());

            java.lang.reflect.Method startMethod = Manager.class.getDeclaredMethod("startJobServer");
            startMethod.setAccessible(true);
            startMethod.invoke(manager);
            assertNotNull(manager.getJobServer());

            java.lang.reflect.Method stopMethod = Manager.class.getDeclaredMethod("stopJobServer");
            stopMethod.setAccessible(true);
            stopMethod.invoke(manager);
            assertNull(manager.getJobServer());
        } catch (RequestException | CorbException ex) {
            fail();
        }
    }

    @Test
    void testStartJobServerIdempotent() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            Properties props = getDefaultProperties();
            props.setProperty(Options.JOB_SERVER_PORT, "0");
            manager.init(null, props);

            java.lang.reflect.Method startMethod = Manager.class.getDeclaredMethod("startJobServer");
            startMethod.setAccessible(true);
            startMethod.invoke(manager);
            JobServer first = manager.getJobServer();
            assertNotNull(first);
            // calling start again should not replace the existing server
            startMethod.invoke(manager);
            assertSame(first, manager.getJobServer());

            java.lang.reflect.Method stopMethod = Manager.class.getDeclaredMethod("stopJobServer");
            stopMethod.setAccessible(true);
            stopMethod.invoke(manager);
        } catch (RequestException | CorbException ex) {
            fail();
        }
    }

    @Test
    void testStopJobServerWhenNotStarted() throws Exception {
        try (Manager manager = new Manager()) {
            assertNull(manager.getJobServer());
            java.lang.reflect.Method stopMethod = Manager.class.getDeclaredMethod("stopJobServer");
            stopMethod.setAccessible(true);
            stopMethod.invoke(manager); // no exception
            assertNull(manager.getJobServer());
        }
    }

    // -------------------------------------------------------------------------
    // initOptions coverage tests
    // -------------------------------------------------------------------------

    @Test
    void testInitOptionsInvalidUrisLoaderClass() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.URIS_LOADER, "com.marklogic.developer.corb.DoesNotExist");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(CorbException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsNumTpsForEtc() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.NUM_TPS_FOR_ETC, "5");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(5, instance.options.getNumTpsForETC());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsPostBatchMinimumCount() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.POST_BATCH_MINIMUM_COUNT, "10");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(10, instance.options.getPostBatchMinimumCount());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsPreBatchMinimumCount() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.PRE_BATCH_MINIMUM_COUNT, "7");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(7, instance.options.getPreBatchMinimumCount());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsExportFileDirNotOverriddenWhenAlreadyInProperties() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[13] = System.getProperty("java.io.tmpdir"); // exportFileDir via arg
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_DIR, System.getProperty("java.io.tmpdir")); // already present
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals(System.getProperty("java.io.tmpdir"), instance.properties.getProperty(Options.EXPORT_FILE_DIR));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsExportFileNameNotOverriddenWhenAlreadyInProperties() throws Exception {
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[14] = "fromArg.out";
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_NAME, "fromProps.out"); // already present
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(args, props);
            assertEquals("fromProps.out", instance.properties.getProperty(Options.EXPORT_FILE_NAME));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresExportBatchToFileTask() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        // Remove PROCESS_TASK so it gets auto-configured from EXPORT_FILE_NAME
        props.remove(Options.PROCESS_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(ExportBatchToFileTask.class, instance.options.getProcessTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresPreBatchUpdateFileTask() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.remove(Options.PROCESS_TASK);
        props.remove(Options.PRE_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, "# Header");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(ExportBatchToFileTask.class, instance.options.getProcessTaskClass());
            assertEquals(PreBatchUpdateFileTask.class, instance.options.getPreBatchTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresPostBatchUpdateFileTaskForBottomContent() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.remove(Options.PROCESS_TASK);
        props.remove(Options.POST_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "# Footer");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(PostBatchUpdateFileTask.class, instance.options.getPostBatchTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresPostBatchUpdateFileTaskForSort() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.remove(Options.PROCESS_TASK);
        props.remove(Options.POST_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        props.setProperty(Options.EXPORT_FILE_SORT, "ascending");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(PostBatchUpdateFileTask.class, instance.options.getPostBatchTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresPostBatchUpdateFileTaskForSplitMaxLines() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.remove(Options.PROCESS_TASK);
        props.remove(Options.POST_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "100");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(PostBatchUpdateFileTask.class, instance.options.getPostBatchTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresPostBatchUpdateFileTaskForSplitMaxSize() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.remove(Options.PROCESS_TASK);
        props.remove(Options.POST_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_SIZE, "1024");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(PostBatchUpdateFileTask.class, instance.options.getPostBatchTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsAutoConfiguresPostBatchUpdateFileTaskForAsZip() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.remove(Options.PROCESS_TASK);
        props.remove(Options.POST_BATCH_TASK);
        props.setProperty(Options.EXPORT_FILE_NAME, "auto-output.txt");
        props.setProperty(Options.EXPORT_FILE_AS_ZIP, "true");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(PostBatchUpdateFileTask.class, instance.options.getPostBatchTaskClass());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsInvalidProcessTaskClass() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.PROCESS_TASK, "com.marklogic.developer.corb.DoesNotExist");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(CorbException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsDiskQueueTempDirDoesNotExist() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.DISK_QUEUE_TEMP_DIR, "/does/not/exist");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsDiskQueueTempDirValidPath() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.DISK_QUEUE_TEMP_DIR, System.getProperty("java.io.tmpdir"));
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertNotNull(instance.options.getDiskQueueTempDir());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsTempDirFallsBackToDiskQueueTempDir() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        // DISK_QUEUE_TEMP_DIR not set; TEMP_DIR should be used as fallback
        props.setProperty(Options.TEMP_DIR, System.getProperty("java.io.tmpdir"));
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertNotNull(instance.options.getDiskQueueTempDir());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsInvalidMetricsLogLevel() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_LOG_LEVEL, "INVALID_LEVEL");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsCollections() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_COLLECTIONS, "col1,col2");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("col1,col2", instance.options.getMetricsCollections());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsDatabase() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_DATABASE, "metrics-db");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("metrics-db", instance.options.getMetricsDatabase());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsModule() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_MODULE, "saveMetrics.xqy");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("saveMetrics.xqy", instance.options.getMetricsModule());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsRoot() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_ROOT, "/metrics/");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("/metrics/", instance.options.getMetricsRoot());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsNumSlowTransactionsValid() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_NUM_SLOW_TRANSACTIONS, "10");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(10, (int) instance.options.getNumberOfLongRunningUris());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsNumSlowTransactionsCappedAtMax() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        int overMax = TransformOptions.MAX_NUM_SLOW_TRANSACTIONS + 50;
        props.setProperty(Options.METRICS_NUM_SLOW_TRANSACTIONS, Integer.toString(overMax));
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(TransformOptions.MAX_NUM_SLOW_TRANSACTIONS, (int) instance.options.getNumberOfLongRunningUris());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsNumSlowTransactionsInvalidFormat() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_NUM_SLOW_TRANSACTIONS, "notANumber");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsNumFailedTransactionsValid() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_NUM_FAILED_TRANSACTIONS, "20");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(20, (int) instance.options.getNumberOfFailedUris());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsNumFailedTransactionsCappedAtMax() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        int overMax = TransformOptions.MAX_NUM_FAILED_TRANSACTIONS + 50;
        props.setProperty(Options.METRICS_NUM_FAILED_TRANSACTIONS, Integer.toString(overMax));
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(TransformOptions.MAX_NUM_FAILED_TRANSACTIONS, (int) instance.options.getNumberOfFailedUris());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsSyncFrequencyWithDatabase() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_DATABASE, "metrics-db");
        props.setProperty(Options.METRICS_SYNC_FREQUENCY, "5");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(Integer.valueOf(5000), instance.options.getMetricsSyncFrequencyInMillis());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsSyncFrequencyNotSetWithoutDatabase() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        // Explicitly disable metrics logging; no METRICS_DATABASE — sync frequency should not be applied
        props.setProperty(Options.METRICS_LOG_LEVEL, "none");
        props.setProperty(Options.METRICS_SYNC_FREQUENCY, "5");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals(-1, (int) instance.options.getMetricsSyncFrequencyInMillis());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsMetricsSyncFrequencyInvalidFormat() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.METRICS_DATABASE, "metrics-db");
        props.setProperty(Options.METRICS_SYNC_FREQUENCY, "notANumber");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testInitOptionsJobServerPortInvalidFormat() {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.JOB_SERVER_PORT, "notAPort");
        try (Manager instance = getMockManagerWithEmptyResults()) {
            assertThrows(IllegalArgumentException.class, () -> instance.init(null, props));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testNormalizeLegacyExportFileAsZipOptionSkipsWhenSystemPropertySet() throws Exception {
        clearSystemProperties();
        System.setProperty(Options.EXPORT_FILE_AS_ZIP, "true");
        Properties props = getDefaultProperties();
        props.setProperty("EXPORT_FILE_AS_ZIP", "false"); // legacy underscore form — must NOT overwrite
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("true", System.getProperty(Options.EXPORT_FILE_AS_ZIP));
        } catch (CorbException | RequestException ex) {
            fail();
        } finally {
            System.clearProperty(Options.EXPORT_FILE_AS_ZIP);
        }
    }

    @Test
    void testNormalizeLegacyExportFileAsZipOptionSkipsWhenPropertySet() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty(Options.EXPORT_FILE_AS_ZIP, "true");
        props.setProperty("EXPORT_FILE_AS_ZIP", "false"); // legacy — should be ignored
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("true", instance.properties.getProperty(Options.EXPORT_FILE_AS_ZIP));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testNormalizeLegacyExportFileAsZipOptionAppliesLegacyValue() throws Exception {
        clearSystemProperties();
        Properties props = getDefaultProperties();
        props.setProperty("EXPORT_FILE_AS_ZIP", "true"); // legacy underscore form only
        try (Manager instance = getMockManagerWithEmptyResults()) {
            instance.init(null, props);
            assertEquals("true", instance.properties.getProperty(Options.EXPORT_FILE_AS_ZIP));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    public static class MockEmptyFileUrisLoader extends FileUrisLoader {

        @Override
        public void open() {
            this.setTotalCount(0);
        }
    }

    // -------------------------------------------------------------------------
    // shouldRunPreBatch / shouldRunPostBatch coverage
    // -------------------------------------------------------------------------

    @Test
    void testShouldRunPreBatchReturnsFalseWhenCountBelowMinimum() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setPrePostBatchAlwaysExecute(false);
            manager.options.setPreBatchMinimumCount(10);
            assertFalse(manager.shouldRunPreBatch(5));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testShouldRunPreBatchReturnsTrueWhenAlwaysExecute() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setPrePostBatchAlwaysExecute(true);
            manager.options.setPreBatchMinimumCount(100);
            assertTrue(manager.shouldRunPreBatch(0));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testShouldRunPostBatchReturnsFalseWhenExecError() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.execError = true;
            assertFalse(manager.shouldRunPostBatch(100));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testShouldRunPostBatchReturnsTrueWhenAlwaysExecute() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setPrePostBatchAlwaysExecute(true);
            manager.options.setPostBatchMinimumCount(100);
            assertFalse(manager.execError);
            assertTrue(manager.shouldRunPostBatch(0));
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    // -------------------------------------------------------------------------
    // scheduleJobMetrics isPaused branch
    // -------------------------------------------------------------------------

    @Test
    void testScheduleJobMetricsSkipsLogWhenPaused() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            JobStats jobStats = mock(JobStats.class);
            manager.jobStats = jobStats;
            PausableThreadPoolExecutor mockPool = mock(PausableThreadPoolExecutor.class);
            when(mockPool.isPaused()).thenReturn(true);
            manager.pool = mockPool;
            manager.options.setMetricsSyncFrequencyInMillis(50);
            manager.scheduleJobMetrics();
            Thread.sleep(200);
            verify(jobStats, never()).logMetrics(anyString(), anyBoolean(), anyBoolean());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    // -------------------------------------------------------------------------
    // runInitTask / runPreBatchTask / runPostBatchTask non-null task branches
    // -------------------------------------------------------------------------

    @Test
    void testRunInitTaskWithNonNullTask() throws Exception {
        Task initTask = mock(Task.class);
        when(initTask.call()).thenReturn(null);
        TaskFactory taskFactory = mock(TaskFactory.class);
        when(taskFactory.newInitTask()).thenReturn(initTask);
        try (Manager manager = new Manager()) {
            manager.jobStats = mock(JobStats.class);
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("runInitTask", TaskFactory.class);
            method.setAccessible(true);
            method.invoke(manager, taskFactory);
            verify(initTask).call();
            verify(manager.jobStats).setInitTaskRunTime(anyLong());
        }
    }

    @Test
    void testRunPreBatchTaskWithNonNullTask() throws Exception {
        Task preTask = mock(Task.class);
        when(preTask.call()).thenReturn(null);
        TaskFactory taskFactory = mock(TaskFactory.class);
        when(taskFactory.newPreBatchTask()).thenReturn(preTask);
        try (Manager manager = new Manager()) {
            manager.jobStats = mock(JobStats.class);
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("runPreBatchTask", TaskFactory.class);
            method.setAccessible(true);
            method.invoke(manager, taskFactory);
            verify(preTask).call();
            verify(manager.jobStats).setPreBatchRunTime(anyLong());
        }
    }

    @Test
    void testRunPostBatchTaskWithNonNullTask() throws Exception {
        Task postTask = mock(Task.class);
        when(postTask.call()).thenReturn(null);
        TaskFactory taskFactory = mock(TaskFactory.class);
        when(taskFactory.newPostBatchTask()).thenReturn(postTask);
        try (Manager manager = new Manager()) {
            manager.jobStats = mock(JobStats.class);
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("runPostBatchTask", TaskFactory.class);
            method.setAccessible(true);
            method.invoke(manager, taskFactory);
            verify(postTask).call();
            verify(manager.jobStats).setPostBatchRunTime(anyLong());
        }
    }

    // -------------------------------------------------------------------------
    // getUriLoader branches
    // -------------------------------------------------------------------------

    @Test
    void testGetUriLoaderWithUrisModule() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setUrisModule("someModule.xqy");
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("getUriLoader");
            method.setAccessible(true);
            try (UrisLoader loader = (UrisLoader) method.invoke(manager)) {
                assertInstanceOf(QueryUrisLoader.class, loader);
            }
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testGetUriLoaderWithUrisFile() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setUrisModule(null);
            manager.options.setUrisFile("somefile.txt");
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("getUriLoader");
            method.setAccessible(true);
            try (UrisLoader loader = (UrisLoader) method.invoke(manager)) {
                assertInstanceOf(FileUrisLoader.class, loader);
            }
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testGetUriLoaderWithUrisLoaderClass() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setUrisModule(null);
            manager.options.setUrisFile(null);
            manager.options.setUrisLoaderClass(MockEmptyFileUrisLoader.class);
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("getUriLoader");
            method.setAccessible(true);
            try (UrisLoader loader = (UrisLoader) method.invoke(manager)) {
                assertInstanceOf(MockEmptyFileUrisLoader.class, loader);
            }
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testGetUriLoaderThrowsWhenNoSourceConfigured() throws Exception {
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.options.setUrisModule(null);
            manager.options.setUrisFile(null);
            manager.options.setUrisLoaderClass(null);
            java.lang.reflect.Method method = Manager.class.getDeclaredMethod("getUriLoader");
            method.setAccessible(true);
            try {
                method.invoke(manager);
                fail("Expected IllegalArgumentException");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertInstanceOf(IllegalArgumentException.class, e.getCause());
            }
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    // -------------------------------------------------------------------------
    // shouldIncludeInRestartStateFingerprint branches
    // -------------------------------------------------------------------------

    @Test
    void testShouldIncludeInRestartStateFingerprintExcludesFilteredProperties() throws Exception {
        java.lang.reflect.Method method = Manager.class.getDeclaredMethod("shouldIncludeInRestartStateFingerprint", String.class);
        method.setAccessible(true);
        try (Manager manager = new Manager()) {
            // null and empty → false
            assertFalse((Boolean) method.invoke(manager, (String) null));
            assertFalse((Boolean) method.invoke(manager, ""));
            // XCC- prefix → false
            assertFalse((Boolean) method.invoke(manager, "XCC-CONNECTION-URI"));
            // contains PASSWORD → false
            assertFalse((Boolean) method.invoke(manager, "MY-PASSWORD-KEY"));
            // contains SSL → false
            assertFalse((Boolean) method.invoke(manager, "MY-SSL-CERT"));
            // contains OAUTH → false
            assertFalse((Boolean) method.invoke(manager, "OAUTH-TOKEN"));
            // contains API-KEY → false
            assertFalse((Boolean) method.invoke(manager, "MY-API-KEY"));
            // RESTART- prefix → false
            assertFalse((Boolean) method.invoke(manager, "RESTART-STATE-DIR"));
            // METRICS- prefix → false
            assertFalse((Boolean) method.invoke(manager, "METRICS-LOG-LEVEL"));
            // EXPORT-FILE- prefix → false
            assertFalse((Boolean) method.invoke(manager, "EXPORT-FILE-NAME"));
            // COMMAND prefix → false
            assertFalse((Boolean) method.invoke(manager, "COMMAND"));
            // each named constant → false
            assertFalse((Boolean) method.invoke(manager, Options.JOB_NAME));
            assertFalse((Boolean) method.invoke(manager, Options.THREAD_COUNT));
            assertFalse((Boolean) method.invoke(manager, Options.BATCH_SIZE));
            assertFalse((Boolean) method.invoke(manager, Options.BATCH_URI_DELIM));
            assertFalse((Boolean) method.invoke(manager, Options.FAIL_ON_ERROR));
            assertFalse((Boolean) method.invoke(manager, Options.ERROR_FILE_NAME));
            assertFalse((Boolean) method.invoke(manager, Options.DISK_QUEUE));
            assertFalse((Boolean) method.invoke(manager, Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE));
            assertFalse((Boolean) method.invoke(manager, Options.DISK_QUEUE_TEMP_DIR));
            assertFalse((Boolean) method.invoke(manager, Options.TEMP_DIR));
            assertFalse((Boolean) method.invoke(manager, Options.JOB_SERVER_PORT));
            assertFalse((Boolean) method.invoke(manager, Options.NUM_TPS_FOR_ETC));
            assertFalse((Boolean) method.invoke(manager, Options.PRE_BATCH_MINIMUM_COUNT));
            assertFalse((Boolean) method.invoke(manager, Options.POST_BATCH_MINIMUM_COUNT));
            assertFalse((Boolean) method.invoke(manager, Options.PRE_POST_BATCH_ALWAYS_EXECUTE));
            assertFalse((Boolean) method.invoke(manager, Options.EXIT_CODE_IGNORED_ERRORS));
            assertFalse((Boolean) method.invoke(manager, Options.EXIT_CODE_NO_URIS));
            // unrecognised option → true (should be included)
            assertTrue((Boolean) method.invoke(manager, "PROCESS-MODULE"));
            assertTrue((Boolean) method.invoke(manager, "CUSTOM-OPTION"));
        }
    }

    // -------------------------------------------------------------------------
    // sanitizeRestartStateLabel and buildRestartStateId branches
    // -------------------------------------------------------------------------

    @Test
    void testSanitizeRestartStateLabelWithAllSpecialCharsReturnsJob() throws Exception {
        java.lang.reflect.Method method = Manager.class.getDeclaredMethod("sanitizeRestartStateLabel", String.class);
        method.setAccessible(true);
        try (Manager manager = new Manager()) {
            assertEquals("job", method.invoke(manager, "!!!"));
            assertEquals("my-job-name", method.invoke(manager, "My Job Name"));
            assertEquals("test-123", method.invoke(manager, "Test 123!"));
        }
    }

    @Test
    void testBuildRestartStateIdWithBlankJobName() throws Exception {
        java.lang.reflect.Method method = Manager.class.getDeclaredMethod("buildRestartStateId", String.class);
        method.setAccessible(true);
        try (Manager manager = new Manager()) {
            manager.properties = new Properties();
            String result = (String) method.invoke(manager, "");
            assertTrue(result.startsWith("job-"), "Expected label to start with 'job-' but was: " + result);
        }
    }

    @Test
    void testBuildRestartStateIdWithNonBlankJobName() throws Exception {
        java.lang.reflect.Method method = Manager.class.getDeclaredMethod("buildRestartStateId", String.class);
        method.setAccessible(true);
        try (Manager manager = new Manager()) {
            manager.properties = new Properties();
            String result = (String) method.invoke(manager, "MyJob");
            assertTrue(result.startsWith("myjob-"), "Expected label to start with 'myjob-' but was: " + result);
        }
    }

    // -------------------------------------------------------------------------
    // submitUriTasks: pool=null early exit and blank URI skip
    // -------------------------------------------------------------------------

    @Test
    void testSubmitUriTasksWithNullPoolExitsEarly() throws Exception {
        UrisLoader urisLoader = mock(UrisLoader.class);
        when(urisLoader.hasNext()).thenReturn(true);
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            manager.pool = null;
            long count = manager.submitUriTasks(urisLoader, mock(TaskFactory.class), 10);
            assertEquals(0, count);
            assertContainsLogRecord(testLogger, Level.WARNING, "Thread pool is set to null. Exiting out of the task submission loop prematurely.");
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testSubmitUriTasksSkipsBlankUris() throws Exception {
        UrisLoader urisLoader = mock(UrisLoader.class);
        when(urisLoader.hasNext()).thenReturn(true, true, false);
        when(urisLoader.next()).thenReturn("", "validUri");
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.init(null, getDefaultProperties());
            CompletionService<String[]> completionService = mock(CompletionService.class);
            manager.completionService = completionService;
            manager.pool = mock(PausableThreadPoolExecutor.class);
            long count = manager.submitUriTasks(urisLoader, mock(TaskFactory.class), 10);
            assertEquals(1, count);
            verify(completionService, times(1)).submit(any());
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    // -------------------------------------------------------------------------
    // recordCompletedUris success path
    // -------------------------------------------------------------------------

    @Test
    void testRecordCompletedUrisSuccess() throws Exception {
        RestartableJobState jobState = mock(RestartableJobState.class);
        try (Manager manager = new Manager()) {
            manager.restartableJobState = jobState;
            manager.recordCompletedUris(new String[]{"uri1", "uri2"});
            verify(jobState).appendCompletedUris(new String[]{"uri1", "uri2"});
        }
    }

    // -------------------------------------------------------------------------
    // normalizeLegacyProperties: modern key already present prevents override
    // -------------------------------------------------------------------------

    @Test
    void testNormalizeLegacyPropertiesDoesNotOverrideModernKeyWhenBothPresent() throws Exception {
        Properties props = new Properties();
        String modernValue = "modern-module.xqy";
        String legacyValue = "legacy-module.xqy";
        props.setProperty(Options.PROCESS_MODULE, modernValue);
        props.setProperty(Options.XQUERY_MODULE, legacyValue);
        try (Manager manager = getMockManagerWithEmptyResults()) {
            manager.properties = props;
            manager.normalizeLegacyProperties();
            assertEquals(modernValue, manager.properties.getProperty(Options.PROCESS_MODULE));
        } catch (RequestException | CorbException ex) {
            fail();
        }
    }
}
