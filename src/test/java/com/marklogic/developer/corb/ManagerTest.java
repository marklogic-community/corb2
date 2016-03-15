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
import static com.marklogic.developer.corb.TestUtils.clearFile;
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
import com.marklogic.xcc.types.XdmItem;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
 * @generatedBy CodePro at 9/18/15 10:51 AM
 * @author matthew.heckel
 * @version $Revision: 1.0 $
 */
public class ManagerTest {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    private final TestHandler testLogger = new TestHandler();
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

    private static final String PROCESS_MODULE = "src/test/resources/transform2.xqy|ADHOC";

    /**
     * Functional test for the Manager using program arguments.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagerUsingProgArgs()
            throws Exception {
        clearSystemProperties();
        String exportFileName = "testManagerUsingProgArgs.txt";
        String exportFileDir = EXPORT_FILE_DIR;
        String[] args = getDefaultArgs();
        args[14] = exportFileName;
        args[15] = null;
        File report = new File(exportFileDir + "/" + exportFileName);
        report.deleteOnExit();

        //First, verify the output using run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();

        System.out.println("testManagerUsingProgArgs,corbOutput=" + corbOutput);
        String expectedOutput = "This is being returned from the PRE-BATCH-MODULE which is often used for column headers.\nThis is a file generated by the XQUERY-MODULE (Transform) which typically contains a report.  This information [The Selector sends its greetings!  The COLLECTION-NAME is StringPassedToTheURIsModule] was passed from the Selector.\nThis is from the POST-BATCH-MODULE using the POST-XQUERY-MODULE.";
        boolean passed = expectedOutput.equals(corbOutput);
        clearFile(report);
        assertTrue(passed);

        //Then verify the exit code when invoking the main()
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager using system properties.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagerUsingSysProps()
            throws Exception {
        clearSystemProperties();
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("COLLECTION-NAME", COLLECTION_NAME);
        System.setProperty("XQUERY-MODULE", XQUERY_MODULE);
        System.setProperty("THREAD-COUNT", THREAD_COUNT);
        System.setProperty("URIS-MODULE", "src/test/resources/selector.xqy|ADHOC");
        System.setProperty("MODULE-ROOT", MODULES_ROOT);
        System.setProperty("MODULES-DATABASE", MODULES_DATABASE);
        System.setProperty("INSTALL", "false");
        System.setProperty("PROCESS-TASK", PROCESS_TASK);
        System.setProperty("PRE-BATCH-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-TASK", PRE_BATCH_TASK);
        System.setProperty("POST-BATCH-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-TASK", POST_BATCH_TASK);
        System.setProperty("EXPORT-FILE-DIR", EXPORT_FILE_DIR);
        System.setProperty("EXPORT-FILE-NAME", "testManagerUsingSysProps.txt");

        String[] args = {};
        //First, verify the output by executing run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();
        File report = new File(EXPORT_FILE_DIR + "/testManagerUsingSysProps.txt");
        report.deleteOnExit();
        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();

        String expectedOutput = "This is being returned from the PRE-BATCH-MODULE which is often used for column headers.\nThis is a file generated by the XQUERY-MODULE (Transform) which typically contains a report.  This information [The Selector sends its greetings!  The COLLECTION-NAME is StringPassedToTheURIsModule] was passed from the Selector.\nThis is from the POST-BATCH-MODULE using the POST-XQUERY-MODULE.";
        boolean passed = expectedOutput.equals(corbOutput);
        clearFile(report);
        assertTrue(passed);

        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager using a properties file.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagerUsingPropsFile()
            throws Exception {
        String exportFileName = EXPORT_FILE_DIR + "/testManagerUsingPropsFile.txt";
        clearSystemProperties();
        System.setProperty("OPTIONS-FILE", "src/test/resources/helloWorld.properties");
        System.setProperty("EXPORT-FILE-NAME", exportFileName);

        String[] args = {};
        //First, verify the output using run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        File report = new File(exportFileName);
        report.deleteOnExit();
        boolean fileExists = report.exists();
        assertTrue(fileExists);
        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();
        System.out.println("testManagerUsingPropsFile,corbOutput=" + corbOutput);
        String expectedOutput = "This is being returned from the PRE-BATCH-MODULE which is often used for column headers.\nThis is a file generated by the XQUERY-MODULE (Transform) which typically contains a report.  This information [The Selector sends its greetings!  The COLLECTION-NAME is StringPassedToTheURIsModule] was passed from the Selector.\nThis is from the POST-BATCH-MODULE using the POST-XQUERY-MODULE.";
        boolean passed = expectedOutput.equals(corbOutput);
        clearFile(report);
        assertTrue(passed);

        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager using an input file for the URI selector.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagerUsingInputFile()
            throws Exception {
        clearSystemProperties();
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("COLLECTION-NAME", COLLECTION_NAME);
        System.setProperty("XQUERY-MODULE", XQUERY_MODULE);
        System.setProperty("THREAD-COUNT", THREAD_COUNT);
        System.setProperty("MODULE-ROOT", MODULES_ROOT);
        System.setProperty("MODULES-DATABASE", MODULES_DATABASE);
        System.setProperty("INSTALL", "false");
        System.setProperty("PROCESS-TASK", PROCESS_TASK);
        System.setProperty("PRE-BATCH-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-TASK", PRE_BATCH_TASK);
        System.setProperty("POST-BATCH-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-TASK", POST_BATCH_TASK);
        System.setProperty("EXPORT-FILE-DIR", EXPORT_FILE_DIR);
        System.setProperty("EXPORT-FILE-NAME", "testManagerUsingInputFile.txt");
        System.setProperty("URIS-FILE", "src/test/resources/uriInputFile.txt");
        String[] args = {};
        //First, verify the output using run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        String exportFilePath = EXPORT_FILE_DIR + "/testManagerUsingInputFile.txt";
        File report = new File(exportFilePath);
        report.deleteOnExit();
        boolean fileExists = report.exists();
        assertTrue(fileExists);
        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();
        System.out.println("testManagerUsingInputFile,corbOutput=" + corbOutput);
        String expectedOutput = "This is being returned from the PRE-BATCH-MODULE which is often used for column headers.\nThis is a file generated by the XQUERY-MODULE (Transform) which typically contains a report.  This information [Hello from the URIS-FILE!] was passed from the Selector.\nThis is from the POST-BATCH-MODULE using the POST-XQUERY-MODULE.";
        boolean passed = expectedOutput.equals(corbOutput);
        clearFile(report);
        assertTrue(passed);
        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager's PRE-BATCH-TASK.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagersPreBatchTask()
            throws Exception {
        clearSystemProperties();
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("COLLECTION-NAME", COLLECTION_NAME);
        System.setProperty("XQUERY-MODULE", XQUERY_MODULE);
        System.setProperty("THREAD-COUNT", THREAD_COUNT);
        System.setProperty("MODULE-ROOT", MODULES_ROOT);
        System.setProperty("MODULES-DATABASE", MODULES_DATABASE);
        System.setProperty("INSTALL", "false");
        System.setProperty("PROCESS-TASK", PROCESS_TASK);
        System.setProperty("PRE-BATCH-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-TASK", PRE_BATCH_TASK);
        System.setProperty("POST-BATCH-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-TASK", POST_BATCH_TASK);
        System.setProperty("EXPORT-FILE-DIR", EXPORT_FILE_DIR);
        System.setProperty("EXPORT-FILE-NAME", "testManagersPreBatchTask.txt");
        System.setProperty("URIS-FILE", URIS_FILE);
        String[] args = {};

        //First, verify output executing run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        String exportFilePath = EXPORT_FILE_DIR + "/testManagersPreBatchTask.txt";
        File report = new File(exportFilePath);
        report.deleteOnExit();
        boolean fileExists = report.exists();
        assertTrue(fileExists);
        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();
        System.out.println("testManagersPreBatchTask,corbOutput=" + corbOutput);
        String expectedOutput = "This is being returned from the PRE-BATCH-MODULE which is often used for column headers.";
        boolean passed = corbOutput.startsWith(expectedOutput);
        clearFile(report);
        assertTrue(passed);

        //Then, verify the exit code running main()
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager's POST-BATCH-TASK.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagersPostBatchTask()
            throws Exception {
        clearSystemProperties();
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("COLLECTION-NAME", COLLECTION_NAME);
        System.setProperty("XQUERY-MODULE", XQUERY_MODULE);
        System.setProperty("THREAD-COUNT", THREAD_COUNT);
        System.setProperty("MODULE-ROOT", MODULES_ROOT);
        System.setProperty("MODULES-DATABASE", MODULES_DATABASE);
        System.setProperty("INSTALL", "false");
        System.setProperty("PROCESS-TASK", PROCESS_TASK);
        System.setProperty("PRE-BATCH-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-TASK", PRE_BATCH_TASK);
        System.setProperty("POST-BATCH-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-TASK", POST_BATCH_TASK);
        System.setProperty("EXPORT-FILE-DIR", EXPORT_FILE_DIR);
        System.setProperty("EXPORT-FILE-NAME", "testManagersPostBatchTask.txt");
        System.setProperty("URIS-FILE", URIS_FILE);
        String[] args = {};
        //First, verify the output using run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        String exportFilePath = EXPORT_FILE_DIR + "/testManagersPostBatchTask.txt";
        File report = new File(exportFilePath);
        boolean fileExists = report.exists();
        assertTrue(fileExists);
        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();
        System.out.println("testManagersPostBatchTask,corbOutput=" + corbOutput);
        String expectedOutput = "This is from the POST-BATCH-MODULE using the POST-XQUERY-MODULE.";
        boolean passed = corbOutput.endsWith(expectedOutput);
        clearFile(report);
        assertTrue(passed);
        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager's POST-BATCH-TASK for zipping the output
     * file.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagersPostBatchTaskZip()
            throws Exception {
        clearSystemProperties();
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("COLLECTION-NAME", COLLECTION_NAME);
        System.setProperty("XQUERY-MODULE", XQUERY_MODULE);
        System.setProperty("THREAD-COUNT", THREAD_COUNT);
        System.setProperty("MODULE-ROOT", MODULES_ROOT);
        System.setProperty("MODULES-DATABASE", MODULES_DATABASE);
        System.setProperty("INSTALL", "false");
        System.setProperty("PROCESS-TASK", PROCESS_TASK);
        System.setProperty("PRE-BATCH-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-TASK", PRE_BATCH_TASK);
        System.setProperty("POST-BATCH-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-TASK", POST_BATCH_TASK);
        System.setProperty("EXPORT-FILE-DIR", EXPORT_FILE_DIR);
        System.setProperty("EXPORT-FILE-NAME", "helloWorld.txt");
        System.setProperty("URIS-FILE", URIS_FILE);
        System.setProperty("EXPORT_FILE_AS_ZIP", "true");
        String[] args = {};
        //First, verify the output using run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        String zippedExportFilePath = EXPORT_FILE_DIR + "/helloWorld.txt.zip";
        File report = new File(zippedExportFilePath);
        boolean fileExists = report.exists();
        clearFile(report);
        assertTrue(fileExists);
        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

    /**
     * Functional test for the Manager's POST-BATCH-TASK.
     *
     * @throws Exception
     *
     */
    @Test
    public void testManagerJavaScriptTransform()
            throws Exception {
        clearSystemProperties();
        System.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        System.setProperty("COLLECTION-NAME", COLLECTION_NAME);
        System.setProperty("XQUERY-MODULE", "src/test/resources/mod-print-uri.sjs|ADHOC");
        System.setProperty("THREAD-COUNT", THREAD_COUNT);
        System.setProperty("MODULE-ROOT", MODULES_ROOT);
        System.setProperty("MODULES-DATABASE", MODULES_DATABASE);
        System.setProperty("INSTALL", "false");
        System.setProperty("PROCESS-TASK", PROCESS_TASK);
        System.setProperty("PRE-BATCH-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-TASK", PRE_BATCH_TASK);
        System.setProperty("POST-BATCH-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-TASK", POST_BATCH_TASK);
        System.setProperty("EXPORT-FILE-DIR", EXPORT_FILE_DIR);
        System.setProperty("EXPORT-FILE-NAME", "testManagerJavaScriptTransform.txt");
        System.setProperty("URIS-FILE", URIS_FILE);
        System.setProperty("XQUERY-MODULE.foo", "bar1");
        String[] args = {};
        //First, verify the output using run()
        Manager manager = new Manager();
        manager.init(args);
        manager.run();

        String exportFilePath = EXPORT_FILE_DIR + "/testManagerJavaScriptTransform.txt";
        File report = new File(exportFilePath);
        report.deleteOnExit();
        boolean fileExists = report.exists();
        assertTrue(fileExists);
        byte[] out = FileUtils.getBytes(report);
        String corbOutput = new String(out).trim();
        System.out.println("testManagerJavaScriptTransform,corbOutput=" + corbOutput);
        String expectedOutput = "object-id-1=bar1";
        System.out.println(corbOutput);
        boolean passed = corbOutput.contains(expectedOutput);
        clearFile(report);
        assertTrue(passed);
        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        Manager.main(args);
    }

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
        Logger logger = Logger.getLogger(Manager.class.getName());
        logger.addHandler(testLogger);
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
        FileUtils.deleteFile(new File(ManagerTest.EXPORT_FILE_DIR));
        clearSystemProperties();
    }

    @Test(expected = NullPointerException.class)
    public void testRejectedExecution_npe() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
    }

    @Test
    public void testRejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);
        @SuppressWarnings("unchecked")
        BlockingQueue<Runnable> queue = mock(BlockingQueue.class);
        when(threadPool.getQueue()).thenReturn(queue).thenThrow(new NullPointerException());

        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
    }

    @Test(expected = MockitoException.class)
    public void testRejectedExecution_rejectedExecution() {
        Runnable r = mock(Runnable.class);
        ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);

        when(threadPool.getQueue()).thenThrow(new InterruptedException());
        threadPool.getQueue();
        RejectedExecutionHandler cbp = new Manager.CallerBlocksPolicy();
        cbp.rejectedExecution(r, threadPool);
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
     * Test of main method, of class Manager.
     */
    @Test
    public void testMain_nullArgs() {
        System.out.println("main");
        String[] args = null;
        exit.expectSystemExit();
        Manager.main(args);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("Error initializing CORB", records.get(0).getMessage());
    }

    @Test
    public void testMain_exception() {
        System.out.println("main");
        String[] args = getDefaultArgs();
        exit.expectSystemExit();
        Manager.main(args);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("Error initializing CORB", records.get(0).getMessage());
    }

    /**
     * Test of init method, of class Manager.
     */
    @Test
    public void testInit_nullArgs_properties() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty("BATCH-SIZE", "5");
        Manager instance = new Manager();
        exit.expectSystemExit();
        instance.init(args, props);
        assertEquals(props, instance.properties);
    }

    @Test
    public void testInit_blankCollection() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        //no "COLLECTION-NAME" specified
        props.setProperty("PROCESS-MODULE", "src/test/resources/mod-print-uri.sjs|ADHOC");

        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals("", instance.collection);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInit_urisFileDoesNoteExist() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = null;
        Properties props = new Properties();
        props.setProperty("XCC-CONNECTION-URI", XCC_CONNECTION_URI);
        props.setProperty("PROCESS-MODULE", "src/test/resources/mod-print-uri.sjs|ADHOC");
        props.setProperty("URIS-FILE", "does/not/exist");

        Manager instance = new Manager();
        instance.init(args, props);
    }

    @Test
    public void testInit_nullArgs_emptyProperties() throws Exception {
        System.out.println("init");
        String[] args = null;
        Properties props = new Properties();
        Manager instance = new Manager();
        exit.expectSystemExit();
        instance.init(args, props);
        assertEquals(props, instance.properties);
    }

    /**
     * Test of initOptions method, of class Manager.
     */
    @Test(expected = NullPointerException.class)
    public void testInitOptions_nullArgs() throws Exception {
        System.out.println("initOptions");
        String[] args = null;
        Manager instance = new Manager();
        instance.initOptions(args);
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions() throws Exception {
        System.out.println("initOptions");
        String[] args = {};
        Manager instance = new Manager();
        instance.initOptions(args);
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions_withEmptyProperties() throws Exception {
        System.out.println("initOptions");
        String[] args = null;
        Manager instance = new Manager();
        instance.properties = new Properties();
        instance.initOptions(args);
    }

    @Test
    public void testInitOptions_urisFileIsBlank() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = "      ";

        Properties props = new Properties();

        Manager instance = new Manager();
        instance.init(args, props);
        assertNull(instance.options.getUrisFile());
    }

    @Test
    public void testInitOptions_urisFileIsNull() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[15] = null;

        Properties props = new Properties();

        Manager instance = new Manager();
        instance.init(args, props);
        assertNull(instance.options.getUrisFile());
    }

    @Test
    public void testInitOptions_setXQUERY_MODULE_property() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null;

        Properties props = new Properties();
        props.setProperty("XQUERY-MODULE", PROCESS_MODULE);
        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
    }

    @Test
    public void testInitOptions_setPROCESS_MODULE_property() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";

        Properties props = new Properties();
        props.setProperty("PROCESS-MODULE", PROCESS_MODULE);
        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
    }

    @Test
    public void testInitOptions_missingPROCESS_MODULE() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = "";

        Properties props = new Properties();
        Manager instance = new Manager();
        instance.init(args, props);
        assertNull(instance.options.getProcessModule());
    }

    @Test
    public void testInitOptions_normalizeLegacySystemProperties() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        System.setProperty("XQUERY-MODULE", PROCESS_MODULE);
        System.setProperty("XQUERY-MODULE.foo", "process-bar");
        System.setProperty("PRE-BATCH-XQUERY-MODULE", PRE_BATCH_MODULE);
        System.setProperty("PRE-BATCH-XQUERY-MODULE.foo", "pre-bar");
        System.setProperty("POST-BATCH-XQUERY-MODULE", POST_BATCH_MODULE);
        System.setProperty("POST-BATCH-XQUERY-MODULE.foo", "post-bar");

        String[] args = getDefaultArgs();
        args[2] = null; //process-module
        args[11] = null; //pre-batch-module
        args[13] = null; //post-batch-module

        Properties props = new Properties();

        Manager instance = new Manager();
        instance.init(args, props);

        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        assertEquals("process-bar", System.getProperty("PROCESS-MODULE.foo"));
        assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
        assertEquals("pre-bar", System.getProperty("PRE-BATCH-MODULE.foo"));
        assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
        assertEquals("post-bar", System.getProperty("POST-BATCH-MODULE.foo"));
    }

    @Test
    public void testInitOptions_normalizeLegacyProperties() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[2] = null; //process-module
        args[11] = null; //pre-batch-module
        args[13] = null; //post-batch-module

        Properties props = new Properties();
        props.setProperty("XQUERY-MODULE", PROCESS_MODULE);
        props.setProperty("XQUERY-MODULE.foo", "process-bar");
        props.setProperty("PRE-BATCH-XQUERY-MODULE", PRE_BATCH_MODULE);
        props.setProperty("PRE-BATCH-XQUERY-MODULE.foo", "pre-bar");
        props.setProperty("POST-BATCH-XQUERY-MODULE", POST_BATCH_MODULE);
        props.setProperty("POST-BATCH-XQUERY-MODULE.foo", "post-bar");

        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(PROCESS_MODULE, instance.options.getProcessModule());
        assertEquals("process-bar", instance.properties.getProperty("PROCESS-MODULE.foo"));
        assertEquals(PRE_BATCH_MODULE, instance.options.getPreBatchModule());
        assertEquals("pre-bar", instance.properties.getProperty("PRE-BATCH-MODULE.foo"));
        assertEquals(POST_BATCH_MODULE, instance.options.getPostBatchModule());
        assertEquals("post-bar", instance.properties.getProperty("POST-BATCH-MODULE.foo"));
    }

    @Test(expected = NumberFormatException.class)
    public void testInitOptions_batchSize_parseError() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty("BATCH-SIZE", "one");
        Manager instance = new Manager();
        instance.init(args, props);
    }

    @Test
    public void testInitOptions_batchSize() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty("BATCH-SIZE", "5");
        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(5, instance.options.getBatchSize());
    }

    @Test
    public void testInit_failOnError_falseCaseInsensitive() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        System.setProperty("FAIL-ON-ERROR", "False");
        String[] args = getDefaultArgs();
        Properties props = new Properties();

        Manager instance = new Manager();
        instance.init(args, props);
        assertFalse(instance.options.isFailOnError());
    }

    @Test
    public void testInit_failOnErrorInvalidValue() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        System.setProperty("FAIL-ON-ERROR", "No");
        String[] args = getDefaultArgs();
        Properties props = new Properties();

        Manager instance = new Manager();
        instance.init(args, props);
        assertTrue(instance.options.isFailOnError());
    }

    @Test
    public void testInitOptions_ensurePropertiesAreSet() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        System.setProperty("ERROR-FILE-NAME", EXPORT_FILE_DIR + "/out");
        System.setProperty("EXPORT-FILE-PART-EXT", "pt");
        String[] args = getDefaultArgs();
        Properties props = new Properties();

        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(1, instance.options.getBatchSize());
        assertEquals(EXPORT_FILE_DIR, instance.properties.getProperty("EXPORT-FILE-DIR"));
        assertEquals(EXPORT_FILE_NAME, instance.properties.getProperty("EXPORT-FILE-NAME"));
        assertEquals(EXPORT_FILE_DIR + "/out", instance.properties.getProperty("ERROR-FILE-NAME"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitOptions_exportDirNotExists() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();

        String[] args = getDefaultArgs();
        args[13] = "/does/not/exist";

        Properties props = new Properties();
        Manager instance = new Manager();
        instance.init(args, props);
    }

    @Test
    public void testInitOptions_exportFileAndErrorFileExists() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();
        String errorFilename = "error.txt";
        File errorFile = new File(EXPORT_FILE_DIR, errorFilename);
        errorFile.createNewFile();
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
        exportFile.createNewFile();
        String[] args = getDefaultArgs();

        Properties props = new Properties();
        props.setProperty("ERROR-FILE-NAME", errorFilename);
        Manager instance = new Manager();
        instance.init(args, props);
        assertFalse(errorFile.exists());
        assertFalse(exportFile.exists());
    }

    @Test
    public void testInitOptions_clearExportFilePartExt() throws Exception {
        System.out.println("initOptions");
        clearSystemProperties();

        System.setProperty("EXPORT-FILE-PART-EXT", "exp");
        String[] args = getDefaultArgs();
        args[12] = null;
        Properties props = new Properties();
        props.setProperty("EXPORT-FILE-PART-EXT", "expt");
        Manager instance = new Manager();
        instance.init(args, props);
        assertNull(instance.properties.getProperty("EXPORT-FILE-PART-EXT"));
        assertNull(System.getProperty("EXPORT-FILE-PART-EXT"));
    }

    @Test
    public void testInitOptions_defaultOptions() throws Exception {
        System.out.println("initOptions");
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

        Manager instance = new Manager();
        instance.init(args, props);

        assertNull(instance.properties.getProperty("PROCESS-TASK"));
        assertFalse(instance.options.isDoInstall());
        assertNull(instance.options.getUrisModule());
        assertEquals("Modules", instance.options.getModulesDatabase());
        assertNull(instance.options.getUrisFile());
        assertEquals(1, instance.options.getBatchSize());
        assertTrue(instance.options.isFailOnError());
        assertNull(instance.properties.getProperty("EXPORT-FILE-DIR"));
        assertNull(instance.properties.getProperty("EXPORT-FILE-NAME"));
        assertNull(instance.properties.getProperty("ERROR-FILE-NAME"));
        assertNull(instance.options.getInitModule());
        assertNull(instance.options.getInitTaskClass());
        assertNull(instance.options.getPreBatchModule());
        assertNull(instance.options.getPreBatchTaskClass());
        assertNull(instance.options.getPostBatchModule());
        assertNull(instance.options.getPostBatchTaskClass());
        assertNull(instance.properties.getProperty("EXPORT-FILE-PART-EXT"));
        assertNull(System.getProperty("EXPORT-FILE-PART-EXT"));
    }

    @Test
    public void testInitOptions_initModule() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty("INIT-MODULE", "initModule");
        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals("initModule", instance.options.getInitModule());
    }

    @Test
    public void testInitOptions_processTaskClass() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        Properties props = new Properties();
        props.setProperty("INIT-TASK", PROCESS_TASK);
        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(PROCESS_TASK, instance.options.getProcessTaskClass().getName());
    }

    @Test
    public void testInitOptions_customUrisLoader() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        String loader = "com.marklogic.developer.corb.FileUrisLoader";
        Properties props = new Properties();
        props.setProperty("URIS-LOADER", loader);
        Manager instance = new Manager();
        instance.init(args, props);
        assertEquals(loader, instance.options.getUrisLoaderClass().getName());
    }

    @Test
    public void testInitOptions_InstallWithBlankModules() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/selector.xqy";
        args[6] = "";
        args[7] = "true";
        String loader = "com.marklogic.developer.corb.FileUrisLoader";
        Properties props = new Properties();
        Manager instance = new Manager();
        instance.options.setModulesDatabase("");
        instance.init(args, props);
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, new LogRecord(Level.WARNING, "XCC configured for the filesystem: please install modules manually")));
    }

    @Test(expected = NullPointerException.class)
    public void testInitOptions_InstallWithMissingModule() throws Exception {
        System.out.println("init");
        clearSystemProperties();
        String[] args = getDefaultArgs();
        args[4] = "src/test/resources/doesNotExist.xqy";
        args[7] = "true";
        Properties props = new Properties();
        Manager instance = new Manager();
        instance.init(args, props);
    }

    @Test
    public void testNormalizeLegacyProperties_whenPropertiesIsNull() {
        System.out.println("normalizeLegacyProperties");
        Manager manager = new Manager();
        manager.properties = null;
        manager.normalizeLegacyProperties();
        assertNull(manager.properties);
    }

    @Test
    public void testNormalizeLegacyProperties() {
        System.out.println("normalizeLegacyProperties");
        Properties props = new Properties();
        props.setProperty("XQUERY-MODULE", "foo");
        props.setProperty("XQUERY-MODULE.bar", "baz");
        Manager manager = new Manager();
        manager.properties = props;
        manager.normalizeLegacyProperties();

        assertEquals("foo", manager.properties.getProperty("PROCESS-MODULE"));
        assertEquals("baz", manager.properties.getProperty("PROCESS-MODULE.bar"));
    }

    @Test
    public void testNormalizeLegacyProperties_precedenceChecks() {
        System.out.println("normalizeLegacyProperties");
        Properties props = new Properties();
        props.setProperty("PROCESS-MODULE.bar", "foo");
        props.setProperty("XQUERY-MODULE.bar", "baz");
        Manager manager = new Manager();
        manager.properties = props;
        manager.normalizeLegacyProperties();

        assertEquals("foo", manager.properties.getProperty("PROCESS-MODULE.bar"));
    }

    /**
     * Test of getTaskCls method, of class Manager.
     */
    @Test
    public void testGetTaskCls() throws Exception {
        System.out.println("getTaskCls");
        String type = "";
        String className = "com.marklogic.developer.corb.Transform";
        Manager instance = new Manager();
        Class<? extends Task> expResult = Transform.class;
        Class<? extends Task> result = instance.getTaskCls(type, className);
        assertEquals(expResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTaskCls_notTaskClass() throws Exception {
        System.out.println("getTaskCls");
        String type = "";
        String className = "java.lang.String";
        Manager instance = new Manager();
        Class<? extends Task> expResult = Transform.class;
        Class<? extends Task> result = instance.getTaskCls(type, className);
        assertEquals(expResult, result);
    }

    /**
     * Test of getUrisLoaderCls method, of class Manager.
     */
    @Test
    public void testGetUrisLoaderCls() throws Exception {
        System.out.println("getUrisLoaderCls");
        String className = "com.marklogic.developer.corb.FileUrisLoader";
        Manager instance = new Manager();
        Class<? extends UrisLoader> expResult = FileUrisLoader.class;
        Class<? extends UrisLoader> result = instance.getUrisLoaderCls(className);
        assertEquals(expResult, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUrisLoaderCls_notUrisClass() throws Exception {
        System.out.println("getUrisLoaderCls");
        String className = "java.lang.String";
        Manager instance = new Manager();
        Class<? extends UrisLoader> expResult = FileUrisLoader.class;
        Class<? extends UrisLoader> result = instance.getUrisLoaderCls(className);
        assertEquals(expResult, result);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testGetUrisLoaderCls_badClassname() throws Exception {
        System.out.println("getUrisLoaderCls");
        String className = "does.not.Exist";
        Manager instance = new Manager();
        instance.getUrisLoaderCls(className);
    }

    /**
     * Test of usage method, of class Manager.
     */
    @Test
    public void testUsage() {
        System.out.println("usage");
        Manager instance = new Manager();
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(outContent));
        instance.usage();
        assertTrue(outContent.toString().contains("usage"));
    }

    /**
     * Test of run method, of class Manager.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRun_missingURIS_MODULE_FILE_AND_LOADER() throws Exception {
        System.out.println("run");
        Manager instance = new Manager();
        instance.run();
    }

    @Test(expected = NullPointerException.class)
    public void testRun_getURILoader_withURIS_MODULE_noContentSource() throws Exception {
        System.out.println("run");
        Manager instance = new Manager();
        instance.options.setUrisModule("someFile.xqy");
        instance.run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRun_getURILoader_withURIS_MODULE_invalidCollection() throws Exception {
        System.out.println("run");
        Manager instance = new Manager();
        instance.contentSource = ContentSourceFactory.newContentSource(new URI(XCC_CONNECTION_URI));
        instance.options.setUrisModule("someFile.xqy");
        instance.run();
    }

    @Test
    public void testRun_getURILoader_withURIS_MODULE() throws Exception {
        System.out.println("run");
        Manager instance = new Manager();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke moduleInvoke = mock(ModuleInvoke.class);
        ResultSequence res = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        ResultItem uriCountResult = mock(ResultItem.class);
        XdmItem batchRefItem = mock(XdmItem.class);
        XdmItem uriCount = mock(XdmItem.class);
        when(contentSource.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(moduleInvoke);
        when(session.submitRequest(moduleInvoke)).thenReturn(res);
        when(res.next()).thenReturn(resultItem).thenReturn(uriCountResult).thenReturn(null);
        when(resultItem.getItem()).thenReturn(batchRefItem);
        when(uriCountResult.getItem()).thenReturn(uriCount);
        when(batchRefItem.asString()).thenReturn("batchRefVal");
        when(uriCount.asString()).thenReturn("0");

        instance.contentSource = contentSource;
        instance.collection = "Modules";
        instance.options.setUrisModule("someFile.xqy");
        instance.run();
    }

    /**
     * Test of registerStatusInfo method, of class Manager.
     */
    @Test
    public void testRegisterStatusInfo() throws RequestException {
        System.out.println("registerStatusInfo");
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
        when(firstXdmItem.asString()).thenReturn("0");
        when(first.getItem()).thenReturn(firstXdmItem);
        when(first.getIndex()).thenReturn(0);
        when(secondXdmItem.asString()).thenReturn(xccRootValue);
        when(second.getItem()).thenReturn(secondXdmItem);
        when(second.getIndex()).thenReturn(1);
        when(resultSequence.next()).thenReturn(first, second);
        when(session.submitRequest(any(Request.class))).thenReturn(resultSequence);

        Manager instance = new Manager();
        instance.contentSource = contentSource;
        instance.registerStatusInfo();

        assertEquals(xccRootValue, instance.options.getXDBC_ROOT());
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(16, records.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRegisterStatusInfo_nullContentSource() {
        System.out.println("registerStatusInfo");
        Manager instance = new Manager();
        instance.registerStatusInfo();
    }

    /**
     * Test of logProperties method, of class Manager.
     */
    @Test
    public void testLogProperties() {
        System.out.println("logProperties");
        Properties props = new Properties();
        props.setProperty("key1", "value1");
        props.setProperty("key2", "value2");
        Manager instance = new Manager();
        instance.properties = props;
        instance.logProperties();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(props.size(), records.size());
    }

    @Test
    public void testLogProperties_nullProperties() {
        System.out.println("logProperties");
        Manager instance = new Manager();
        instance.logProperties();
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(0, records.size());
    }

    /**
     * Test of stop method, of class Manager.
     */
    @Test
    public void testStop_0args() {
        System.out.println("stop");
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
        System.out.println("stop");
        ExecutionException e = new ExecutionException("test", new Error());
        Manager instance = new Manager();
        instance.stop(e);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals("fatal error", records.get(0).getMessage());
    }

    @Test
    public void testCommandFilePause() throws IOException, Exception {
        System.out.println("pause/resume test");
        clearSystemProperties();
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
        exportFile.deleteOnExit();

        File commandFile = new File(EXPORT_FILE_DIR, Math.random() + ".txt");
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        System.setProperty(Options.URIS_FILE, URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, "1");
        System.setProperty(Options.PROCESS_MODULE, "src/test/resources/transformSlow.xqy|ADHOC");
        System.setProperty(Options.PROCESS_TASK, PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());

        Manager instance = new Manager();
        instance.init(new String[0]);

        Runnable pause = new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put(Options.COMMAND, "pause");
                File commandFile = new File(System.getProperty(Options.COMMAND_FILE));
                try {
                    commandFile.createNewFile();
                    FileOutputStream fos = new FileOutputStream(commandFile);
                    props.store(fos, null);
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(ManagerTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        Runnable resume = new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put(Options.COMMAND, "RESUME");
                props.put(Options.THREAD_COUNT, "6");
                File commandFile = new File(System.getProperty(Options.COMMAND_FILE));
                try {
                    FileOutputStream fos = new FileOutputStream(commandFile);
                    props.store(fos, null);
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(ManagerTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(pause, 1, TimeUnit.SECONDS);
        service.schedule(resume, 5, TimeUnit.SECONDS);

        instance.run();

        int lineCount = FileUtils.getLineCount(exportFile);
        assertEquals(8, lineCount);
        List<LogRecord> records = testLogger.getLogRecords();

        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "pausing")));
        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "resuming")));
    }

    @Test
    public void testCommandFilePause_resumeWhenCommandFileChangedAndNoCommand() throws IOException, Exception {
        System.out.println("pause/resume test");
        clearSystemProperties();
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
        exportFile.deleteOnExit();

        File commandFile = new File(EXPORT_FILE_DIR, Math.random() + ".txt");
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        System.setProperty(Options.URIS_FILE, URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, "1");
        System.setProperty(Options.PROCESS_MODULE, "src/test/resources/transformSlow.xqy|ADHOC");
        System.setProperty(Options.PROCESS_TASK, PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());

        Manager instance = new Manager();
        instance.init(new String[0]);

        Runnable pause = new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put(Options.COMMAND, "pause");
                File commandFile = new File(System.getProperty(Options.COMMAND_FILE));
                try {
                    commandFile.createNewFile();
                    FileOutputStream fos = new FileOutputStream(commandFile);
                    props.store(fos, null);
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(ManagerTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        Runnable resume = new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                File commandFile = new File(System.getProperty(Options.COMMAND_FILE));
                try {
                    FileOutputStream fos = new FileOutputStream(commandFile);
                    props.store(fos, null);
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(ManagerTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(pause, 1, TimeUnit.SECONDS);
        service.schedule(resume, 4, TimeUnit.SECONDS);

        instance.run();

        int lineCount = FileUtils.getLineCount(exportFile);
        assertEquals(8, lineCount);
        List<LogRecord> records = testLogger.getLogRecords();

        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "pausing")));
        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "resuming")));
    }

    @Test
    public void testCommandFileStop() throws IOException, Exception {
        System.out.println("stop test");
        clearSystemProperties();
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
        exportFile.deleteOnExit();

        File commandFile = new File(EXPORT_FILE_DIR, Math.random() + ".txt");
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        System.setProperty(Options.URIS_FILE, URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, "1");
        System.setProperty(Options.PROCESS_MODULE, "src/test/resources/transformSlow.xqy|ADHOC");
        System.setProperty(Options.PROCESS_TASK, PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());

        Runnable stop = new Runnable() {
            @Override
            public void run() {
                Properties props = new Properties();
                props.put(Options.COMMAND, "STOP");
                File commandFile = new File(System.getProperty(Options.COMMAND_FILE));
                try {
                    commandFile.createNewFile();
                    FileOutputStream fos = new FileOutputStream(commandFile);
                    props.store(fos, null);
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(ManagerTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };

        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(stop, 1, TimeUnit.SECONDS);
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_STOP_COMMAND);
        Manager.main(new String[0]);
        int lineCount = FileUtils.getLineCount(exportFile);
        assertNotEquals(8, lineCount);
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "cleaning up")));
    }

    @Test
    public void testCommandFileLowerThreads() throws IOException, Exception {
        System.out.println("lower THREAD-COUNT test");
        clearSystemProperties();
        File exportFile = new File(EXPORT_FILE_DIR, EXPORT_FILE_NAME);
        exportFile.deleteOnExit();

        File commandFile = new File(EXPORT_FILE_DIR, Math.random() + ".txt");
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, XCC_CONNECTION_URI);
        System.setProperty(Options.URIS_FILE, URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, "4");
        System.setProperty(Options.PROCESS_MODULE, "src/test/resources/transformSlow.xqy|ADHOC");
        System.setProperty(Options.PROCESS_TASK, PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());

        Manager instance = new Manager();
        instance.init(new String[0]);

        Runnable adjustThreads = new Runnable() {
            @Override
            public void run() {
                File commandFile = new File(System.getProperty(Options.COMMAND_FILE));
                try {
                    commandFile.createNewFile();
                    Properties props = new Properties();
                    props.put(Options.THREAD_COUNT, "1");
                    FileOutputStream fos = new FileOutputStream(commandFile);
                    props.store(fos, null);
                    fos.close();
                } catch (IOException ex) {
                    Logger.getLogger(ManagerTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };

        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(adjustThreads, 1, TimeUnit.SECONDS);

        instance.run();
        int lineCount = FileUtils.getLineCount(exportFile);
        assertEquals(8, lineCount);
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "Changed {0} to {1}")));

    }

    @Test
    public void testSetThreadCount() {
        Manager instance = new Manager();
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

    public String[] getDefaultArgs() {
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
}
