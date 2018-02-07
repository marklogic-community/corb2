package com.marklogic.developer.corb;

import com.marklogic.client.*;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.eval.*;
import com.marklogic.client.io.*;
import com.marklogic.developer.corb.util.*;
import org.junit.*;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static com.marklogic.developer.corb.TestUtils.clearFile;
import static com.marklogic.developer.corb.TestUtils.containsLogRecord;
import static org.junit.Assert.*;

public class DataMovementManagerIT extends ManagerIT {

    private static final Logger LOG = Logger.getLogger(DataMovementManagerIT.class.getName());
    public static final String HTTP_CONNECTION_URI = "http://marklogic-corb-admin:marklogic-corb-admin-password@localhost:8224/marklogic-corb-content";

    @Test
    public void testMainNoUris() {
        try {
            File empty = File.createTempFile(this.getClass().getSimpleName(), "txt");
            empty.deleteOnExit();
            exit.expectSystemExitWithStatus(Manager.EXIT_CODE_NO_URIS);
            //Using the DataMovementManager
            DataMovementManager.main(getConnectionUri(), "", "src/test/resources/transform.xqy|ADHOC", "1", "", "", "", "", "", "", "", "", "", "", "", empty.getAbsolutePath());
        } catch (IOException ex) {
            fail();
        }
    }

    @Test
    public void testInitOptionsSetNumTPSForETC() {
        Properties properties = ManagerTest.getDefaultProperties();
        //Need to use the REST server, not XCC
        properties.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        properties.setProperty(Options.NUM_TPS_FOR_ETC, Integer.toString(500));
        Manager manager = newManager();
        try {
            manager.init(properties);
        } catch (CorbException ex) {
            fail();
        }
        assertEquals(500, manager.options.getNumTpsForETC());
    }

    @Test
    public void testMainErrorRunning() {
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_PROCESSING_ERROR);
        DataMovementManager.main(getConnectionUri(), "", "missing.xqy|ADHOC", "1", "missing.xqy|ADHOC");
    }

    @Test
    public void testManagerUsingProgArgs() {
        clearSystemProperties();
        String exportFileName = "testManagerUsingProgArgs.txt";
        String exportFileDir = ManagerTest.EXPORT_FILE_DIR;
        String[] args = ManagerTest.getDefaultArgs();
        args[0] = getConnectionUri(); //Need REST server
        args[14] = exportFileName;
        args[15] = null;
        File report = new File(exportFileDir + SLASH + exportFileName);
        report.deleteOnExit();
        boolean passed = testManager(args, report);
        clearFile(report);
        assertTrue(passed);

        //Then verify the exit code when invoking the main()
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        DataMovementManager.main(args);
    }

    @Test
    public void testManagerUsingSysProps() {
        clearSystemProperties();
        String exportFileName = "testManagerUsingSysProps2.txt";
        ManagerTest.setDefaultSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri()); //Need REST server
        System.setProperty(Options.URIS_MODULE, "src/test/resources/selector.xqy|ADHOC");
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        String[] args = null;
        File report = new File(ManagerTest.EXPORT_FILE_DIR + SLASH + exportFileName);
        report.deleteOnExit();

        boolean passed = testManager(args, report);
        clearFile(report);
        assertTrue(passed);

        //Then verify the exit code when invoking the main()
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        DataMovementManager.main(args);
    }

    @Test
    public void testManagerUsingSysPropsLargeUrisList() {
        clearSystemProperties();
        int uriCount = 100;
        String exportFilename = "testManagerUsingSysProps1.txt";
        Properties properties = ManagerTest.getDefaultProperties();
        //Need to use the REST server, not XCC
        properties.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        properties.setProperty(Options.THREAD_COUNT, String.valueOf(4));
        properties.setProperty(Options.URIS_MODULE, LARGE_URIS_MODULE);
        properties.setProperty(Options.URIS_MODULE + ".count", String.valueOf(uriCount));
        properties.setProperty(Options.BATCH_SIZE, String.valueOf(1));
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        properties.setProperty(Options.DISK_QUEUE, "true");
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(10));
        properties.setProperty(Options.DISK_QUEUE_TEMP_DIR, "/var/tmp");

        Manager manager = newManager();
        try {
            manager.init(properties);
            manager.run();
            File report = new File(ManagerTest.EXPORT_FILE_DIR + SLASH + exportFilename);
            report.deleteOnExit();
            int lineCount = FileUtils.getLineCount(report);
            assertEquals(uriCount + 2, lineCount);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testManagerUsingSysPropsLargeUrisListAndBatchSize() {
        clearSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        int uriCount = 100;
        int batchSize = 3;
        String exportFilename = "testManagerUsingSysProps1.txt";
        Properties properties = ManagerTest.getDefaultProperties();
        properties.setProperty(Options.THREAD_COUNT, String.valueOf(4));
        properties.setProperty(Options.URIS_MODULE, LARGE_URIS_MODULE);
        properties.setProperty(Options.URIS_MODULE + ".count", String.valueOf(uriCount));
        properties.setProperty(Options.BATCH_SIZE, String.valueOf(batchSize));
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(10));

        Manager manager = newManager();
        try {
            manager.init(properties);
            manager.run();
            File report = new File(ManagerTest.EXPORT_FILE_DIR + SLASH + exportFilename);
            report.deleteOnExit();
            int lineCount = FileUtils.getLineCount(report);
            assertEquals((int)Math.ceil(uriCount / (float)batchSize) + 2, lineCount);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testManagerUsingPropsFile() {
        String exportFileName = ManagerTest.EXPORT_FILE_DIR + SLASH + "testManagerUsingPropsFile.txt";
        clearSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.OPTIONS_FILE, "src/test/resources/helloWorld.properties");
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        String[] args = {};
        File report = new File(exportFileName);
        report.deleteOnExit();

        boolean passed = testManager(args, report);
        assertTrue(passed);

        clearFile(report);
        //Then verify the exit code when invoking the main()
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        DataMovementManager.main(args);
    }

    @Test
    public void testManagerUsingInputFile() {
        clearSystemProperties();
        String exportFileName = "testManagerUsingInputFile.txt";
        ManagerTest.setDefaultSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        System.setProperty(Options.URIS_FILE, "src/test/resources/uriInputFile.txt");
        String[] args = {};
        String exportFilePath = ManagerTest.EXPORT_FILE_DIR + SLASH + exportFileName;
        File report = new File(exportFilePath);
        report.deleteOnExit();
        testManager(args, report);
        try {
            assertTrue(TestUtils.readFile(report).contains("Hello from the URIS-FILE!"));
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testManagersPreBatchTask() {
        clearSystemProperties();
        String exportFileName = "testManagersPreBatchTask.txt";
        ManagerTest.setDefaultSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        System.setProperty(Options.PRE_BATCH_MODULE, "preBatchModule.xqy|ADHOC");
        System.setProperty(Options.PRE_BATCH_TASK, "com.marklogic.developer.corb.PreBatchUpdateFileTask");
        String[] args = {};
        String exportFilePath = ManagerTest.EXPORT_FILE_DIR + SLASH + exportFileName;
        File report = new File(exportFilePath);
        report.deleteOnExit();
        testManager(args, report);
        try {
            assertTrue(TestUtils.readFile(report).contains(PRE_XQUERY_MODULE_OUTPUT));
            clearFile(report);
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testManagersPostBatchTask() {
        clearSystemProperties();
        String exportFileName = "testManagersPostBatchTask.txt";
        ManagerTest.setDefaultSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        System.setProperty(Options.POST_BATCH_MODULE, "postBatchModule.xqy|ADHOC");
        System.setProperty(Options.POST_BATCH_TASK, "com.marklogic.developer.corb.PostBatchUpdateFileTask");
        String[] args = {};
        String exportFilePath = ManagerTest.EXPORT_FILE_DIR + SLASH + exportFileName;
        File report = new File(exportFilePath);
        report.deleteOnExit();

        testManager(args, report);
        try {
            assertTrue(TestUtils.readFile(report).contains(POST_XQUERY_MODULE_OUTPUT));
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testManagersPostBatchTaskZip() {
        clearSystemProperties();
        String exportFileName = "testManagersPostBatchTaskZip.txt";
        ManagerTest.setDefaultSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        System.setProperty(Options.EXPORT_FILE_AS_ZIP, Boolean.toString(true));
        String[] args = {};
        //First, verify the output using run()
        Manager manager = newManager();
        try {
            manager.init();
            manager.run();

            String zippedExportFilePath = ManagerTest.EXPORT_FILE_DIR + SLASH + exportFileName + ".zip";
            File report = new File(zippedExportFilePath);

            assertTrue(report.exists());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        //Then verify the exit code when using the main() method
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        DataMovementManager.main(args);
    }

    @Test
    public void testManagerJavaScriptTransform() {
        clearSystemProperties();
        String exportFileName = "testManagerJavaScriptTransform.txt";
        ManagerTest.setDefaultSystemProperties();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.PROCESS_MODULE, "src/test/resources/mod-print-uri.sjs|ADHOC");
        System.setProperty("XQUERY-MODULE.foo", "bar1");
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        String[] args = {};

        String exportFilePath = ManagerTest.EXPORT_FILE_DIR + SLASH + exportFileName;
        File report = new File(exportFilePath);
        report.deleteOnExit();

        testManager(args, report);
        try {
            assertTrue(TestUtils.readFile(report).contains("bar1"));
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        clearFile(report);
        //Then verify the exit code when invoking the main()
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_SUCCESS);
        DataMovementManager.main(args);
    }

    @Test
    public void testMainNullArgs() {
        String[] args = null;
        exit.expectSystemExit();
        DataMovementManager.main(args);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(CORB_INIT_ERROR_MSG, records.get(0).getMessage());
    }

    @Test
    public void testMainException() {
        String[] args = ManagerTest.getDefaultArgs();
        exit.expectSystemExit();
        DataMovementManager.main(args);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(Level.SEVERE, records.get(0).getLevel());
        assertEquals(CORB_INIT_ERROR_MSG, records.get(0).getMessage());
    }

    @Test
    public void testCommandFilePause() {
        clearSystemProperties();
        File exportFile = new File(ManagerTest.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_NAME);
        exportFile.deleteOnExit();
        File commandFile = new File(ManagerTest.EXPORT_FILE_DIR, Math.random() + EXT_TXT);
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.URIS_FILE, ManagerTest.URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, Integer.toString(1));
        System.setProperty(Options.PROCESS_MODULE, TRANSFORM_SLOW_MODULE);
        System.setProperty(Options.PROCESS_TASK, ManagerTest.PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, ManagerTest.EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());

        Runnable pause = () -> {
            Properties props = new Properties();
            props.put(Options.COMMAND, SLOW_CMD);
            File commandFile1 = new File(System.getProperty(Options.COMMAND_FILE));
            try {
                if (commandFile1.createNewFile()) {
                    try (final FileOutputStream fos = new FileOutputStream(commandFile1)) {
                        props.store(fos, null);
                    }
                }
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        };
        Runnable resume = () -> {
            Properties props = new Properties();
            props.put(Options.COMMAND, "RESUME");
            props.put(Options.THREAD_COUNT, Integer.toString(6));
            File commandFile1 = new File(System.getProperty(Options.COMMAND_FILE));
            try (final FileOutputStream fos = new FileOutputStream(commandFile1)) {
                props.store(fos, null);
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        };
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(pause, 1, TimeUnit.SECONDS);
        service.schedule(resume, 5, TimeUnit.SECONDS);

        Manager instance = newManager();
        try {
            instance.init();
            instance.run();
            int lineCount = FileUtils.getLineCount(exportFile);
            assertEquals(8, lineCount);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, PAUSING));
        assertTrue(containsLogRecord(records, RESUMING));
    }

    @Test
    public void testCommandFilePauseResumeWhenCommandFileChangedAndNoCommand() {
        clearSystemProperties();
        File exportFile = new File(ManagerTest.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_NAME);
        exportFile.deleteOnExit();
        File commandFile = new File(ManagerTest.EXPORT_FILE_DIR, Math.random() + EXT_TXT);
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.URIS_MODULE, "src/test/resources/selectorLargeList.xqy|ADHOC");
        System.setProperty(Options.URIS_MODULE + ".count", "8");
        System.setProperty(Options.URIS_FILE, ManagerTest.URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, Integer.toString(1));
        System.setProperty(Options.PROCESS_MODULE, TRANSFORM_SLOW_MODULE);
        System.setProperty(Options.PROCESS_TASK, ManagerTest.PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, ManagerTest.EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());
        Runnable pause = () -> {
            Properties props = new Properties();
            props.put(Options.COMMAND, SLOW_CMD);
            File commandFile1 = new File(System.getProperty(Options.COMMAND_FILE));
            try {
                if (commandFile1.createNewFile()) {
                    try (final FileOutputStream fos = new FileOutputStream(commandFile1)) {
                        props.store(fos, null);
                    }
                }
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        };
        Runnable resume = () -> {
            Properties props = new Properties();
            File commandFile1 = new File(System.getProperty(Options.COMMAND_FILE));
            try (final FileOutputStream fos = new FileOutputStream(commandFile1)) {
                props.store(fos, null);
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        };
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(pause, 1, TimeUnit.SECONDS);
        service.schedule(resume, 4, TimeUnit.SECONDS);

        DataMovementManager instance = newManager();
        try {
            instance.init();
            instance.run();
            int lineCount = FileUtils.getLineCount(exportFile);
            assertEquals(8, lineCount);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, PAUSING));
        assertTrue(containsLogRecord(records, RESUMING));
    }

    @Test
    public void testCommandFileStop() {
        clearSystemProperties();
        File exportFile = new File(ManagerTest.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_NAME);
        exportFile.deleteOnExit();
        File commandFile = new File(ManagerTest.EXPORT_FILE_DIR, Math.random() + EXT_TXT);
        commandFile.deleteOnExit();
        System.setProperty(Options.XCC_CONNECTION_URI, getConnectionUri());
        System.setProperty(Options.URIS_FILE, ManagerTest.URIS_FILE);
        System.setProperty(Options.THREAD_COUNT, Integer.toString(1));
        System.setProperty(Options.PROCESS_MODULE, TRANSFORM_SLOW_MODULE);
        System.setProperty(Options.PROCESS_TASK, ManagerTest.PROCESS_TASK);
        System.setProperty(Options.EXPORT_FILE_NAME, ManagerTest.EXPORT_FILE_NAME);
        System.setProperty(Options.EXPORT_FILE_DIR, ManagerTest.EXPORT_FILE_DIR);
        System.setProperty(Options.COMMAND_FILE, commandFile.getAbsolutePath());
        Runnable stop = () -> {
            Properties props = new Properties();
            props.put(Options.COMMAND, "STOP");
            File commandFile1 = new File(System.getProperty(Options.COMMAND_FILE));
            try {
                if (commandFile1.createNewFile()) {
                    try (final FileOutputStream fos = new FileOutputStream(commandFile1)) {
                        props.store(fos, null);
                    }
                }
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        };
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        service.schedule(stop, 1, TimeUnit.SECONDS);
        exit.expectSystemExitWithStatus(Manager.EXIT_CODE_STOP_COMMAND);
        DataMovementManager.main();
        try {
            int lineCount = FileUtils.getLineCount(exportFile);
            assertNotEquals(8, lineCount);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(containsLogRecord(records, new LogRecord(Level.INFO, "cleaning up")));
    }


    @Override
    public DataMovementManager newManager() {
        return new DataMovementManager();
    }

    @Override
    public String getConnectionUri() {
        return HTTP_CONNECTION_URI;
    }
}
