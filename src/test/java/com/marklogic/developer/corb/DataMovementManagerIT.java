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

import static com.marklogic.developer.corb.TestUtils.containsLogRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataMovementManagerIT extends ManagerIT {

    private static final Logger LOG = Logger.getLogger(DataMovementManagerIT.class.getName());
    public static final String HTTP_CONNECTION_URI = "http://marklogic-corb-admin:marklogic-corb-admin-password@localhost:8224/marklogic-corb-content";

    @Test
    public void testCommandFilePauseResumeWhenCommandFileChangedAndNoCommand() {
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

    @Override
    public DataMovementManager newManager() {
        return new DataMovementManager();
    }

    @Override
    public String getConnectionUri() {
        return HTTP_CONNECTION_URI;
    }
}
