package com.marklogic.developer.corb;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.marklogic.developer.corb.util.FileUtils;

public class ManagerDemo {

    public static final String SLASH = "/";
    private static final Logger LOG = Logger.getLogger(ManagerDemo.class.getName());
    private static final String TRANSFORM_SLOW_MODULE = "src/test/resources/transformSlow.xqy|ADHOC";

    public static Manager startManager(int uriCount) {
        return startManager(uriCount, true);
    }

    public static Manager startManager(int uriCount, boolean withJobServer) {

        String exportFilename = "testManagerUsingExtremelyLargeUris.txt";

        Properties properties = ManagerTest.getDefaultProperties();
        properties.setProperty(Options.THREAD_COUNT, "8");
        properties.setProperty(Options.URIS_MODULE, "src/test/resources/selectorLargeList.xqy|ADHOC");
        properties.setProperty(Options.URIS_MODULE + ".count", String.valueOf(uriCount));
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(10000));
        properties.setProperty(Options.DISK_QUEUE_TEMP_DIR, "/var/tmp");
        if (withJobServer) {
            properties.setProperty(Options.JOB_SERVER_PORT, "9080-9090");
        }
        properties.setProperty(Options.PROCESS_MODULE, TRANSFORM_SLOW_MODULE);
        properties.setProperty(Options.FAIL_ON_ERROR, "false");
        properties.setProperty(Options.METRICS_SYNC_FREQUENCY, "5");
        properties.setProperty(Options.METRICS_DATABASE, "marklogic-corb-content");
        properties.setProperty(Options.METRICS_COLLECTIONS, "managerDemo");

        Manager manager = new Manager();

        try {
            manager.init(properties);

            Thread managerThread = new Thread() {
                @Override
                public void run() {

                    try {
                        manager.run();
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "Encountered an error running a job", e);
                    } finally {
                        File report = new File(ManagerTest.EXPORT_FILE_DIR + SLASH + exportFilename);
                        report.deleteOnExit();
                        try {
                            int lineCount = FileUtils.getLineCount(report);
                        } catch (IOException e) {
                            LOG.log(Level.SEVERE, "Encountered an error reading export", e);
                        }
                    }
                }
            };
            managerThread.start();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            manager.stop();
            fail();
        }
        LOG.info("Running with id: " + manager.jobId);
        return manager;
    }

    public static void main(String[] args) throws IOException {
        JobServer jobServer = JobServer.create(9091);
        jobServer.start();
        //these jobs will use the JobServer port
        jobServer.addManager(startManager(100000, false));
        jobServer.addManager(startManager(100000, false));
        //add multiple jobs to the JobServer, these will use their own port
        jobServer.addManager(startManager(100000));
        jobServer.addManager(startManager(100000));
        // these are standalone jobs with their own port that we can discover in the UI
        startManager(100000);
        startManager(100000);
        startManager(100000);
        startManager(100000);
    }

}
