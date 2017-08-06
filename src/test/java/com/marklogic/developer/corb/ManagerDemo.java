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
    private static final Logger LOG = Logger.getLogger(ManagerPT.class.getName());
    private static final String TRANSFORM_ERROR_MODULE = "src/test/resources/transform-error.xqy|ADHOC";
    private static final String TRANSFORM_SLOW_MODULE = "src/test/resources/transformSlow.xqy|ADHOC";
    static int count=1;
    public static void startManager(int uriCount) {

        String exportFilename = "testManagerUsingExtremelyLargeUris.txt";

        Properties properties = ManagerTest.getDefaultProperties();
        properties.setProperty(Options.THREAD_COUNT, "8");
        properties.setProperty(Options.URIS_MODULE, "src/test/resources/selectorLargeList.xqy|ADHOC");
        properties.setProperty(Options.URIS_MODULE + ".count", String.valueOf(uriCount));
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(10000));
        properties.setProperty(Options.DISK_QUEUE_TEMP_DIR, "/var/tmp");
        properties.setProperty(Options.JOB_SERVER_PORT, "9080-9090");
        properties.setProperty(Options.PROCESS_MODULE, TRANSFORM_SLOW_MODULE);
        properties.setProperty(Options.FAIL_ON_ERROR, "false");
        properties.setProperty(Options.METRICS_SYNC_FREQUENCY, "5");
        properties.setProperty(Options.METRICS_DB_NAME, "marklogic-corb-content");
        properties.setProperty(Options.METRICS_DOC_COLLECTIONS, "managerDemo");

        Manager manager = new Manager();

        try {
        	manager.init(properties);

        	Thread managerThread=new Thread(){
        		@Override
        		public void run() {

                    try {
						manager.run();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                    finally {
                    	File report = new File(ManagerTest.EXPORT_FILE_DIR + SLASH + exportFilename);
                        report.deleteOnExit();
                        try {
							int lineCount = FileUtils.getLineCount(report);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
        		}
        	};
        	managerThread.start();



        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        finally{
        	manager.stop();
        }
    }
    public static void main(String[] args) throws IOException {
    	HTTPServer jobServer = new HTTPServer(9091);
		HTTPServer.VirtualHost host = jobServer.getVirtualHost(null); // default host
		host.setAllowGeneratedIndex(false); // with directory index pages
		HTTPServer.ContextHandler htmlContextHandler = new HTTPServer.ClasspathResourceContextHandler("corb2-web","/web");
		host.addContext("/web", htmlContextHandler);
		jobServer.start();
		startManager(100000);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		startManager(100000);
//		startManager(100000);
//		startManager(100000);
//		startManager(100000);
//		startManager(100000);

	}
}
