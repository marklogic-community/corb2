package com.marklogic.developer.corb;

import static org.junit.Assert.assertEquals;
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
        properties.setProperty(Options.JOB_SERVER_PORT, "8010-8020");
        
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
    }
    public static void main(String[] args) throws IOException {
    	HTTPServer jobServer = new HTTPServer(8021);
		HTTPServer.VirtualHost host = jobServer.getVirtualHost(null); // default host
		host.setAllowGeneratedIndex(false); // with directory index pages
		HTTPServer.ContextHandler htmlContextHandler = new HTTPServer.ClasspathResourceContextHandler("corb2-web","/web");
		host.addContext("/web", htmlContextHandler);
		jobServer.start();
		startManager(100000);
		startManager(100000);
		startManager(100000);
		startManager(100000);
		startManager(100000);
		startManager(100000);
		
	}
}
