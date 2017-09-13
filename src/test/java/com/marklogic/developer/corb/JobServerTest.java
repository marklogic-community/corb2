package com.marklogic.developer.corb;

import com.marklogic.developer.TestHandler;
import com.marklogic.developer.corb.util.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.*;

public class JobServerTest {
    private final TestHandler testLogger = new TestHandler();
    private static final Logger JOBSERVER_LOGGER = Logger.getLogger(JobServer.class.getName());

    @Before
    public void setUp() throws IOException {
        JOBSERVER_LOGGER.addHandler(testLogger);
    }

    @Test
    public void testCreateAndGet() {
        int port = 9999;
        try {
            JobServer server = JobServer.create(port);
            assertNotNull(server);
            server.start();

            //request to base URL for job page
            URL url = new URL("http://localhost:" + port );
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            byte[] content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(200, conn.getResponseCode());
            assertNotNull(content);

            url = new URL("http://localhost:" + port + "/");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(200, conn.getResponseCode());
            assertNotNull(content);

            // ensure that base path (without extenstion) gets a response
            url = new URL("http://localhost:" + port +  server.METRICS_PATH);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(200, conn.getResponseCode());
            assertNotNull(content);

            //verify that invalid paths result in 404
            url = new URL("http://localhost:" + port + "/DoesNotExist");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            assertEquals(404, conn.getResponseCode());

            server.stop(0);
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testGetContentType() throws Exception {
        String HTML = "text/html; charset=utf-8";
        assertEquals(HTML, JobServer.getContentType("doc.html"));
        assertEquals("application/javascript", JobServer.getContentType("doc.js"));
        assertEquals("text/css", JobServer.getContentType("doc.css"));
        assertEquals(HTML, JobServer.getContentType(""));
        assertEquals(HTML, JobServer.getContentType(null));
    }

    @Test
    public void testLogUsageWithoutManager() throws Exception {
        JobServer server = JobServer.create(9998);
        server.logUsage();
        server.stop(0);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(5, records.size());
    }

    @Test
    public void testLogUsageWithManager() throws Exception {
        Manager manager = new Manager();
        JobServer server = JobServer.create(Collections.singleton(9998), manager);
        server.logUsage();
        server.stop(0);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(5, records.size());
    }
}
