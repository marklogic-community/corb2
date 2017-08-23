package com.marklogic.developer.corb;

import com.marklogic.developer.TestHandler;
import com.marklogic.developer.corb.util.IOUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
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
            HttpServer server = JobServer.create(port);
            assertNotNull(server);
           
            URL url = new URL("http://localhost:" + port + "/web/");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            byte[] content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(200, conn.getResponseCode());
            assertNotNull(content);

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
    public void testLogUsage() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(9998), 0);
        JobServer.logUsage(server);
        server.stop(0);
        List<LogRecord> records = testLogger.getLogRecords();
        assertEquals(4, records.size());
    }

}
