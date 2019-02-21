/*
 * * Copyright (c) 2004-2019 MarkLogic Corporation
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 * *
 * * The use of the Apache License does not indicate that this project is
 * * affiliated with the Apache Software Foundation.
 */
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
        String localhostUrl = "http://localhost:" + port;
        String GET = "GET";
        try {
            JobServer server = JobServer.create(port);
            assertNotNull(server);
            server.start();

            //request to base URL for job page
            URL url = new URL(localhostUrl );
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(GET);
            byte[] content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            assertNotNull(content);

            url = new URL(localhostUrl + "/");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(GET);
            content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            assertNotNull(content);

            // ensure that base path (without extenstion) gets a response
            url = new URL(localhostUrl +  JobServer.METRICS_PATH);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(GET);
            content = IOUtils.toByteArray(conn.getInputStream());
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            assertNotNull(content);

            //verify that invalid paths result in 404
            url = new URL(localhostUrl + "/DoesNotExist");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(GET);
            assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());

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

    @Test
    public void testAddManagerWithJobIDWillAddContext() throws Exception {
        Manager manager = new Manager();
        manager.jobId = "foo";
        JobServer server = JobServer.create(Collections.singleton(9998), manager);
        server.addManager(manager);
        assertNotNull(manager.jobServer);
        assertEquals(server, manager.jobServer);
    }
}
