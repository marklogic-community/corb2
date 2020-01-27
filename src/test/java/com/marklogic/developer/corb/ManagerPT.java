/*
 * Copyright (c) 2004-2020 MarkLogic Corporation
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

import com.marklogic.developer.corb.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ManagerPT {

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    public static final String SLASH = "/";
    private static final Logger LOG = Logger.getLogger(ManagerPT.class.getName());

    private void clearSystemProperties() {
		TestUtils.clearSystemProperties();
	    System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "0");
	    System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
	}

    @Before
    public void setUp() throws IOException {
        clearSystemProperties();

        File tempDir = TestUtils.createTempDirectory();
        ManagerTest.EXPORT_FILE_DIR = tempDir.toString();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteFile(ManagerTest.EXPORT_FILE_DIR);
        clearSystemProperties();
    }

    @Test
    public void testExtremelyLargeUrisList() {

        int uriCount = 9999999;
        String exportFilename = "testManagerUsingExtremelyLargeUris.txt";

        Properties properties = ManagerTest.getDefaultProperties();
        properties.setProperty(Options.THREAD_COUNT, "8");
        properties.setProperty(Options.URIS_MODULE, "src/test/resources/selectorLargeList.xqy|ADHOC");
        properties.setProperty(Options.URIS_MODULE + ".count", String.valueOf(uriCount));
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(10000));
        properties.setProperty(Options.DISK_QUEUE_TEMP_DIR, "/var/tmp");
        properties.setProperty(Options.JOB_SERVER_PORT,"8000-9000");
        properties.setProperty(Options.JOB_NAME, "Manager Integration Test");
        properties.setProperty(Options.METRICS_LOG_LEVEL, "INFO");
        properties.setProperty(Options.METRICS_DATABASE, "marklogic-corb-content");
        properties.setProperty(Options.METRICS_COLLECTIONS, "corb-metrics");

        Manager manager = new Manager();
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

}
