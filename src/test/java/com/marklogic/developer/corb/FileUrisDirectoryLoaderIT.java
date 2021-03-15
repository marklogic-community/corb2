/*
  * * Copyright (c) 2004-2021 MarkLogic Corporation
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

import static com.marklogic.developer.corb.ManagerIT.SLASH;
import com.marklogic.developer.corb.util.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisDirectoryLoaderIT {

    public static final String LOADER_PROCESS_MODULE = "src/test/resources/fileUrisLoaderProcess.xqy|ADHOC";
    public static final String DOC_LOADER_PROCESS_MODULE = "src/test/resources/fileDocLoaderProcess.xqy|ADHOC";
    private static final Logger LOG = Logger.getLogger(FileUrisDirectoryLoaderIT.class.getName());
    private File tempDir;

    public FileUrisDirectoryLoaderIT() {
    }

    @Before
    public void setUp() throws IOException {
        tempDir = TestUtils.createTempDirectory();
        tempDir.deleteOnExit();
    }

    @Test
    public void testLoadAll() {
        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, "testLoadAll.txt");
        try {
            testFileUrisDirectoryLoader(properties);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            ex.printStackTrace();
            fail();
        }
    }

    @Test
    public void testLoadAllAsDocs() {
        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, "testLoadAllAsDocs.txt");
        properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_DOC);
        properties.setProperty(Options.PROCESS_MODULE, DOC_LOADER_PROCESS_MODULE);
        try {
            testFileUrisDirectoryLoader(properties);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            fail();
        }
    }

    @Test(expected = IOException.class)
    public void testLoadAllAsDocsWithBatch() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, "testLoadAllAsDocsWithBatch.txt");
        properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_DOC);
        properties.setProperty(Options.BATCH_SIZE, Integer.toString(10));
        properties.setProperty(Options.PROCESS_MODULE, DOC_LOADER_PROCESS_MODULE);

        testFileUrisDirectoryLoader(properties);
    }

    @Test
    public void testLoadAllWithDiskQueue() {
        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, "testLoadAllWithDiskQueue.txt");
        properties.setProperty(Options.DISK_QUEUE, Boolean.toString(true));
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, Integer.toString(2));
        try {
            testFileUrisDirectoryLoader(properties);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            fail();
        }
    }

    private void testFileUrisDirectoryLoader(Properties additionalProperties) throws Exception {
        if (additionalProperties == null || !additionalProperties.containsKey(Options.EXPORT_FILE_NAME)) {
            fail();
        }
        Properties properties = new Properties();
        properties.putAll(getDefaultProperties());

        properties.putAll(additionalProperties);
        String exportFilename = additionalProperties.getProperty(Options.EXPORT_FILE_NAME);
        Manager manager = new Manager();

        manager.init(properties);
        manager.run();

        String exportFilePath = tempDir.getPath() + SLASH + exportFilename;
        verifyLoaderReport(exportFilePath);
    }

    public static void verifyLoaderReport(String exportFilePath) throws IOException {
        File report = new File(exportFilePath);
        report.deleteOnExit();

        String results = TestUtils.readFile(report);
        assertEquals(11, FileUtils.getLineCount(report));
        assertTrue(results.contains("Demographic_Statistics_By_Zip_Code.xml,element"));
        assertTrue(results.contains("Demographic_Statistics_By_Zip_Code.rdf,element"));
        assertTrue(results.contains("Demographic_Statistics_By_Zip_Code.json,object"));
        assertTrue(results.contains("Demographic_Statistics_By_Zip_Code.csv,text"));
        assertTrue(results.contains("simple document.docx,binary"));
        assertTrue(results.contains("simple document.html,element"));
        assertTrue(results.contains("simple document.pdf,binary"));
        assertTrue(results.contains("logo-community-white.svg")); //document filter returning application/octet-stream in ML 10
        assertTrue(results.contains("markLogic.gif,binary"));
        assertTrue(results.contains("MarkLogic.png,binary"));
        assertTrue(results.contains("diagram-legal.jpg,binary"));
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(Options.XCC_CONNECTION_URI, ManagerTest.XCC_CONNECTION_URI);
        properties.setProperty(Options.EXPORT_FILE_DIR, tempDir.getPath());
        properties.setProperty(Options.LOADER_PATH, FileUrisDirectoryLoaderTest.TEST_DIR);
        properties.setProperty(Options.PROCESS_TASK, ManagerTest.PROCESS_TASK);
        properties.setProperty(Options.THREAD_COUNT, Integer.toString(8));
        properties.setProperty(Options.URIS_LOADER, FileUrisDirectoryLoader.class.getName());
        properties.setProperty(Options.PROCESS_MODULE, LOADER_PROCESS_MODULE);
        return properties;
    }
}
