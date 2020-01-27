/*
  * * Copyright (c) 2004-2020 MarkLogic Corporation
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
import static com.marklogic.developer.corb.TestUtils.clearFile;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static com.marklogic.developer.corb.FileUrisStreamingXMLLoaderTest.BUU_DIR;
import static com.marklogic.developer.corb.FileUrisStreamingXMLLoaderTest.BUU_FILENAME;
import static com.marklogic.developer.corb.FileUrisStreamingXMLLoaderTest.BUU_SCHEMA;
import static com.marklogic.developer.corb.FileUrisStreamingXMLLoaderTest.NOT_BUU_SCHEMA;
import com.marklogic.developer.corb.util.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisStreamingXMLLoaderIT {

    private static final String STREAMING_XML_LOADER = "com.marklogic.developer.corb.FileUrisStreamingXMLLoader";
    private static final String LARGE_PREFIX = "LARGE.";
    private static final String LARGE_BUU_FILENAME = LARGE_PREFIX + BUU_FILENAME;
    private static final int LARGE_COPIES_OF_BEM = 1000;
    private String exportFileDir;
    private static final Logger LOG = Logger.getLogger(FileUrisStreamingXMLLoaderIT.class.getName());

    @Before
    public void setUp() throws IOException {
        clearSystemProperties();
        File tempDir = TestUtils.createTempDirectory();
        exportFileDir = tempDir.toString();
        generateLargeInput(LARGE_COPIES_OF_BEM);
    }

    @After
    public void tearDown() throws IOException {
        clearSystemProperties();
        FileUtils.deleteFile(exportFileDir);
        File largeFile = new File(BUU_DIR + LARGE_BUU_FILENAME);
        if (largeFile.exists() && !largeFile.delete()) {
            LOG.log(Level.WARNING, "Unable to delete directory");
        }
    }

    @Test
    public void testInvalidLarge() {
        Properties properties = getBUUProperties();
        properties.setProperty(Options.XML_SCHEMA, BUU_DIR + NOT_BUU_SCHEMA);
        try {
            testStreamingXMLUrisLoader(LARGE_BUU_FILENAME, properties);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testStreamingXMLUrisLoaderWithDefaultXPath() {
        try {
            int result = testStreamingXMLUrisLoader(BUU_FILENAME, getBUUProperties());
            assertEquals(6, result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testStreamingXMLUrisLoaderWithXPath() {
        try {
            Properties props = getBUUPropertiesWithXPath();
            props.put("LAST_LINE","NA");

            int result = testStreamingXMLUrisLoader(BUU_FILENAME, props);
            assertEquals(7, result);

            String last = (String)props.get("LAST_LINE");
            assertEquals("POST-BATCH-MODULE,5,5,true",last);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testStreamingXMLUrisLoaderWithLargeInput() {
        int expected = LARGE_COPIES_OF_BEM + 1;
        Properties properties = getBUUProperties();
        properties.setProperty(Options.XML_FILE, BUU_DIR + LARGE_BUU_FILENAME);
        try {
            int result = testStreamingXMLUrisLoader(LARGE_BUU_FILENAME, properties);
            assertEquals(expected, result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    public int testStreamingXMLUrisLoader(String filename, Properties properties) {
        int lineCount = -1;
        String exportFileName = filename + "IT_report.txt";
        File report = new File(exportFileDir + SLASH + exportFileName);
        report.deleteOnExit();
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFileName);

        Manager manager = new Manager();
        try {
            manager.init(properties);
            manager.run();

            lineCount = FileUtils.getLineCount(report);

            if(properties.containsKey("LAST_LINE")) {
                BufferedReader input = new BufferedReader(new FileReader(report));
                String last=null, line = null;
                while ((line = input.readLine()) != null) {
                    last = line;
                }
                properties.put("LAST_LINE", last != null ? last : "NULL");
            }
            clearFile(report);

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        return lineCount;
    }

    public void generateLargeInput(int copies) {
        String exportFileName = LARGE_BUU_FILENAME;
        Properties properties = new Properties();
        properties.setProperty(Options.XCC_CONNECTION_URI, ManagerTest.XCC_CONNECTION_URI);
        properties.setProperty(Options.THREAD_COUNT, Integer.toString(10));
        properties.setProperty(Options.XML_FILE, BUU_DIR + BUU_FILENAME);
        properties.setProperty(Options.URIS_LOADER, "com.marklogic.developer.corb.FileUrisXMLLoader");
        properties.setProperty(Options.XML_NODE, "/*/*[2]");
        properties.setProperty(Options.PRE_BATCH_TASK, "com.marklogic.developer.corb.PreBatchUpdateFileTask");
        properties.setProperty(Options.PROCESS_TASK, "com.marklogic.developer.corb.ExportBatchToFileTask");
        properties.setProperty(Options.POST_BATCH_TASK, "com.marklogic.developer.corb.PostBatchUpdateFileTask");
        properties.setProperty(Options.PROCESS_MODULE, BUU_DIR + "processMultiply.xqy|ADHOC");
        properties.setProperty(Options.PROCESS_MODULE + ".COPIES", Integer.toString(copies));
        properties.setProperty(Options.EXPORT_FILE_TOP_CONTENT,
                "<bem:BenefitEnrollmentRequest xmlns:bem=\"http://bem.corb.developer.marklogic.com\">"
                + "<bem:FileInformation>\n"
                + "		<bem:InterchangeSenderID>PHANTOM</bem:InterchangeSenderID>\n"
                + "		<bem:InterchangeReceiverID>CMSFFM</bem:InterchangeReceiverID>\n"
                + "		<bem:GroupSenderID>11512NC0060024</bem:GroupSenderID>\n"
                + "		<bem:GroupReceiverID>NC0</bem:GroupReceiverID>\n"
                + "		<bem:GroupControlNumber>20140909</bem:GroupControlNumber>\n"
                + "		<bem:GroupTimeStamp>2014-10-15T00:00:00</bem:GroupTimeStamp>\n"
                + "		<bem:VersionNumber>22</bem:VersionNumber>\n"
                + "	</bem:FileInformation>");
        properties.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "</bem:BenefitEnrollmentRequest>");
        properties.setProperty(Options.EXPORT_FILE_DIR, BUU_DIR);
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(false));

        try {
            Manager manager = new Manager();
            manager.init(properties);
            manager.run();

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    public Properties getBUUProperties() {
        Properties properties = new Properties();
        properties.setProperty(Options.XCC_CONNECTION_URI, ManagerTest.XCC_CONNECTION_URI);
        properties.setProperty(Options.EXPORT_FILE_DIR, exportFileDir);
        properties.setProperty(Options.XML_FILE, BUU_DIR + BUU_FILENAME);
        properties.setProperty(Options.XML_SCHEMA, BUU_DIR + BUU_SCHEMA);
        properties.setProperty(Options.URIS_LOADER, STREAMING_XML_LOADER);
        properties.setProperty(Options.PRE_BATCH_MODULE, BUU_DIR + "preBatch.xqy|ADHOC");
        properties.setProperty(Options.PROCESS_TASK, ExportBatchToFileTask.class.getName());
        properties.setProperty(Options.PROCESS_MODULE, BUU_DIR + "process.xqy|ADHOC");
        properties.setProperty(Options.POST_BATCH_MODULE, BUU_DIR + "postBatch.xqy|ADHOC");
        properties.setProperty(Options.THREAD_COUNT, Integer.toString(10));
        properties.setProperty(Options.LOADER_SET_URIS_BATCH_REF, Boolean.toString(true));
        return properties;
    }

    public Properties getBUUPropertiesWithXPath() {
        Properties properties = new Properties();
        properties.setProperty(Options.XCC_CONNECTION_URI, ManagerTest.XCC_CONNECTION_URI);
        properties.setProperty(Options.EXPORT_FILE_DIR, exportFileDir);
        properties.setProperty(Options.XML_FILE, BUU_DIR + BUU_FILENAME);
        properties.setProperty(Options.XML_SCHEMA, BUU_DIR + BUU_SCHEMA);
        properties.setProperty(Options.URIS_LOADER, STREAMING_XML_LOADER);

        properties.setProperty(Options.XML_NODE, "/BenefitEnrollmentRequest/BenefitEnrollmentMaintenance");
        properties.setProperty(Options.XML_METADATA, "/BenefitEnrollmentRequest/FileInformation");
        properties.setProperty(Options.PRE_BATCH_TASK, PreBatchUpdateFileTask.class.getName());
        properties.setProperty(Options.PRE_BATCH_MODULE, BUU_DIR + "preBatch2.xqy|ADHOC");
        properties.setProperty(Options.PROCESS_TASK, ExportBatchToFileTask.class.getName());
        properties.setProperty(Options.PROCESS_MODULE, BUU_DIR + "process2.xqy|ADHOC");
        properties.setProperty(Options.POST_BATCH_TASK, PostBatchUpdateFileTask.class.getName());
        properties.setProperty(Options.POST_BATCH_MODULE, BUU_DIR + "postBatch2.xqy|ADHOC");

        properties.setProperty(Options.THREAD_COUNT, Integer.toString(10));
        properties.setProperty(Options.LOADER_SET_URIS_BATCH_REF, Boolean.toString(true));

        String batchId = System.currentTimeMillis()+String.format("%06d", (int)(Math.random()*1000000));
        properties.setProperty(Options.PRE_BATCH_MODULE+".BATCH_ID",batchId);
        properties.setProperty(Options.PROCESS_MODULE+".BATCH_ID",batchId);
        properties.setProperty(Options.POST_BATCH_MODULE+".BATCH_ID",batchId);
        return properties;
    }
}
