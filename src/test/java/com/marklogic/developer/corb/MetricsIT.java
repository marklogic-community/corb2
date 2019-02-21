/*
 * Copyright (c) 2004-2019 MarkLogic Corporation
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

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static java.util.logging.Level.SEVERE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import com.marklogic.developer.TestHandler;
import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class MetricsIT {

    private static final String JSON_EXT = ".json";
    private static final String XML_EXT = ".xml";
    private static final String METRICS_DB_NAME = "Documents";
    private static final String JS_MODULE = "saveMetrics.sjs|ADHOC";
    private static final String XQUERY_MODULE = "save-metrics.xqy|ADHOC";

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();
    public static final String SLASH = "/";
    private final TestHandler testLogger = new TestHandler();
    private static final Logger MANAGER_LOG = Logger.getLogger(Manager.class.getName());
    private static final Logger LOG = Logger.getLogger(MetricsIT.class.getName());
    private static final String LARGE_URIS_MODULE = "src/test/resources/selectorLargeList.xqy|ADHOC";
    private static final String TRANSFORM_ERROR_MODULE = "src/test/resources/transform-error.xqy|ADHOC";
    protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\";\n";

    @Before
    public void setUp() throws IOException {
        clearSystemProperties();
        MANAGER_LOG.addHandler(testLogger);
        File tempDir = TestUtils.createTempDirectory();
        ManagerTest.EXPORT_FILE_DIR = tempDir.toString();
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteFile(ManagerTest.EXPORT_FILE_DIR);
        clearSystemProperties();
    }

    @Test
    public void testManagerMetricsPeriodicSyncUsingSysPropsLargeUrisListJS() throws CorbException{
        clearSystemProperties();
        int uriCount = 10;
        String collectionName = "testManagerMetricsPeriodicSyncUsingSysPropsLargeUrisListJS";
        String exportFilename = "testManagerMetricsUsingSysProps1.txt";
        String extension = JSON_EXT;
        String syncFrequency = "2";
        testManager(uriCount, collectionName, exportFilename, JS_MODULE, syncFrequency, extension);
    }

    @Test
    public void testManagerMetricsPeriodicSyncUsingSysPropsLargeUrisListXQUERY() throws CorbException{
        clearSystemProperties();
        int uriCount = 10;
        String collectionName = "testManagerMetricsPeriodicSyncUsingSysPropsLargeUrisListXQUERY";
        String exportFilename = "testManagerMetricsUsingSysProps1.txt";
        String extension = XML_EXT;
        String syncFrequency = "2";
        testManager(uriCount, collectionName, exportFilename, XQUERY_MODULE, syncFrequency, extension);
    }

    @Test
    public void testManagerMetricsNOPeriodicSyncJS() throws CorbException{
        clearSystemProperties();
        int uriCount = 10;
        String collectionName = "testManagerMetricsNOPeriodicSyncJS";
        String exportFilename = "testManagerMetricsNOPeriodicSync.txt";
        testManager(uriCount, collectionName, exportFilename, JS_MODULE, null, JSON_EXT);
    }

    @Test
    public void testManagerMetricsNOPeriodicSyncXQUERY() throws CorbException{
        clearSystemProperties();
        int uriCount = 10;
        String collectionName = "testManagerMetricsNOPeriodicSyncXQUERY";
        String exportFilename = "testManagerMetricsNOPeriodicSync.txt";
        testManager(uriCount, collectionName, exportFilename, null, null, XML_EXT);
    }

    public static void cleanupDocs(ContentSourcePool contentSourcePool, String collection, String dbName) throws CorbException{
        try (Session session = contentSourcePool.get().newSession()) {
            AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_ML
                    + "xdmp:invoke-function(function(){xdmp:collection-delete('" + collection
                    + "')}, <options  xmlns='xdmp:eval'><database>{xdmp:database('" + dbName
                    + "')}</database><transaction-mode>update-auto-commit</transaction-mode></options>)");
            session.submitRequest(q);
        } catch (RequestException e) {
            LOG.log(SEVERE, "registerStatusInfo request failed", e);
        }
    }

    public static List<String> collectionCount(ContentSourcePool contentSourcePool, String collection, String dbName) throws CorbException{
        List<String> result = new ArrayList<>();
        try (Session session = contentSourcePool.get().newSession()) {
            AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_ML
                    + "xdmp:invoke-function(function(){cts:uris((),(),cts:collection-query('" + collection
                    + "'))}, <options  xmlns='xdmp:eval'><database>{xdmp:database('" + dbName
                    + "')}</database><transaction-mode>update-auto-commit</transaction-mode></options>)");
            ResultSequence rs = session.submitRequest(q);
            result.addAll(Arrays.asList(rs.asStrings()));
        } catch (RequestException e) {
            LOG.log(SEVERE, "registerStatusInfo request failed", e);
        }
        return result;
    }

    public static List<String> docsWithEndTime(ContentSourcePool contentSourcePool, String collection, String dbName, boolean isXML) throws CorbException{
        List<String> result = new ArrayList<>();
        try (Session session = contentSourcePool.get().newSession()) {
            AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_ML + "declare namespace corb='" + JobStats.CORB_NAMESPACE + "';"
                    + "xdmp:invoke-function(function(){cts:uris((),(),cts:and-query(("
                    + "		cts:element-query(xs:QName('"
                    + (isXML ? "corb:" : "")
                    + "endTime'),cts:true-query()),"
                    + "	cts:collection-query('" + collection + "'))))}, <options  xmlns='xdmp:eval'><database>{xdmp:database('" + dbName
                    + "')}</database><transaction-mode>update-auto-commit</transaction-mode></options>)");
            ResultSequence rs = session.submitRequest(q);
            result.addAll(Arrays.asList(rs.asStrings()));
        } catch (RequestException e) {
            LOG.log(SEVERE, "registerStatusInfo request failed", e);
        }
        return result;
    }

    private void testManager(int uriCount, String collectionName, String exportFilename,
                             String METRICS_MODULE, String syncFrequency, String extension) throws CorbException{
        Properties properties = getMetricsTestProperties(uriCount, collectionName, exportFilename, METRICS_MODULE, syncFrequency);

        Manager manager = new Manager();
        try {
            manager.init(properties);
            cleanupDocs(manager.csp, collectionName, METRICS_DB_NAME);
            manager.run();
            File report = new File(ManagerTest.EXPORT_FILE_DIR + SLASH + exportFilename);
            report.deleteOnExit();
            int lineCount = FileUtils.getLineCount(report);
            assertEquals(uriCount / 2 + 2, lineCount);
            List<String> uris = collectionCount(manager.csp, collectionName, METRICS_DB_NAME);
            if (syncFrequency == null) {
                assertTrue(uris.size() == 1);//more than one as it will log periodically
            } else {
                assertTrue(uris.size() > 1);//more than one as it will log periodically
            }
            //Check all uris have the same extension
            for (String uri : uris) {
                assertTrue("Extension should be " + extension, uri.contains(extension));
            }
            List<String> urisWithEndTime = docsWithEndTime(manager.csp, collectionName, METRICS_DB_NAME, XML_EXT.equals(extension));
            assertTrue("Only one URI with End Time", urisWithEndTime.size() == 1);
        } catch (Exception ex) {
            LOG.log(SEVERE, null, ex);
            fail();
        } finally {
            cleanupDocs(manager.csp, collectionName, METRICS_DB_NAME);
        }
    }

    private Properties getMetricsTestProperties(int uriCount, String collectionName, String exportFilename,
            String METRICS_MODULE, String syncFrequency) {
        Properties properties = ManagerTest.getDefaultProperties();
        properties.setProperty(Options.THREAD_COUNT, String.valueOf(4));
        properties.setProperty(Options.URIS_MODULE, LARGE_URIS_MODULE);
        properties.setProperty(Options.URIS_MODULE + ".count", String.valueOf(uriCount));
        properties.setProperty(Options.BATCH_SIZE, String.valueOf(1));
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        properties.setProperty(Options.DISK_QUEUE, "true");
        properties.setProperty(Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(10));
        properties.setProperty(Options.DISK_QUEUE_TEMP_DIR, "/var/tmp");
        properties.setProperty(Options.PROCESS_MODULE, TRANSFORM_ERROR_MODULE);
        properties.setProperty(Options.FAIL_ON_ERROR, "false");
        if (syncFrequency != null) {
            properties.setProperty(Options.METRICS_SYNC_FREQUENCY, syncFrequency);
        }
        properties.setProperty(Options.METRICS_DATABASE, METRICS_DB_NAME);
        properties.setProperty(Options.METRICS_COLLECTIONS, collectionName);
        if (METRICS_MODULE != null) {
            properties.setProperty(Options.METRICS_MODULE, METRICS_MODULE);
        }
        return properties;
    }
}
