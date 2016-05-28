/*
 * * Copyright (c) 2004-2016 MarkLogic Corporation
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
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.QueryStackFrame;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import com.marklogic.xcc.exceptions.RequestServerException;
import com.marklogic.xcc.exceptions.RetryableJavaScriptException;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractTaskTest {

    private final TestHandler testLogger = new TestHandler();
    private static final Logger logger = Logger.getLogger(AbstractTask.class.getName());

    @Before
    public void setUp() {
        clearSystemProperties();
        logger.addHandler(testLogger);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    /**
     * Test of setContentSource method, of class AbstractTask.
     */
    @Test
    public void testSetContentSource() {
        ContentSource cs = mock(ContentSource.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.setContentSource(cs);
        assertEquals(cs, instance.cs);
    }

    /**
     * Test of setModuleType method, of class AbstractTask.
     */
    @Test
    public void testSetModuleType() {
        String moduleType = "foo";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleType(moduleType);
        assertEquals(moduleType, instance.moduleType);
    }

    /**
     * Test of setModuleURI method, of class AbstractTask.
     */
    @Test
    public void testSetModuleURI() {
        String moduleUri = "test.xqy";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleURI(moduleUri);
        assertEquals(moduleUri, instance.moduleUri);
    }

    /**
     * Test of setAdhocQuery method, of class AbstractTask.
     */
    @Test
    public void testSetAdhocQuery() {
        String adhocQuery = "adhoc.xqy";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setAdhocQuery(adhocQuery);
        assertEquals(adhocQuery, instance.adhocQuery);
    }

    /**
     * Test of setQueryLanguage method, of class AbstractTask.
     */
    @Test
    public void testSetQueryLanguage() {
        String language = "XQuery";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setQueryLanguage(language);
        assertEquals(language, instance.language);
    }

    /**
     * Test of setProperties method, of class AbstractTask.
     */
    @Test
    public void testSetProperties() {
        Properties props = new Properties();
        Properties properties = props;
        AbstractTask instance = new AbstractTaskImpl();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
    }

    /**
     * Test of setInputURI method, of class AbstractTask.
     */
    @Test
    public void testSetInputURI() {
        String[] inputUri = {"foo", "bar", "baz"};
        AbstractTask instance = new AbstractTaskImpl();
        instance.setInputURI(inputUri);
        Assert.assertArrayEquals(inputUri, instance.inputUris);
    }

    @Test
    public void testSetInputURI_null() {
        AbstractTask instance = new AbstractTaskImpl();
        assertNull(instance.inputUris);
        instance.setInputURI(null);
        assertNotNull(instance.inputUris);
    }

    /**
     * Test of setFailOnError method, of class AbstractTask.
     */
    @Test
    public void testSetFailOnError() {
        AbstractTask instance = new AbstractTaskImpl();
        instance.setFailOnError(false);
        assertFalse(instance.failOnError);
        instance.setFailOnError(true);
        assertTrue(instance.failOnError);
    }

    /**
     * Test of setExportDir method, of class AbstractTask.
     */
    @Test
    public void testSetExportDir() {
        String exportFileDir = "/tmp";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setExportDir(exportFileDir);
        assertEquals(exportFileDir, instance.exportDir);
    }

    /**
     * Test of getExportDir method, of class AbstractTask.
     */
    @Test
    public void testGetExportDir() {
        AbstractTask instance = new AbstractTaskImpl();
        String expResult = "/tmp";
        instance.exportDir = expResult;
        String result = instance.getExportDir();
        assertEquals(expResult, result);
    }

    /**
     * Test of newSession method, of class AbstractTask.
     */
    @Test
    public void testNewSession() {
        AbstractTask instance = new AbstractTaskImpl();
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(cs.newSession()).thenReturn(session);
        Session expResult = session;
        instance.cs = cs;
        Session result = instance.newSession();
        assertEquals(expResult, result);
    }

    /**
     * Test of invokeModule method, of class AbstractTask.
     */
    @Test
    public void testInvokeModule() throws Exception {
        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = "module.xqy";
        instance.adhocQuery = "adhoc.xqy";
        instance.inputUris = new String[]{"foo", "bar", "baz"};
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        ResultSequence seq = mock(ResultSequence.class);

        when(cs.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(null);
        when(session.submitRequest(request)).thenReturn(seq);

        instance.cs = cs;
        instance.moduleType = "foo";
        Properties props = new Properties();
        props.setProperty("foo.bar", "baz");
        props.setProperty("foo.baz", "boo");
        props.setProperty("BATCH-URI-DELIM", "");
        instance.properties = props;

        instance.inputUris = new String[]{"uri1", "uri2"};
        instance.invokeModule();
        assertTrue(AbstractTask.MODULE_PROPS.get("foo").contains("foo.bar"));
        assertTrue(AbstractTask.MODULE_PROPS.get("foo").contains("foo.baz"));
    }

    @Test
    public void testGetIntProperty() {
        Properties props = new Properties();
        props.setProperty("one", "one");
        props.setProperty("two", "2");
        props.setProperty("three", "");
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        assertEquals(-1, instance.getIntProperty("one"));
        assertEquals(2, instance.getIntProperty("two"));
        assertEquals(-1, instance.getIntProperty("three"));
        assertEquals(-1, instance.getIntProperty("four"));
    }

    @Test
    public void testInvokeModule_RetryableXQueryException() throws Exception {
        Request req = mock(Request.class);
        RetryableXQueryException retryableException = new RetryableXQueryException(req, "code", "401", "1.0-ml", "something bad happened", "", "", true, new String[0], new QueryStackFrame[0]);
        testHandleRequestException("RetryableXQueryException", retryableException, false, 2);
    }

    @Test
    public void testInvokeModule_XQueryException() throws Exception {
        Request req = mock(Request.class);
        XQueryException xqueryException = new XQueryException(req, "code", "401", "1.0-ml", "something bad happened", "", "", true, new String[0], new QueryStackFrame[0]);
        testHandleRequestException("XQueryException", xqueryException, false, 2);
    }

    @Test
    public void testInvokeModule_RetryableJavaScriptException() throws Exception {
        Request req = mock(Request.class);
        RetryableJavaScriptException retryableException = new RetryableJavaScriptException(req, "code", "401", "something bad happened", "", "", true, new String[0], new QueryStackFrame[0]);
        testHandleRequestException("RetryableJavaScriptException", retryableException, false, 2);
    }

    @Test
    public void testHandleRequestException_RequestServerException() throws CorbException, IOException {
        Request req = mock(Request.class);
        RequestServerException serverException = new RequestServerException("something bad happened", req);
        testHandleRequestException("RequestServerException", serverException, false, 2);
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestException_RequestServerException_fail() throws CorbException, IOException {
        Request req = mock(Request.class);
        RequestServerException serverException = new RequestServerException("something bad happened", req);
        testHandleRequestException("RequestServerException", serverException, true, 0);
        fail();
    }

    @Test
    public void testHandleRequestException_RequestPermissionException() throws CorbException, IOException {
        Request req = mock(Request.class);
        RequestPermissionException serverException = new RequestPermissionException("something bad happened", req, "admin");
        testHandleRequestException("RequestPermissionException", serverException, false, 2);
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestException_RequestPermissionException_fail() throws CorbException, IOException {
        Request req = mock(Request.class);
        RequestPermissionException serverException = new RequestPermissionException("something bad happened", req, "admin");
        testHandleRequestException("RequestPermissionException", serverException, true, 2);
        fail();
    }

    @Test
    public void testHandleRequestException_ServerConnectionException() throws CorbException, IOException {
        Request req = mock(Request.class);
        ServerConnectionException serverException = new ServerConnectionException("something bad happened", req);
        testHandleRequestException("ServerConnectionException", serverException, false, 2);
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestException_ServerConnectionException_fail() throws CorbException, IOException {
        Request req = mock(Request.class);
        ServerConnectionException serverException = new ServerConnectionException("something bad happened", req);
        testHandleRequestException("ServerConnectionException", serverException, true, 0);
        fail();
    }

    @Test
    public void testShouldRetryNotRetryableQueryExceptionCSVwithSpaces() {

        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "foo, SVC-EXTIME, XDMP-EXTIME, bar");
        XQueryException exception = new XQueryException(req, "SVC-EXTIME", "401", "1.0-ml", "timeout", "", "", false, new String[0], new QueryStackFrame[0]);
        assertTrue(instance.shouldRetry(exception));
    }

    @Test
    public void testShouldRetryNotRetryableQueryException() {

        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-FOO,SVC-BAR,XDMP-BAZ");
        XQueryException exception = new XQueryException(req, "SVC-EXTIME", "401", "1.0-ml", "timeout", "", "", false, new String[0], new QueryStackFrame[0]);

        assertFalse(instance.shouldRetry(exception));

        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-EXTIME,XDMP-EXTIME");

        assertTrue(instance.shouldRetry(exception));

        instance.properties.remove(Options.QUERY_RETRY_ERROR_CODES);

        assertFalse(instance.shouldRetry(exception)); //no match on code(and no exception attempting to split null)
    }

    @Test
    public void testShouldRetryRetryableQueryException() {

        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-FOO,SVC-BAR,XDMP-BAZ");
        XQueryException exception = new XQueryException(req, "SVC-EXTIME", "401", "1.0-ml", "timeout", "", "", true, new String[0], new QueryStackFrame[0]);

        assertTrue(instance.shouldRetry(exception)); //since it's retryable, doesn't matter if code matches

        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-EXTIME,XDMP-EXTIME");
        assertTrue(instance.shouldRetry(exception)); //is retryable and the code matches

        instance.properties.remove(Options.QUERY_RETRY_ERROR_CODES);
        assertTrue(instance.shouldRetry(exception));
    }

    @Test
    public void testShouldRetryRequestPermissionException() {

        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-FOO,SVC-BAR,XDMP-BAZ");
        RequestPermissionException exception = new RequestPermissionException("denied!", req, "user-name", false);
        assertFalse(instance.shouldRetry(exception));

        exception = new RequestPermissionException("denied!", req, "user-name", true);
        assertTrue(instance.shouldRetry(exception));
    }

    @Test
    public void testHasRetryableMessage() {
        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_MESSAGE, "FOO,Authentication failure for user,BAR");
        RequestPermissionException exception = new RequestPermissionException("denied!", req, "user-name", false);
        assertFalse(instance.hasRetryableMessage(exception));

        exception = new RequestPermissionException("Authentication failure for user 'user-name'", req, "user-name", false);
        assertTrue(instance.hasRetryableMessage(exception));
    }

    public void testHandleRequestException(String type, RequestException exception, boolean fail, int retryLimit)
            throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        testHandleRequestException(type, exception, fail, uris, retryLimit);
    }

    public void testHandleRequestException(String type, RequestException exception, boolean fail, String[] uris, int retryLimit)
            throws CorbException, IOException {
        File exportDir = TestUtils.createTempDirectory();
        File exportFile = File.createTempFile("error", ".err", exportDir);
        testHandleRequestException(type, exception, fail, uris, null, exportDir, exportFile.getName(), retryLimit);
    }

    public void testHandleRequestException(String type, RequestException exception, boolean fail, String[] uris, String delim, File exportDir, String errorFilename, int retryLimit)
            throws CorbException, IOException {
        if (exportDir == null) {
            exportDir = TestUtils.createTempDirectory();
            exportDir.deleteOnExit();
        }
        AbstractTask instance = new AbstractTaskImpl();
        instance.failOnError = fail;
        instance.inputUris = uris;
        instance.exportDir = exportDir.getAbsolutePath();
        instance.properties = new Properties();
        if (errorFilename != null) {
            instance.properties.setProperty("ERROR-FILE-NAME", errorFilename);
        }
        if (delim != null) {
            instance.properties.setProperty("BATCH-URI-DELIM", delim);
        }

        instance.properties.setProperty("XCC-CONNECTION-RETRY-INTERVAL", "1");
        instance.properties.setProperty("XCC-CONNECTION-RETRY-LIMIT", Integer.toString(retryLimit));

        instance.properties.setProperty("QUERY-RETRY-INTERVAL", "1");
        instance.properties.setProperty("QUERY-RETRY-LIMIT", Integer.toString(retryLimit));

        instance.handleRequestException(exception);
        List<LogRecord> records = testLogger.getLogRecords();
        System.out.println("logrecords " + records.size());
        assertEquals(Level.WARNING, records.get(0).getLevel());
        if (!(instance.shouldRetry(exception))) {
            assertEquals("failOnError is false. Encountered " + type + " at URI: " + instance.asString(uris), records.get(0).getMessage());
        }
    }

    public File testWriteToError(String[] uris, String delim, File exportDir, String errorFilename, String message)
            throws CorbException, IOException {
        Request req = mock(Request.class);
        RequestServerException serverException = new RequestServerException(message, req);
        testHandleRequestException("RequestServerException", serverException, false, uris, delim, exportDir, errorFilename, 0);
        File file = null;
        try {
            file = new File(exportDir, errorFilename);
        } catch (Exception e) {
        }
        return file;
    }

    @Test
    public void testWriteToErrorFile_nullUris() throws CorbException, IOException {
        String[] uris = null;
        File exportDir = TestUtils.createTempDirectory();
        exportDir.deleteOnExit();
        String filename = "testWriteToErrorFile_nullUris.error";
        String delim = null;
        String message = null;
        testWriteToError(uris, delim, exportDir, filename, message);
        File file = new File(exportDir, filename);
        assertFalse(file.exists());
    }

    @Test
    public void testWriteToErrorFile_emptyUris() throws CorbException, IOException {
        String[] uris = new String[]{};
        File exportDir = TestUtils.createTempDirectory();
        exportDir.deleteOnExit();
        String filename = "testWriteToErrorFile_emptyUris.error";
        String delim = null;
        String message = null;
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        assertFalse(errorFile.exists());
    }

    @Test
    public void testWriteToErrorFile_nullErrorFilename() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        exportDir.deleteOnExit();
        String filename = null;
        String delim = null;
        String message = "ERROR";
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        assertNull(errorFile);
    }

    @Test
    public void testWriteToErrorFile_emptyErrorFilename() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        exportDir.deleteOnExit();
        String filename = "";
        String delim = null;
        String message = "ERROR";
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        //testWriteToError constructs a File object that is the containing directory when filename is blank
        assertFalse(errorFile.isFile());
    }

    @Test
    public void testWriteToErrorFile_nullBatchUridelim() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        exportDir.deleteOnExit();
        String filename = "testWriteToErrorFile_nullBatchUridelim.err";
        String delim = null;
        String message = "ERROR";
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        assertTrue(TestUtils.readFile(errorFile).contains(Manager.DEFAULT_BATCH_URI_DELIM));
    }

    @Test
    public void testWriteToErrorFile_emptyBatchUridelim() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        exportDir.deleteOnExit();
        String filename = "testWriteToErrorFile_emptyBatchUridelim.err";
        String delim = null;
        String message = "ERROR";
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        assertTrue(TestUtils.readFile(errorFile).contains(Manager.DEFAULT_BATCH_URI_DELIM));
    }

    @Test
    public void testWriteToErrorFile_customBatchUridelim() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        String filename = "testWriteToErrorFile_customBatchUridelim.err";
        String delim = "$";
        String message = "ERROR";
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);

        assertTrue(TestUtils.readFile(errorFile).contains(delim));
    }

    @Test
    public void testWriteToErrorFile_nullMessage() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        String filename = "testWriteToErrorFile_customBatchUridelim.err";
        String delim = "$";
        String message = null;
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        assertFalse(TestUtils.readFile(errorFile).contains(delim));
    }

    @Test
    public void testWriteToErrorFile_emptyMessage() throws CorbException, IOException {
        String[] uris = new String[]{"uri1"};
        File exportDir = TestUtils.createTempDirectory();
        String filename = "testWriteToErrorFile_customBatchUridelim.err";
        String delim = "$";
        String message = "";
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);

        assertFalse(TestUtils.readFile(errorFile).contains(delim));
    }

    /**
     * Test of asString method, of class AbstractTask.
     */
    @Test
    public void testAsString() {
        String[] uris = new String[]{"foo", "bar", "baz"};
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.asString(uris);
        assertEquals("foo,bar,baz", result);
    }

    @Test
    public void testAsString_emptyArray() {
        String[] uris = new String[]{};
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.asString(uris);
        assertEquals("", result);
    }

    @Test
    public void testAsString_null() {
        String[] uris = null;
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.asString(uris);
        assertEquals("", result);
    }

    /**
     * Test of cleanup method, of class AbstractTask.
     */
    @Test
    public void testCleanup() {
        AbstractTask instance = new AbstractTaskImpl();
        instance.cs = mock(ContentSource.class);
        instance.moduleType = "moduleType";
        instance.moduleUri = "moduleUri";
        instance.properties = new Properties();
        instance.inputUris = new String[]{};
        instance.adhocQuery = "adhocQuery";
        instance.cleanup();
        assertNull(instance.cs);
        assertNull(instance.moduleType);
        assertNull(instance.moduleUri);
        assertNull(instance.properties);
        assertNull(instance.inputUris);
        assertNull(instance.adhocQuery);
    }

    /**
     * Test of getProperty method, of class AbstractTask.
     */
    @Test
    public void testGetProperty() {
        String key = "INIT-TASK";
        String val = "foo";
        Properties props = new Properties();
        props.setProperty(key, val);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        String result = instance.getProperty(key);
        assertEquals(val, result);
    }

    @Test
    public void testGetProperty_systemPropertyTakesPrecedence() {
        String key = "INIT-TASK";
        String val = "foo";
        System.setProperty(key, val);
        Properties props = new Properties();
        props.setProperty(key, "bar");
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        String result = instance.getProperty(key);
        assertEquals(val, result);
        clearSystemProperties();
    }

    /**
     * Test of getValueAsBytes method, of class AbstractTask.
     */
    @Test
    public void testGetValueAsBytes_xdmBinary() {
        XdmItem item = mock(XdmBinary.class);

        AbstractTask instance = new AbstractTaskImpl();
        byte[] result = instance.getValueAsBytes(item);
        assertNull(result);
    }

    @Test
    public void testGetValueAsBytes_xdmItem() {
        XdmItem item = mock(XdmItem.class);
        String value = "foo";
        when(item.asString()).thenReturn(value);
        AbstractTask instance = new AbstractTaskImpl();
        byte[] result = instance.getValueAsBytes(item);
        Assert.assertArrayEquals(value.getBytes(), result);
    }

    @Test
    public void testGetValueAsBytes_default() {
        XdmItem item = null;
        AbstractTask instance = new AbstractTaskImpl();
        byte[] result = instance.getValueAsBytes(item);
        Assert.assertArrayEquals(new byte[]{}, result);
    }

    private static class AbstractTaskImpl extends AbstractTask {

        @Override
        public String processResult(ResultSequence seq) throws CorbException {
            return "";
        }

        @Override
        public String[] call() throws Exception {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }

}
