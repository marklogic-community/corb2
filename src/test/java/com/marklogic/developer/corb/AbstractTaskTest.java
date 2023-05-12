/*
 * * Copyright (c) 2004-2023 MarkLogic Corporation
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
import static com.marklogic.developer.corb.Options.INIT_MODULE;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_ERROR_MESSAGE;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.ValueFactory;
import com.marklogic.xcc.exceptions.QueryStackFrame;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import com.marklogic.xcc.exceptions.RequestServerException;
import com.marklogic.xcc.exceptions.RetryableJavaScriptException;
import com.marklogic.xcc.exceptions.RetryableXQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.exceptions.XQueryException;
import com.marklogic.xcc.impl.SessionImpl;
import com.marklogic.xcc.types.ValueType;
import com.marklogic.xcc.types.XName;
import com.marklogic.xcc.types.XSString;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
import com.marklogic.xcc.types.XdmValue;
import com.marklogic.xcc.types.XdmVariable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
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
    private static final Logger LOG = Logger.getLogger(AbstractTask.class.getName());
    private static final String XQUERY_VERSION = "1.0-ml";
    private static final String URI = "/doc.xml";
    private static final String ERROR = "ERROR";
    private static final String FOO = "foo";
    private static final String BAR = "bar";
    private static final String BAZ = "baz";
    protected static final String USER_NAME = "user-name";
    private static final String W3C_CODE = "401";
    private static final String CODE = "code";
    private static final String ERROR_MSG = "something bad happened";
    private static final String SVC_EXTIME = "SVC-EXTIME";
    private static final String ADMIN = "admin";
    private static final String ONE = "one";
    private static final String TWO = "two";
    private static final String THREE = "three";
    protected static final String REJECTED_MSG = "denied!";
    private static final String TMP_DIR = "/tmp";
    private static final String ADHOC_MODULE = "adhoc.xqy";
    private static final String EMPTY_DOC_ELEMENT = "<doc/>";

    @Before
    public void setUp() {
        clearSystemProperties();
        LOG.addHandler(testLogger);
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    @Test
    public void testSetContentSource() {
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        AbstractTask task = new AbstractTaskImpl();
        task.setContentSourcePool(contentSourcePool);
        assertEquals(contentSourcePool, task.csp);
    }

    @Test
    public void testSetModuleType() {
        String moduleType = FOO;
        AbstractTask task = new AbstractTaskImpl();
        task.setModuleType(moduleType);
        assertEquals(moduleType, task.moduleType);
    }

    @Test
    public void testSetModuleURI() {
        String moduleUri = "test.xqy";
        AbstractTask task = new AbstractTaskImpl();
        task.setModuleURI(moduleUri);
        assertEquals(moduleUri, task.moduleUri);
    }

    @Test
    public void testSetAdhocQuery() {
        String adhocQuery = ADHOC_MODULE;
        AbstractTask task = new AbstractTaskImpl();
        task.setAdhocQuery(adhocQuery);
        assertEquals(adhocQuery, task.adhocQuery);
    }

    @Test
    public void testSetQueryLanguage() {
        String language = "XQuery";
        AbstractTask task = new AbstractTaskImpl();
        task.setQueryLanguage(language);
        assertEquals(language, task.language);
    }

    @Test
    public void testSetProperties() {
        Properties properties = new Properties();
        AbstractTask task = new AbstractTaskImpl();
        task.setProperties(properties);
        assertEquals(properties, task.properties);
    }

    @Test
    public void testSetInputURI() {
        String[] inputUri = {FOO, BAR, BAZ};
        AbstractTask task = new AbstractTaskImpl();
        task.setInputURI(inputUri);
        assertArrayEquals(inputUri, task.inputUris);
    }

    @Test
    public void testSetInputURINull() {
        AbstractTask task = new AbstractTaskImpl();
        assertNull(task.inputUris);
        task.setInputURI((String[]) null);
        assertNotNull(task.inputUris);
    }

    @Test
    public void testSetFailOnError() {
        AbstractTask task = new AbstractTaskImpl();
        task.setFailOnError(false);
        assertFalse(task.failOnError);
        task.setFailOnError(true);
        assertTrue(task.failOnError);
    }

    @Test
    public void testSetExportDir() {
        String exportFileDir = TMP_DIR;
        AbstractTask task = new AbstractTaskImpl();
        task.setExportDir(exportFileDir);
        assertEquals(exportFileDir, task.exportDir);
    }

    @Test
    public void testGetExportDir() {
        AbstractTask task = new AbstractTaskImpl();
        String expResult = TMP_DIR;
        task.exportDir = expResult;
        String result = task.getExportDir();
        assertEquals(expResult, result);
    }

    @Test
    public void testNewSession() throws CorbException{
        AbstractTask task = new AbstractTaskImpl();
        ContentSourcePool csp = mock(ContentSourcePool.class);
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(csp.get()).thenReturn(cs);
        when(cs.newSession()).thenReturn(session);
        task.csp = csp;
        Session result = task.newSession();
        assertEquals(session, result);
    }

    @Test
    public void testInvokeModule() {
        String[] inputUris = new String[]{FOO, BAR, BAZ};
        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = "module.xqy";
        task.adhocQuery = ADHOC_MODULE;
        task.inputUris = inputUris;

        try {
            ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
            ContentSource cs = mock(ContentSource.class);
            Session session = mock(Session.class);
            ModuleInvoke request = mock(ModuleInvoke.class);
            ResultSequence seq = mock(ResultSequence.class);

            when(contentSourcePool.get()).thenReturn(cs);
            when(cs.newSession()).thenReturn(session);
            when(session.newModuleInvoke(anyString())).thenReturn(request);
            when(request.setNewStringVariable(anyString(), anyString())).thenReturn(null);
            when(session.submitRequest(request)).thenReturn(seq);
            String key1 = "foo.bar";
            String key2 = "foo.baz";
            task.csp = contentSourcePool;
            task.moduleType = FOO;
            Properties props = new Properties();
            props.setProperty(key1, BAZ);
            props.setProperty(key2, "boo");
            props.setProperty(Options.BATCH_URI_DELIM, "");
            task.properties = props;

            task.inputUris = new String[]{URI, "uri2"};
            String[] result = task.invokeModule();
            assertArrayEquals(task.inputUris, result);
        } catch (RequestException | CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testGenerateRequestModuleInvoke() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.language = "JavaScript";
        task.timeZone = TimeZone.getDefault();
        task.inputUris = new String[]{"a", "b", "c"};
        task.setModuleType(INIT_MODULE);
        task.properties.setProperty(INIT_MODULE + '.' + FOO, BAR);

        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        RequestOptions options = request.getOptions();
        assertEquals(task.language, options.getQueryLanguage());
        assertEquals(task.timeZone, options.getTimeZone());
        List<XdmVariable> variableList = Arrays.asList(request.getVariables());

        XdmVariable uriVariable = buildStringXdmVariable("URI", "a;b;c");
        assertTrue(variableList.contains(uriVariable));

        XdmVariable customInputVariable = buildStringXdmVariable(FOO, BAR);
        assertTrue(variableList.contains(customInputVariable));
    }

    @Test
    public void testGenerateRequestDocLoader() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        String[] uris = new String[]{"<doc1/>", "<doc2/>", "<doc3/>"};
        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_DOC);
        task.inputUris = uris;

        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        List<XdmVariable> variableList = Arrays.asList(request.getVariables());
        assertEquals(1, variableList.size());
        XdmValue value = variableList.get(0).getValue();
        assertNotNull(value);
        assertTrue(value instanceof XdmItem);
    }

    @Test
    public void testGenerateRequestURILoaderWithXML() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_URI);
        task.inputUris = new String[]{EMPTY_DOC_ELEMENT};

        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        List<XdmVariable> variableList = Arrays.asList(request.getVariables());
        assertEquals(1, variableList.size());
        assertEquals(EMPTY_DOC_ELEMENT, variableList.get(0).getValue().asString());
    }

    @Test
    public void testGenerateRequestURILoaderWithMultipleXMLStringValues() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_URI);
        task.inputUris = new String[]{EMPTY_DOC_ELEMENT, EMPTY_DOC_ELEMENT, EMPTY_DOC_ELEMENT};

        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        List<XdmVariable> variableList = Arrays.asList(request.getVariables());
        assertEquals(1, variableList.size());

        assertEquals(String.join(Manager.DEFAULT_BATCH_URI_DELIM, task.inputUris), variableList.get(0).getValue().asString());
    }

    @Test
    public void testGenerateRequestModuleInvokeSetLanguage() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.language = "Sparql"; //not currently supported, just verifying that it accepts any string
        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        RequestOptions options = request.getOptions();
        assertEquals(task.language, options.getQueryLanguage());
    }

    @Test
    public void testGenerateRequestModuleInvokeSetTimeZone() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.timeZone = TimeZone.getTimeZone("PST");
        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        RequestOptions options = request.getOptions();
        assertEquals(task.timeZone, options.getTimeZone());
    }

    @Test
    public void testGenerateRequestModuleInvokeWithCustomInputs() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.setModuleType(INIT_MODULE);
        task.properties.setProperty(INIT_MODULE + '.' + FOO, BAR);
        task.properties.setProperty(Options.POST_BATCH_MODULE + '.' + BAZ, BAR);
        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        List<XdmVariable> variableList = Arrays.asList(request.getVariables());

        XdmVariable customInputVariable = buildStringXdmVariable(FOO, BAR);
        XdmVariable customInputVariableBaz = buildStringXdmVariable(BAZ, BAR);

        assertTrue(variableList.contains(customInputVariable));
        assertFalse(variableList.contains(customInputVariableBaz)); //verify that only custom inputs for this moduleType are set
    }

    @Test
    public void testGenerateRequestModuleInvokeWithUrisBatchRef() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        task.moduleUri = URI;
        task.setModuleType(INIT_MODULE);
        task.properties.setProperty(Options.URIS_BATCH_REF, BAZ);
        try {
            task.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        List<XdmVariable> variableList = Arrays.asList(request.getVariables());

        XdmVariable customInputVariable = buildStringXdmVariable(Options.URIS_BATCH_REF, BAR);
        assertTrue(variableList.contains(customInputVariable));
    }

    @Test
    public void testGenerateRequestModuleInvokeWithoutModuleUri() {
        AdhocQuery request = new AdhocQueryImpl();
        Session session = mock(Session.class);
        when(session.newAdhocQuery(anyString())).thenReturn(request);

        AbstractTask task = new AbstractTaskImpl();
        try {
            Request requestResult = task.generateRequest(session);
            assertTrue(requestResult instanceof AdhocQuery);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private XdmVariable buildStringXdmVariable(String name, String value) {
        XName xName = new XName(name);
        XSString xValue = ValueFactory.newXSString(value);
        return ValueFactory.newVariable(xName, xValue);
    }

    @Test
    public void testGetCustomInputPropertyNames() {
        String key1 = FOO + ".bar";
        String key2 = FOO + ".baz";
        AbstractTask task = new AbstractTaskImpl();
        task.setModuleType(FOO);
        System.setProperty(key1, BAZ);
        Properties props = new Properties();
        props.setProperty(key1, BAZ);
        props.setProperty(key2, "boo");
        props.setProperty(Options.BATCH_URI_DELIM, "");
        task.properties = props;
        Set<String> inputs = task.getCustomInputPropertyNames();
        assertEquals(2, inputs.size());
        assertTrue(inputs.contains(key1));
        assertTrue(inputs.contains(key2));
        System.clearProperty(key1);
    }

    @Test
    public void testGetIntProperty() {
        Properties props = new Properties();
        props.setProperty(ONE, ONE);
        props.setProperty(TWO, "2");
        props.setProperty(THREE, "");
        AbstractTask task = new AbstractTaskImpl();
        task.properties = props;
        assertEquals(-1, task.getIntProperty(ONE));
        assertEquals(2, task.getIntProperty(TWO));
        assertEquals(-1, task.getIntProperty(THREE));
        assertEquals(-1, task.getIntProperty("four"));
    }

    @Test
    public void testInvokeModuleRetryableXQueryException() {
        Request req = mock(Request.class);
        RetryableXQueryException retryableException = new RetryableXQueryException(req, CODE, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", true, new String[0], new QueryStackFrame[0]);
        try {
            assertTrue(testHandleRequestException(retryableException, false, 2));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInvokeModuleXQueryException() {
        Request req = mock(Request.class);
        XQueryException xqueryException = new XQueryException(req, CODE, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", true, new String[0], new QueryStackFrame[0]);
        try {
            assertTrue(testHandleRequestException(xqueryException, false, 2));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testInvokeModuleRetryableJavaScriptException() {
        Request req = mock(Request.class);
        RetryableJavaScriptException retryableException = new RetryableJavaScriptException(req, CODE, W3C_CODE, ERROR_MSG, "", "", true, new String[0], new QueryStackFrame[0]);
        try {
            assertTrue(testHandleRequestException(retryableException, false, 2));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testHandleRequestExceptionRequestServerException() {
        Request req = mock(Request.class);
        RequestServerException serverException = new RequestServerException(ERROR_MSG, req);
        try {
            assertTrue(testHandleRequestException(serverException, false, 2));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestExceptionRequestServerExceptionFail() throws CorbException {
        Request req = mock(Request.class);
        RequestServerException serverException = new RequestServerException(ERROR_MSG, req);
        try {
            testHandleRequestException(serverException, true, 0);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testHandleRequestExceptionRequestPermissionException() {
        Request req = mock(Request.class);
        RequestPermissionException serverException = new RequestPermissionException(ERROR_MSG, req, ADMIN);
        try {
            assertTrue(testHandleRequestException(serverException, false, 2));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestExceptionRequestPermissionExceptionFail() throws CorbException {
        Request req = mock(Request.class);
        RequestPermissionException serverException = new RequestPermissionException(ERROR_MSG, req, ADMIN);
        try {
            testHandleRequestException(serverException, true, 2);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestExceptionServerConnectionExceptionFail() throws CorbException {
        Request req = mock(Request.class);
        ServerConnectionException serverException = new ServerConnectionException(ERROR_MSG, req);
        try {
            testHandleRequestException(serverException, true, 0);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testHandleRequestExceptionRequestExceptionWithServerConnectionExceptionNoFail() throws CorbException {
        Request req = mock(Request.class);
        SessionImpl session = mock(SessionImpl.class);
        when(req.getSession()).thenReturn(session);
        when(session.getServerVersion()).thenReturn("MockMarkLogic 1.0");
        when(session.toString()).thenReturn("mockSessionImpl");
        ServerConnectionException exception = new ServerConnectionException("Error parsing HTTP headers: Premature EOF, partial header line read: ''", req);
        int retry = 1;
        String[] uris = new String[]{"eofTest"};
        AbstractTask task = new AbstractTaskImpl();
        task.failOnError = true;
        task.inputUris = uris;
        task.properties = new Properties();
        task.properties.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(retry));
        task.properties.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, Integer.toString(retry));
        task.properties.setProperty(Options.QUERY_RETRY_INTERVAL, Integer.toString(retry));
        task.properties.setProperty(Options.QUERY_RETRY_LIMIT, Integer.toString(retry));
        task.handleRequestException(exception);
    }

    @Test
    public void testHandleRequestExceptionRequestExceptionAndRetryServerConnectionException() {
        Request req = mock(Request.class);
        SessionImpl session = mock(SessionImpl.class);
        when(req.getSession()).thenReturn(session);
        when(session.getServerVersion()).thenReturn("MockMarkLogic 1.0");
        when(session.toString()).thenReturn("mockSessionImpl");
        ServerConnectionException exception = new ServerConnectionException("Error parsing HTTP headers: Premature EOF, partial header line read: ''", req);
        int retry = 1;
        String[] uris = new String[]{"eofTest"};
        try {
            AbstractTask task = new AbstractTaskImpl();
            task.failOnError = true;
            task.inputUris = uris;
            task.properties = new Properties();
            task.properties.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, Integer.toString(retry));
            task.properties.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, Integer.toString(retry));
            task.properties.setProperty(Options.QUERY_RETRY_INTERVAL, Integer.toString(retry));
            task.properties.setProperty(Options.QUERY_RETRY_LIMIT, Integer.toString(retry));
            task.properties.setProperty(QUERY_RETRY_ERROR_MESSAGE, "Premature EOF");

            task.handleRequestException(exception);
            List<LogRecord> records = testLogger.getLogRecords();

            assertEquals(Level.WARNING, records.get(0).getLevel());
            assertTrue("Since we told it to retry on EOF, no exception (re)thrown", task.shouldRetry(exception));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test (expected = CorbException.class)
    public void testHandleProcessException() throws CorbException {
        Exception ex = mock(Exception.class);
        AbstractTask task = new AbstractTaskImpl();
        task.handleProcessException(ex);
    }

    @Test
    public void testHandleProcessExceptionDoNotFailOnError() {
        String[] uris = new String[]{"foo"};
        Exception ex = mock(Exception.class);
        AbstractTask task = new AbstractTaskImpl();
        task.failOnError = false;
        task.inputUris = uris;
        try {
            String[] result = task.handleProcessException(ex);
            assertArrayEquals(uris, result);
        } catch (CorbException exc) {
            fail();
        }
    }

    @Test
    public void testShouldRetryNotRetryableQueryExceptionCSVwithSpaces() {
        Request req = mock(Request.class);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = new Properties();
        task.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "foo, SVC-EXTIME, XDMP-EXTIME, bar");
        XQueryException exception = new XQueryException(req, SVC_EXTIME, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", false, new String[0], new QueryStackFrame[0]);
        assertTrue(task.shouldRetry(exception));
    }

    @Test
    public void testShouldRetryNotRetryableQueryException() {
        Request req = mock(Request.class);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = new Properties();
        task.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-FOO,XDMP-BAR,XDMP-BAZ");
        XQueryException exception = new XQueryException(req, SVC_EXTIME, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", false, new String[0], new QueryStackFrame[0]);

        assertFalse(task.shouldRetry(exception));

        task.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, SVC_EXTIME + ",XDMP-EXTIME");
        assertTrue(task.shouldRetry(exception));

        task.properties.remove(Options.QUERY_RETRY_ERROR_CODES);
        assertFalse(task.shouldRetry(exception)); //no match on code(and no exception attempting to split null)
    }

    @Test
    public void testShouldRetryRetryableQueryException() {
        Request req = mock(Request.class);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = new Properties();
        task.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-FOO,SVC-BAR,XDMP-BAZ");
        XQueryException exception = new XQueryException(req, SVC_EXTIME, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", true, new String[0], new QueryStackFrame[0]);

        assertTrue(task.shouldRetry(exception)); //since it's retryable, doesn't matter if code matches

        task.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, SVC_EXTIME + ",XDMP-EXTIME");
        assertTrue(task.shouldRetry(exception)); //is retryable and the code matches

        task.properties.remove(Options.QUERY_RETRY_ERROR_CODES);
        assertTrue(task.shouldRetry(exception));
    }

    @Test
    public void testShouldRetryRequestPermissionException() {
        Request req = mock(Request.class);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = new Properties();
        task.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "XDMP-FOO,SVC-BAR,SVC-BAZ");
        RequestPermissionException exception = new RequestPermissionException(REJECTED_MSG, req, USER_NAME, false);
        assertFalse(task.shouldRetry(exception));

        exception = new RequestPermissionException(REJECTED_MSG, req, USER_NAME, true);
        assertTrue(task.shouldRetry(exception));
    }

    @Test
    public void testHasRetryableMessage() {
        Request req = mock(Request.class);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = new Properties();
        task.properties.setProperty(Options.QUERY_RETRY_ERROR_MESSAGE, "FOO,Authentication failure for user,BAR");
        RequestPermissionException exception = new RequestPermissionException(REJECTED_MSG, req, USER_NAME, false);
        assertFalse(task.hasRetryableMessage(exception));

        exception = new RequestPermissionException("Authentication failure for user 'user-name'", req, USER_NAME, false);
        assertTrue(task.hasRetryableMessage(exception));
    }

    @Test
    public void testHasRetryableMessageWithServerConnectionException() {
        Request req = mock(Request.class);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = new Properties();

        ServerConnectionException exception = new ServerConnectionException("Error parsing HTTP headers: Premature EOF, partial header line read: ''", req);
        assertFalse(task.hasRetryableMessage(exception));

        task.properties.setProperty(Options.QUERY_RETRY_ERROR_MESSAGE, "Premature EOF");
        assertTrue(task.hasRetryableMessage(exception));
    }

    @Test
    public void testToXdmItems() {
        AbstractTask task = new AbstractTaskImpl();
        try {
            XdmItem[] result = task.toXdmItems("", FOO, EMPTY_DOC_ELEMENT);
            assertEquals(3, result.length);
            assertEquals("", result[0].asString());
            assertEquals(FOO, result[1].asString());
            assertEquals(EMPTY_DOC_ELEMENT, result[2].asString());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testToXdmItemsMalformedXML() {
        AbstractTask task = new AbstractTaskImpl();
        try {
            XdmItem[] result = task.toXdmItems(EMPTY_DOC_ELEMENT);
            assertEquals(EMPTY_DOC_ELEMENT, result[0].asString());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testWriteToErrorFileNullUris() throws CorbException, IOException {
        String[] uris = null;
        File exportDir = createTempDirectory();
        String filename = "testWriteToErrorFileNullUris.error";
        String delim = null;
        String message = null;
        testWriteToError(uris, delim, exportDir, filename, message);
        File file = new File(exportDir, filename);
        assertFalse(file.exists());
    }

    @Test
    public void testWriteToErrorFileEmptyUris() throws CorbException, IOException {
        String[] uris = new String[]{};
        File exportDir = createTempDirectory();
        String filename = "testWriteToErrorFileEmptyUris.error";
        String delim = null;
        String message = null;
        File errorFile = testWriteToError(uris, delim, exportDir, filename, message);
        assertFalse(errorFile.exists());
    }

    @Test(expected = NullPointerException.class)
    public void testWriteToErrorFileNullErrorFilename() {
        String[] uris = new String[]{URI};
        String filename = null;
        String delim = null;
        try {
            File exportDir = createTempDirectory();
            testWriteToError(uris, delim, exportDir, filename, ERROR);
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testWriteToErrorFileEmptyErrorFilename() {
        String[] uris = new String[]{URI};
        String filename = "";
        String delim = null;
        try {
            File exportDir = createTempDirectory();
            File errorFile = testWriteToError(uris, delim, exportDir, filename, ERROR);
            //testWriteToError constructs a File object that is the containing directory when filename is blank
            assertFalse(errorFile.isFile());
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testWriteToErrorFileNullBatchUriDelim() {
        testWriteToErrorFileDefaultDelimiter(null);
    }

    @Test
    public void testWriteToErrorFileEmptyBatchUriDelim() {
        testWriteToErrorFileDefaultDelimiter("");
    }

    public void testWriteToErrorFileDefaultDelimiter(String delimiter) {
        String[] uris = new String[]{URI};
        try {
            File exportDir = createTempDirectory();
            File errorFile = testWriteToError(uris, delimiter, exportDir, "testWriteToErrorFile.err", ERROR);
            assertTrue(TestUtils.readFile(errorFile).contains(Manager.DEFAULT_BATCH_URI_DELIM));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testWriteToErrorFileCustomBatchUridelim() {
        String[] uris = new String[]{URI};
        String filename = "testWriteToErrorFileCustomBatchUridelim.err";
        String delim = "$";

        try {
            File exportDir = createTempDirectory();
            File errorFile = testWriteToError(uris, delim, exportDir, filename, ERROR);

            assertTrue(TestUtils.readFile(errorFile).contains(delim));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testWriteToErrorFileNullMessage() {
        String[] uris = new String[]{URI};
        String filename = "testWriteToErrorFileNullMessage.err";
        String delim = "|";
        String message = null;

        try {
            File exportDir = createTempDirectory();
            File errorFile = testWriteToError(uris, delim, exportDir, filename, message);

            assertFalse(TestUtils.readFile(errorFile).contains(delim));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testWriteToErrorFileEmptyMessage() {
        String[] uris = new String[]{URI};
        String filename = "testWriteToErrorFileEmptyMessage.err";
        String delim = "~";
        String message = "";
        try {
            File exportDir = createTempDirectory();
            File errorFile = testWriteToError(uris, delim, exportDir, filename, message);

            assertFalse(TestUtils.readFile(errorFile).contains(delim));
        } catch (CorbException | IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testUrisAsString() {
        String[] uris = new String[]{FOO, BAR, BAZ};
        AbstractTask task = new AbstractTaskImpl();
        String result = task.urisAsString(uris);
        assertEquals("foo,bar,baz", result);
    }

    @Test
    public void testUrisAsStringEmptyArray() {
        String[] uris = new String[]{};
        AbstractTask task = new AbstractTaskImpl();
        String result = task.urisAsString(uris);
        assertEquals("", result);
    }

    @Test
    public void testUrisAsStringNull() {
        String[] uris = null;
        AbstractTask task = new AbstractTaskImpl();
        String result = task.urisAsString(uris);
        assertEquals("", result);
    }

    @Test
    public void testCleanup() {
        AbstractTask task = new AbstractTaskImpl();
        task.csp = mock(ContentSourcePool.class);
        task.moduleType = "moduleType";
        task.moduleUri = "moduleUri";
        task.properties = new Properties();
        task.inputUris = new String[]{};
        task.adhocQuery = "adhocQuery";
        task.cleanup();
        assertNull(task.csp);
        assertNull(task.moduleType);
        assertNull(task.moduleUri);
        assertNull(task.properties);
        assertNull(task.inputUris);
        assertNull(task.adhocQuery);
    }

    @Test
    public void testGetProperty() {
        String key = Options.INIT_TASK;
        String val = FOO;
        Properties props = new Properties();
        props.setProperty(key, val);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = props;
        String result = task.getProperty(key);
        assertEquals(val, result);
    }

    @Test
    public void testGetPropertySystemPropertyTakesPrecedence() {
        String key = Options.INIT_TASK;
        String val = FOO;
        System.setProperty(key, val);
        Properties props = new Properties();
        props.setProperty(key, BAR);
        AbstractTask task = new AbstractTaskImpl();
        task.properties = props;
        String result = task.getProperty(key);
        assertEquals(val, result);
        clearSystemProperties();
    }

    @Test
    public void testGetValueAsBytesXdmBinary() {
        XdmItem item = mock(XdmBinary.class);

        byte[] result = AbstractTaskImpl.getValueAsBytes(item);
        assertNull(result);
    }

    @Test
    public void testGetValueAsBytesXdmItem() {
        XdmItem item = mock(XdmItem.class);
        String value = FOO;
        when(item.asString()).thenReturn(value);
        byte[] result = AbstractTaskImpl.getValueAsBytes(item);
        assertArrayEquals(value.getBytes(), result);
    }

    public File createTempDirectory() throws IOException {
        File dir = TestUtils.createTempDirectory();
        dir.deleteOnExit();
        return dir;
    }

    public boolean testHandleRequestException(RequestException exception, boolean fail, int retryLimit)
            throws CorbException, IOException {
        String[] uris = new String[]{URI};
        return testHandleRequestException(exception.getClass().getSimpleName(), exception, fail, uris, retryLimit);
    }

    public boolean testHandleRequestException(String type, RequestException exception, boolean fail, String[] uris, int retryLimit)
            throws CorbException, IOException {
        File exportDir = createTempDirectory();
        File exportFile = File.createTempFile(ERROR, ".err", exportDir);
        return testHandleRequestException(type, exception, fail, uris, null, exportDir, exportFile.getName(), retryLimit);
    }

    public boolean testHandleRequestException(String type, RequestException exception, boolean fail, String[] uris, String delim, File exportDir, String errorFilename, int retryLimit)
            throws CorbException, IOException {
        File dir = exportDir;
        if (dir == null) {
            dir = createTempDirectory();
        }
        AbstractTask task = new AbstractTaskImpl();
        task.failOnError = fail;
        task.inputUris = uris;
        task.exportDir = dir.getAbsolutePath();
        task.properties = new Properties();
        if (errorFilename != null) {
            task.properties.setProperty(Options.ERROR_FILE_NAME, errorFilename);
        }
        if (delim != null) {
            task.properties.setProperty(Options.BATCH_URI_DELIM, delim);
        }

        task.properties.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "1");
        task.properties.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, Integer.toString(retryLimit));

        task.properties.setProperty(Options.QUERY_RETRY_INTERVAL, "2");
        task.properties.setProperty(Options.QUERY_RETRY_LIMIT, Integer.toString(retryLimit));

        task.handleRequestException(exception);
        List<LogRecord> records = testLogger.getLogRecords();

        boolean hasWarning = Level.WARNING.equals(records.get(0).getLevel());
        boolean shouldRetryOrFailOnErrorIsFalse = task.shouldRetry(exception)
                || ("failOnError is false. Encountered " + type + " at URI: " + task.urisAsString(uris)).equals(records.get(0).getMessage());

        return hasWarning && shouldRetryOrFailOnErrorIsFalse;
    }

    public File testWriteToError(String[] uris, String delim, File exportDir, String errorFilename, String message)
            throws CorbException, IOException {
        Request req = mock(Request.class);
        RequestServerException serverException = new RequestServerException(message, req);
        testHandleRequestException(RequestServerException.class.getSimpleName(), serverException, false, uris, delim, exportDir, errorFilename, 0);
        return new File(exportDir, errorFilename);
    }

    @Test
    public void testGetValueAsBytesDefault() {
        XdmItem item = null;
        byte[] result = AbstractTaskImpl.getValueAsBytes(item);
        assertArrayEquals(new byte[]{}, result);
    }

    @Test
    public void testWrapProcessException() {
        AbstractTask task = new AbstractTaskImpl();
        Exception ex = new IllegalAccessException();
        CorbException corbException = task.wrapProcessException(ex, "uri1", "uri2");
        assertEquals(corbException.getCause().getClass(), IllegalAccessException.class);
        assertTrue(corbException.getMessage().contains("uri1"));
    }

    @Test
    public void testWrapProcessExceptionNullUri() {
        AbstractTask task = new AbstractTaskImpl();
        Exception ex = new IllegalAccessException();
        CorbException corbException = task.wrapProcessException(ex, null);
        assertEquals(corbException.getCause().getClass(), IllegalAccessException.class);
        assertNotNull(corbException.getMessage());
    }

    @Test
    public void testWrapProcessExceptionRedacted() {
        AbstractTask task = new AbstractTaskImpl();
        Properties properties = new Properties();
        properties.setProperty(Options.URIS_REDACTED, Boolean.TRUE.toString());
        task.setProperties(properties);
        Exception ex = new IllegalAccessException();
        CorbException corbException = task.wrapProcessException(ex, "uri1", "uri2");
        assertEquals(corbException.getCause().getClass(), IllegalAccessException.class);
        assertFalse(corbException.getMessage().contains("uri1"));
    }

    private static class AbstractTaskImpl extends AbstractTask {

        @Override
        public String processResult(ResultSequence seq) throws CorbException {
            return "";
        }

        @Override
        public String[] call() throws Exception {
            throw new UnsupportedOperationException();
        }

    }

    private static class RequestImpl implements Request {

        private long count = -1;
        private long position = -1;
        private RequestOptions requestOptions = new RequestOptions();
        private final List<XdmVariable> variables = new ArrayList<>();

        @Override
        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public long getCount() {
            return this.count;
        }

        @Override
        public void setPosition(long position) {
            this.position = position;
        }

        @Override
        public long getPosition() {
            return this.position;
        }

        @Override
        public Session getSession() {
            return null;
        }

        @Override
        public void setOptions(RequestOptions requestOptions) {
            this.requestOptions = requestOptions;
        }

        @Override
        public RequestOptions getOptions() {
            return this.requestOptions;
        }

        @Override
        public RequestOptions getEffectiveOptions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setVariable(XdmVariable xv) {
            variables.add(xv);
        }

        @Override
        public XdmVariable setNewVariable(XName xname, XdmValue xv) {
            XdmVariable variable = ValueFactory.newVariable(xname, xv);
            variables.add(variable);
            return variable;
        }

        @Override
        public XdmVariable setNewVariable(String string, String string1, ValueType vt, Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XdmVariable setNewVariable(String string, ValueType vt, Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XdmVariable setNewStringVariable(String name, String value, String defaultValue) {
            XName xName = new XName(name);
            String val = value == null ? defaultValue : value;
            XSString xValue = ValueFactory.newXSString(val);
            XdmVariable variable = ValueFactory.newVariable(xName, xValue);
            variables.add(variable);
            return variable;
        }

        public void setNewVariables(String string, ValueType vt, Object... o) {
            throw new UnsupportedOperationException();
        }

        public void setNewVariables(String string, String string2, ValueType vt, Object... o) {
            throw new UnsupportedOperationException();
        }

        public void setNewVariables(XName string, XdmValue... xdmValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XdmVariable setNewStringVariable(String name, String value) {
            return setNewStringVariable(name, value, null);
        }

        @Override
        public XdmVariable setNewIntegerVariable(String string, String string1, long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XdmVariable setNewIntegerVariable(String string, long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearVariable(XdmVariable xdmVariable) {
            variables.remove(xdmVariable);
        }

        @Override
        public void clearVariables() {
            variables.clear();
        }

        @Override
        public XdmVariable[] getVariables() {
            return variables.toArray(new XdmVariable[0]);
        }
    }

    private static class AdhocQueryImpl extends RequestImpl implements AdhocQuery {

        private String query;

        @Override
        public void setQuery(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getQuery() {
            return query;
        }

        @Override
        public void setNewVariables(String string, ValueType vt, Object... o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNewVariables(String string, String string2, ValueType vt, Object... o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNewVariables(XName string, XdmValue... xdmValue) {
            throw new UnsupportedOperationException();
        }
    }

    private static class ModuleInvokeImpl extends RequestImpl implements ModuleInvoke {

        private String moduleUri;

        @Override
        public String getModuleUri() {
            return moduleUri;
        }

        @Override
        public void setModuleUri(String moduleUri) {
            this.moduleUri = moduleUri;
        }

        @Override
        public void setNewVariables(String string, ValueType vt, Object[] o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNewVariables(String string, String string2, ValueType vt, Object[] o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNewVariables(XName string, XdmValue[] xdmValue) {
            throw new UnsupportedOperationException();
        }
    }
}
