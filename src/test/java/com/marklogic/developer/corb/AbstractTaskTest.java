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
import static com.marklogic.developer.corb.Options.INIT_MODULE;
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
        ContentSourcePool csp = mock(ContentSourcePool.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.setContentSourcePool(csp);
        assertEquals(csp, instance.csp);
    }

    @Test
    public void testSetModuleType() {
        String moduleType = FOO;
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleType(moduleType);
        assertEquals(moduleType, instance.moduleType);
    }

    @Test
    public void testSetModuleURI() {
        String moduleUri = "test.xqy";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleURI(moduleUri);
        assertEquals(moduleUri, instance.moduleUri);
    }

    @Test
    public void testSetAdhocQuery() {
        String adhocQuery = ADHOC_MODULE;
        AbstractTask instance = new AbstractTaskImpl();
        instance.setAdhocQuery(adhocQuery);
        assertEquals(adhocQuery, instance.adhocQuery);
    }

    @Test
    public void testSetQueryLanguage() {
        String language = "XQuery";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setQueryLanguage(language);
        assertEquals(language, instance.language);
    }

    @Test
    public void testSetProperties() {
        Properties properties = new Properties();
        AbstractTask instance = new AbstractTaskImpl();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
    }

    @Test
    public void testSetInputURI() {
        String[] inputUri = {FOO, BAR, BAZ};
        AbstractTask instance = new AbstractTaskImpl();
        instance.setInputURI(inputUri);
        assertArrayEquals(inputUri, instance.inputUris);
    }

    @Test
    public void testSetInputURINull() {
        AbstractTask instance = new AbstractTaskImpl();
        assertNull(instance.inputUris);
        instance.setInputURI((String[]) null);
        assertNotNull(instance.inputUris);
    }

    @Test
    public void testSetFailOnError() {
        AbstractTask instance = new AbstractTaskImpl();
        instance.setFailOnError(false);
        assertFalse(instance.failOnError);
        instance.setFailOnError(true);
        assertTrue(instance.failOnError);
    }

    @Test
    public void testSetExportDir() {
        String exportFileDir = TMP_DIR;
        AbstractTask instance = new AbstractTaskImpl();
        instance.setExportDir(exportFileDir);
        assertEquals(exportFileDir, instance.exportDir);
    }

    @Test
    public void testGetExportDir() {
        AbstractTask instance = new AbstractTaskImpl();
        String expResult = TMP_DIR;
        instance.exportDir = expResult;
        String result = instance.getExportDir();
        assertEquals(expResult, result);
    }

    @Test
    public void testNewSession() throws CorbException{
        AbstractTask instance = new AbstractTaskImpl();
        ContentSourcePool csp = mock(ContentSourcePool.class);
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(csp.get()).thenReturn(cs);
        when(cs.newSession()).thenReturn(session);
        instance.csp = csp;
        Session result = instance.newSession();
        assertEquals(session, result);
    }

    @Test
    public void testInvokeModule() {
        String[] inputUris = new String[]{FOO, BAR, BAZ};
        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = "module.xqy";
        instance.adhocQuery = ADHOC_MODULE;
        instance.inputUris = inputUris;

        try {
        		ContentSourcePool csp = mock(ContentSourcePool.class);
            ContentSource cs = mock(ContentSource.class);
            Session session = mock(Session.class);
            ModuleInvoke request = mock(ModuleInvoke.class);
            ResultSequence seq = mock(ResultSequence.class);

            when(csp.get()).thenReturn(cs);
            when(cs.newSession()).thenReturn(session);
            when(session.newModuleInvoke(anyString())).thenReturn(request);
            when(request.setNewStringVariable(anyString(), anyString())).thenReturn(null);
            when(session.submitRequest(request)).thenReturn(seq);
            String key1 = "foo.bar";
            String key2 = "foo.baz";
            instance.csp = csp;
            instance.moduleType = FOO;
            Properties props = new Properties();
            props.setProperty(key1, BAZ);
            props.setProperty(key2, "boo");
            props.setProperty(Options.BATCH_URI_DELIM, "");
            instance.properties = props;

            instance.inputUris = new String[]{URI, "uri2"};
            String[] result = instance.invokeModule();
            assertArrayEquals(instance.inputUris, result);
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

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.language = "JavaScript";
        instance.timeZone = TimeZone.getDefault();
        instance.inputUris = new String[]{"a", "b", "c"};
        instance.setModuleType(INIT_MODULE);
        instance.properties.setProperty(INIT_MODULE + "." + FOO, BAR);

        try {
            instance.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        RequestOptions options = request.getOptions();
        assertEquals(instance.language, options.getQueryLanguage());
        assertEquals(instance.timeZone, options.getTimeZone());
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
        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_DOC);
        instance.inputUris = uris;

        try {
            instance.generateRequest(session);
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

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_URI);
        instance.inputUris = new String[]{EMPTY_DOC_ELEMENT};

        try {
            instance.generateRequest(session);
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

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.properties.setProperty(Options.LOADER_VARIABLE, AbstractTask.REQUEST_VARIABLE_URI);
        instance.inputUris = new String[]{EMPTY_DOC_ELEMENT, EMPTY_DOC_ELEMENT, EMPTY_DOC_ELEMENT};

        try {
            instance.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        List<XdmVariable> variableList = Arrays.asList(request.getVariables());
        assertEquals(1, variableList.size());

        assertEquals(String.join(Manager.DEFAULT_BATCH_URI_DELIM, instance.inputUris), variableList.get(0).getValue().asString());
    }

    @Test
    public void testGenerateRequestModuleInvokeSetLanguage() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.language = "Sparql"; //not currently supported, just verifying that it accepts any string
        try {
            instance.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        RequestOptions options = request.getOptions();
        assertEquals(instance.language, options.getQueryLanguage());
    }

    @Test
    public void testGenerateRequestModuleInvokeSetTimeZone() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.timeZone = TimeZone.getTimeZone("PST");
        try {
            instance.generateRequest(session);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        RequestOptions options = request.getOptions();
        assertEquals(instance.timeZone, options.getTimeZone());
    }

    @Test
    public void testGenerateRequestModuleInvokeWithCustomInputs() {
        ModuleInvoke request = new ModuleInvokeImpl();
        Session session = mock(Session.class);
        when(session.newModuleInvoke(anyString())).thenReturn(request);

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.setModuleType(INIT_MODULE);
        instance.properties.setProperty(INIT_MODULE + "." + FOO, BAR);
        instance.properties.setProperty(Options.POST_BATCH_MODULE + "." + BAZ, BAR);
        try {
            instance.generateRequest(session);
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

        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = URI;
        instance.setModuleType(INIT_MODULE);
        instance.properties.setProperty(Options.URIS_BATCH_REF, BAZ);
        try {
            instance.generateRequest(session);
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

        AbstractTask instance = new AbstractTaskImpl();
        try {
            Request requestResult = instance.generateRequest(session);
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
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleType(FOO);
        System.setProperty(key1, BAZ);
        Properties props = new Properties();
        props.setProperty(key1, BAZ);
        props.setProperty(key2, "boo");
        props.setProperty(Options.BATCH_URI_DELIM, "");
        instance.properties = props;
        Set<String> inputs = instance.getCustomInputPropertyNames();
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
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        assertEquals(-1, instance.getIntProperty(ONE));
        assertEquals(2, instance.getIntProperty(TWO));
        assertEquals(-1, instance.getIntProperty(THREE));
        assertEquals(-1, instance.getIntProperty("four"));
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
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "foo, SVC-EXTIME, XDMP-EXTIME, bar");
        XQueryException exception = new XQueryException(req, SVC_EXTIME, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", false, new String[0], new QueryStackFrame[0]);
        assertTrue(instance.shouldRetry(exception));
    }

    @Test
    public void testShouldRetryNotRetryableQueryException() {
        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "SVC-FOO,XDMP-BAR,XDMP-BAZ");
        XQueryException exception = new XQueryException(req, SVC_EXTIME, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", false, new String[0], new QueryStackFrame[0]);

        assertFalse(instance.shouldRetry(exception));

        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, SVC_EXTIME + ",XDMP-EXTIME");
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
        XQueryException exception = new XQueryException(req, SVC_EXTIME, W3C_CODE, XQUERY_VERSION, ERROR_MSG, "", "", true, new String[0], new QueryStackFrame[0]);

        assertTrue(instance.shouldRetry(exception)); //since it's retryable, doesn't matter if code matches

        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, SVC_EXTIME + ",XDMP-EXTIME");
        assertTrue(instance.shouldRetry(exception)); //is retryable and the code matches

        instance.properties.remove(Options.QUERY_RETRY_ERROR_CODES);
        assertTrue(instance.shouldRetry(exception));
    }

    @Test
    public void testShouldRetryRequestPermissionException() {
        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_CODES, "XDMP-FOO,SVC-BAR,SVC-BAZ");
        RequestPermissionException exception = new RequestPermissionException(REJECTED_MSG, req, USER_NAME, false);
        assertFalse(instance.shouldRetry(exception));

        exception = new RequestPermissionException(REJECTED_MSG, req, USER_NAME, true);
        assertTrue(instance.shouldRetry(exception));
    }

    @Test
    public void testHasRetryableMessage() {
        Request req = mock(Request.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_MESSAGE, "FOO,Authentication failure for user,BAR");
        RequestPermissionException exception = new RequestPermissionException(REJECTED_MSG, req, USER_NAME, false);
        assertFalse(instance.hasRetryableMessage(exception));

        exception = new RequestPermissionException("Authentication failure for user 'user-name'", req, USER_NAME, false);
        assertTrue(instance.hasRetryableMessage(exception));
    }

    @Test
    public void testToXdmItems() {
        AbstractTask instance = new AbstractTaskImpl();
        try {
            XdmItem[] result = instance.toXdmItems("", FOO, EMPTY_DOC_ELEMENT);
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
        AbstractTask instance = new AbstractTaskImpl();
        try {
            XdmItem[] result = instance.toXdmItems(EMPTY_DOC_ELEMENT);
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
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.urisAsString(uris);
        assertEquals("foo,bar,baz", result);
    }

    @Test
    public void testUrisAsStringEmptyArray() {
        String[] uris = new String[]{};
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.urisAsString(uris);
        assertEquals("", result);
    }

    @Test
    public void testUrisAsStringNull() {
        String[] uris = null;
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.urisAsString(uris);
        assertEquals("", result);
    }

    @Test
    public void testCleanup() {
        AbstractTask instance = new AbstractTaskImpl();
        instance.csp = mock(ContentSourcePool.class);
        instance.moduleType = "moduleType";
        instance.moduleUri = "moduleUri";
        instance.properties = new Properties();
        instance.inputUris = new String[]{};
        instance.adhocQuery = "adhocQuery";
        instance.cleanup();
        assertNull(instance.csp);
        assertNull(instance.moduleType);
        assertNull(instance.moduleUri);
        assertNull(instance.properties);
        assertNull(instance.inputUris);
        assertNull(instance.adhocQuery);
    }

    @Test
    public void testGetProperty() {
        String key = Options.INIT_TASK;
        String val = FOO;
        Properties props = new Properties();
        props.setProperty(key, val);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        String result = instance.getProperty(key);
        assertEquals(val, result);
    }

    @Test
    public void testGetPropertySystemPropertyTakesPrecedence() {
        String key = Options.INIT_TASK;
        String val = FOO;
        System.setProperty(key, val);
        Properties props = new Properties();
        props.setProperty(key, BAR);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        String result = instance.getProperty(key);
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
        AbstractTask instance = new AbstractTaskImpl();
        instance.failOnError = fail;
        instance.inputUris = uris;
        instance.exportDir = dir.getAbsolutePath();
        instance.properties = new Properties();
        if (errorFilename != null) {
            instance.properties.setProperty(Options.ERROR_FILE_NAME, errorFilename);
        }
        if (delim != null) {
            instance.properties.setProperty(Options.BATCH_URI_DELIM, delim);
        }

        instance.properties.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "1");
        instance.properties.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, Integer.toString(retryLimit));

        instance.properties.setProperty(Options.QUERY_RETRY_INTERVAL, "2");
        instance.properties.setProperty(Options.QUERY_RETRY_LIMIT, Integer.toString(retryLimit));

        instance.handleRequestException(exception);
        List<LogRecord> records = testLogger.getLogRecords();

        boolean hasWarning = Level.WARNING.equals(records.get(0).getLevel());
        boolean shouldRetryOrFailOnErrorIsFalse = instance.shouldRetry(exception)
                || ("failOnError is false. Encountered " + type + " at URI: " + instance.urisAsString(uris)).equals(records.get(0).getMessage());

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
        assertTrue(corbException.getMessage() != null);
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
            return variables.toArray(new XdmVariable[variables.size()]);
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
