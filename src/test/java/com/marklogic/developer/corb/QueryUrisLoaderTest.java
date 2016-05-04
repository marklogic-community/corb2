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

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.XdmItem;
import com.marklogic.xcc.types.XdmVariable;
import java.io.File;
import java.io.FileWriter;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class QueryUrisLoaderTest {

    public QueryUrisLoaderTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of open method, of class QueryUrisLoader.
     */
    @Test(expected = NullPointerException.class)
    public void testOpen_nullPropertiesAndNullOptions() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpen_withBadUrisReplacePattern() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        Properties props = new Properties();
        props.setProperty("URIS-REPLACE-PATTERN", "foo");
        instance.properties = props;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = CorbException.class)
    public void testOpen_badUriCount() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        XdmVariable var = mock(XdmVariable.class);
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(contentSource.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(var).thenReturn(var).thenReturn(var);
        when(session.submitRequest(request)).thenReturn(seq);
        when(seq.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xdmItem).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("none").thenReturn("none");
        Properties props = new Properties();
        props.setProperty("URIS-REPLACE-PATTERN", "foo,");
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("/module");
        transformOptions.setModuleRoot("/root");
        instance.collection = "";
        instance.properties = props;
        instance.cs = contentSource;
        instance.options = transformOptions;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = CorbException.class)
    public void testOpen_inlineUriModule() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        AdhocQuery request = mock(AdhocQuery.class);
        XdmVariable var = mock(XdmVariable.class);
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(contentSource.newSession()).thenReturn(session);
        when(session.newAdhocQuery(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(var).thenReturn(var).thenReturn(var);
        when(session.submitRequest(request)).thenReturn(seq);
        when(seq.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xdmItem).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("none").thenReturn("none");
        Properties props = new Properties();
        props.setProperty("URIS-REPLACE-PATTERN", "foo,");
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("INLINE-XQUERY|for $i in (1 to 5) return $i || '.xml'");
        transformOptions.setModuleRoot("/root");
        instance.collection = "";
        instance.properties = props;
        instance.cs = contentSource;
        instance.options = transformOptions;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_noCodeInInline() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        XdmVariable var = mock(XdmVariable.class);
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(contentSource.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(var).thenReturn(var).thenReturn(var);
        when(session.submitRequest(request)).thenReturn(seq);
        when(seq.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xdmItem).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("none").thenReturn("none");
        Properties props = new Properties();
        props.setProperty("URIS-REPLACE-PATTERN", "foo,");
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("INLINE-XQUERY|");
        transformOptions.setModuleRoot("/root");

        instance.properties = props;
        instance.cs = contentSource;
        instance.options = transformOptions;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_adHocIsDirectory() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("|ADHOC");
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_adHocIsEmpty() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        File file = File.createTempFile("adhoc", "xqy");
        file.deleteOnExit();
        transformOptions.setUrisModule(file.getAbsolutePath() + "|ADHOC");
        instance.options = transformOptions;
        instance.cs = contentSource;

        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test
    public void testOpen() throws Exception {
        System.out.println("open");

        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        AdhocQuery request = mock(AdhocQuery.class);
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xItemFirst = mock(XdmItem.class);
        XdmItem xItemCount = mock(XdmItem.class);
        when(contentSource.newSession()).thenReturn(session);
        when(session.newAdhocQuery(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(null).thenReturn(null).thenReturn(null).thenReturn(null);
        when(session.submitRequest(request)).thenReturn(resultSequence);
        when(resultSequence.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xItemFirst).thenReturn(xItemFirst).thenReturn(xItemCount);
        when(xItemFirst.asString()).thenReturn("PROCESS-MODULE.foo=bar").thenReturn("PROCESS-MODULE.foo=bar");
        when(xItemCount.asString()).thenReturn("1");
        TransformOptions transformOptions = new TransformOptions();
        File file = File.createTempFile("adhoc", ".js");
        file.deleteOnExit();
        FileWriter writer = new FileWriter(file, true);
        writer.append("var foo;");
        writer.close();
        transformOptions.setUrisModule(file.getAbsolutePath() + "|ADHOC");
        Properties props = new Properties();
        props.setProperty("URIS-MODULE.foo", "bar");

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.properties = props;
        instance.options = transformOptions;
        instance.cs = contentSource;
        instance.collection = "";
        instance.open();
        assertEquals(1, instance.total);
        assertEquals("bar", instance.properties.getProperty("PROCESS-MODULE.foo"));
        instance.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_badAdhocFilenameIsEmpty() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  |ADHOC");
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_maxOptsFromModuleZero() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  |ADHOC");
        Properties props = new Properties();
        props.setProperty("MAX_OPTS_FROM_MODULE", "0");
        instance.properties = props;
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_InvalidMaxOptsFromModuleZero() throws Exception {
        System.out.println("open");
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  |ADHOC");
        Properties props = new Properties();
        props.setProperty("MAX_OPTS_FROM_MODULE", "one");
        instance.properties = props;
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    /**
     * Test of getBatchRef method, of class QueryUrisLoader.
     */
    @Test
    public void testGetBatchRef() {
        System.out.println("getBatchRef");
        QueryUrisLoader instance = new QueryUrisLoader();
        String result = instance.getBatchRef();
        instance.close();
        assertNull(result);
    }

    /**
     * Test of getTotalCount method, of class QueryUrisLoader.
     */
    @Test
    public void testGetTotalCount() {
        System.out.println("getTotalCount");
        QueryUrisLoader instance = new QueryUrisLoader();
        int result = instance.getTotalCount();
        instance.close();
        assertEquals(0, result);
    }

    /**
     * Test of hasNext method, of class QueryUrisLoader.
     */
    @Test
    public void testHasNext_resultSequenceIsNull() throws Exception {
        System.out.println("hasNext");
        QueryUrisLoader instance = new QueryUrisLoader();
        boolean result = instance.hasNext();
        instance.close();
        assertFalse(result);
    }

    @Test
    public void testHasNext_resultSequenceHasNext() throws CorbException, RequestException {
        System.out.println("hasNext");
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(contentSource.newSession()).thenReturn(session);
        when(xdmItem.asString()).thenReturn("1");
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(resultItem.asString()).thenReturn("1");
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(session.submitRequest(request)).thenReturn(resultSequence);

        QueryUrisLoader instance = new QueryUrisLoader();
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("foo");
        instance.options = transformOptions;
        instance.cs = contentSource;
        instance.res = resultSequence;
        instance.open();
        boolean result = instance.hasNext();
        instance.close();
        assertTrue(result);
    }

    @Test
    public void testHasNext_resultSequenceNotHasNext() throws Exception {
        System.out.println("hasNext");
        ResultSequence resultSequence = mock(ResultSequence.class);
        when(resultSequence.hasNext()).thenReturn(false);
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.res = resultSequence;
        boolean result = instance.hasNext();
        instance.close();
        assertFalse(result);
    }

    /**
     * Test of next method, of class QueryUrisLoader.
     */
    @Test
    public void testNext() throws Exception {
        System.out.println("next");
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(contentSource.newSession()).thenReturn(session);
        when(xdmItem.asString()).thenReturn("1");
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(resultItem.asString()).thenReturn("foo_bar_baz-1_2_3");
        when(resultSequence.next()).thenReturn(resultItem).thenReturn(resultItem).thenReturn(resultItem).thenReturn(resultItem);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(session.submitRequest(request)).thenReturn(resultSequence);

        QueryUrisLoader instance = new QueryUrisLoader();
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("foo");
        instance.options = transformOptions;
        instance.cs = contentSource;
        instance.res = resultSequence;
        instance.replacements = new String[]{"_", ",", "-", "\n"};
        instance.open();
        String result = instance.next();
        instance.close();
        assertEquals("foo,bar,baz\n1,2,3", result);
    }

    @Test(expected = NoSuchElementException.class)
    public void testNext_noQueue() throws Exception {
        System.out.println("next");

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.next();
    }

    /**
     * Test of close method, of class QueryUrisLoader.
     */
    @Test
    public void testClose_nullSession() {
        System.out.println("close");
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.close();
    }

    @Test
    public void testClose() {
        System.out.println("close");
        Session session = mock(Session.class);

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.session = session;
        instance.close();
        assertNull(instance.session);
    }

    /**
     * Test of cleanup method, of class QueryUrisLoader.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.options = new TransformOptions();
        instance.cs = mock(ContentSource.class);
        instance.collection = "foo";
        instance.properties = new Properties();
        instance.batchRef = "bar";
        instance.replacements = new String[]{};
        instance.cleanup();
        instance.close();
        assertNull(instance.options);
        assertNull(instance.cs);
        assertNull(instance.collection);
        assertNull(instance.properties);
        assertNull(instance.batchRef);
        assertNull(instance.replacements);
    }

    /**
     * Test of getProperty method, of class QueryUrisLoader.
     */
    @Test
    public void testGetProperty_nullProperties() {
        System.out.println("getProperty");
        String key = "foo";
        QueryUrisLoader instance = new QueryUrisLoader();
        String result = instance.getProperty(key);
        instance.close();
        assertNull(result);
    }

    @Test
    public void testGetProperty() {
        System.out.println("getProperty");
        String key = "foo";
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        instance.close();
        assertNull(result);
    }

    @Test
    public void testGetProperty_exists() {
        System.out.println("getProperty");
        String key = "foo";
        String value = "bar";
        QueryUrisLoader instance = new QueryUrisLoader();
        Properties props = new Properties();
        props.setProperty(key, value);
        instance.properties = props;
        String result = instance.getProperty(key);
        instance.close();
        assertEquals(value, result);
    }

    @Test
    public void testGetMaxOptionsFromModule() {
        QueryUrisLoader instance = new QueryUrisLoader();
        assertEquals(10, instance.getMaxOptionsFromModule());
    }

    @Test
    public void testGetMaxOptionsFromModule_validValue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "42");
        instance.setProperties(props);
        assertEquals(42, instance.getMaxOptionsFromModule());
    }

    @Test
    public void testGetMaxOptionsFromModule_invalidValue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "eleven");
        instance.setProperties(props);
        assertEquals(10, instance.getMaxOptionsFromModule());
    }
}
