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

    private String foo = "foo";
    private String bar = "bar";
    private String none = "none";
    private static final String ADHOC_SUFFIX = "|ADHOC";
    /**
     * Test of open method, of class QueryUrisLoader.
     */
    @Test(expected = NullPointerException.class)
    public void testOpen_nullPropertiesAndNullOptions() throws Exception {
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
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpen_withBadUrisReplacePattern() throws Exception {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, foo);
        instance.properties = props;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testOpen_badUriCount() throws Exception {
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
        when(xdmItem.asString()).thenReturn(none).thenReturn(none);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo1,");
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
        fail();
    }

    @Test(expected = CorbException.class)
    public void testOpen_inlineUriModule() throws Exception {
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
        when(xdmItem.asString()).thenReturn(none).thenReturn(none);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo2,");
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
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_noCodeInInline() throws Exception {
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
        when(xdmItem.asString()).thenReturn(none).thenReturn(none);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo3,");
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
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_adHocIsDirectory() throws Exception {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule(ADHOC_SUFFIX);
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_adHocIsEmpty() throws Exception {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        File file = File.createTempFile("adhoc", "xqy");
        file.deleteOnExit();
        transformOptions.setUrisModule(file.getAbsolutePath() + ADHOC_SUFFIX);
        instance.options = transformOptions;
        instance.cs = contentSource;

        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test
    public void testOpen() throws Exception {
        String processModuleKey = "PROCESS-MODULE.foo";
        String keyEqualsBar = processModuleKey + "=" + bar;
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
        when(xItemFirst.asString()).thenReturn(keyEqualsBar).thenReturn(keyEqualsBar);
        when(xItemCount.asString()).thenReturn("1");
        TransformOptions transformOptions = new TransformOptions();
        File file = File.createTempFile("adhoc", ".js");
        file.deleteOnExit();
        FileWriter writer = new FileWriter(file, true);
        writer.append("var foo;");
        writer.close();
        transformOptions.setUrisModule(file.getAbsolutePath() + ADHOC_SUFFIX);
        Properties props = new Properties();
        props.setProperty("URIS-MODULE.foo", bar);

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.properties = props;
        instance.options = transformOptions;
        instance.cs = contentSource;
        instance.collection = "";
        instance.open();
        assertEquals(1, instance.getTotalCount());
        assertEquals(bar, instance.properties.getProperty(processModuleKey));
        instance.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_badAdhocFilenameIsEmpty() throws Exception {
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
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_maxOptsFromModuleZero() throws Exception {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  |ADHOC");
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "0");
        instance.properties = props;
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_InvalidMaxOptsFromModuleZero() throws Exception {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  |ADHOC");
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "one");
        instance.properties = props;
        instance.options = transformOptions;
        instance.cs = contentSource;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    /**
     * Test of getBatchRef method, of class QueryUrisLoader.
     */
    @Test
    public void testGetBatchRef() {
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
        QueryUrisLoader instance = new QueryUrisLoader();
        boolean result = instance.hasNext();
        instance.close();
        assertFalse(result);
    }

    @Test
    public void testHasNext_resultSequenceHasNext() throws CorbException, RequestException {
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
        transformOptions.setUrisModule(foo);
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
        transformOptions.setUrisModule(foo);
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

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.next();
        fail();
    }

    /**
     * Test of close method, of class QueryUrisLoader.
     */
    @Test
    public void testClose_nullSession() {
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.close();
        assertTrue(instance.getQueue().isEmpty());
        assertNull(instance.session);
    }

    @Test
    public void testClose() {
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
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.options = new TransformOptions();
        instance.cs = mock(ContentSource.class);
        instance.setCollection(foo);
        instance.properties = new Properties();
        instance.setBatchRef(bar);
        instance.replacements = new String[]{};
        instance.cleanup();
        instance.close();
        assertNull(instance.options);
        assertNull(instance.cs);
        assertNull(instance.collection);
        assertNull(instance.properties);
        assertNull(instance.getBatchRef());
        assertNull(instance.replacements);
    }

    /**
     * Test of getProperty method, of class QueryUrisLoader.
     */
    @Test
    public void testGetProperty_nullProperties() {
        QueryUrisLoader instance = new QueryUrisLoader();
        String result = instance.getProperty(foo);
        instance.close();
        assertNull(result);
    }

    @Test
    public void testGetProperty() {

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.properties = new Properties();
        String result = instance.getProperty(foo);
        instance.close();
        assertNull(result);
    }

    @Test
    public void testGetProperty_exists() {
        String key = foo;
        String value = bar;
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
