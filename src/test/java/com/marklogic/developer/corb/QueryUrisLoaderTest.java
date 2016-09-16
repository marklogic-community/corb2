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
    private String root = "/root";
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
        transformOptions.setModuleRoot(root);
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
        transformOptions.setModuleRoot(root);
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
        transformOptions.setModuleRoot(root);

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
        File file = File.createTempFile("adhocXQuery", "xqy");
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
        String processModuleKey1 = "PROCESS-MODULE.foo";
        String processModuleKey2 = "PROCESS-MODULE.foo-foo";
        String processModuleKey3 = "PROCESS-MODULE.foo_foo2";
        String equalsBar = "=bar";
        String keyEqualsBar = processModuleKey1 + equalsBar;
        String keyEqualsBar2 = processModuleKey2 + equalsBar;
        String keyEqualsBar3 = processModuleKey3 + equalsBar;
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        AdhocQuery request = mock(AdhocQuery.class);
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xItem1 = mock(XdmItem.class);
        XdmItem xItem2 = mock(XdmItem.class);
        XdmItem xItem3 = mock(XdmItem.class);
        XdmItem xItemCount = mock(XdmItem.class);
        when(contentSource.newSession()).thenReturn(session);
        when(session.newAdhocQuery(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(null).thenReturn(null).thenReturn(null).thenReturn(null);
        when(session.submitRequest(request)).thenReturn(resultSequence);
        when(resultSequence.next()).thenReturn(item);
        when(item.getItem()).
                thenReturn(xItem1).thenReturn(xItem1).
                thenReturn(xItem2).thenReturn(xItem2).
                thenReturn(xItem3).thenReturn(xItem3).
                thenReturn(xItemCount);
        when(xItem1.asString()).thenReturn(keyEqualsBar).thenReturn(keyEqualsBar);
        when(xItem2.asString()).thenReturn(keyEqualsBar2).thenReturn(keyEqualsBar2);
        when(xItem3.asString()).thenReturn(keyEqualsBar3).thenReturn(keyEqualsBar3);
        when(xItemCount.asString()).thenReturn(Integer.toString(1));
        TransformOptions transformOptions = new TransformOptions();
        File file = File.createTempFile("adhocJS", ".js");
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file, true)) {
            writer.append("var foo;");
        }
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
        assertEquals(bar, instance.properties.getProperty(processModuleKey1));
        assertEquals(bar, instance.properties.getProperty(processModuleKey2));
        assertEquals(bar, instance.properties.getProperty(processModuleKey3));
        instance.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpen_badAdhocFilenameIsEmpty() throws Exception {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  " + ADHOC_SUFFIX);
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
        transformOptions.setUrisModule("  " + ADHOC_SUFFIX);
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
        transformOptions.setUrisModule("  " + ADHOC_SUFFIX);
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
        String result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.getBatchRef();
        }
        assertNull(result);
    }

    /**
     * Test of getTotalCount method, of class QueryUrisLoader.
     */
    @Test
    public void testGetTotalCount() {
        int result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.getTotalCount();
        }
        assertEquals(0, result);
    }

    /**
     * Test of hasNext method, of class QueryUrisLoader.
     */
    @Test
    public void testHasNext_resultSequenceIsNull() throws Exception {
        boolean result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.hasNext();
        }
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
        when(xdmItem.asString()).thenReturn(Integer.toString(1));
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(resultItem.asString()).thenReturn(Integer.toString(1));
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(session.submitRequest(request)).thenReturn(resultSequence);

        boolean result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            TransformOptions transformOptions = new TransformOptions();
            transformOptions.setUrisModule(foo);
            instance.options = transformOptions;
            instance.cs = contentSource;
            instance.res = resultSequence;
            instance.open();
            result = instance.hasNext();
        }
        assertTrue(result);
    }

    @Test
    public void testHasNext_resultSequenceNotHasNext() throws Exception {
        ResultSequence resultSequence = mock(ResultSequence.class);
        when(resultSequence.hasNext()).thenReturn(false);
        boolean result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            instance.res = resultSequence;
            result = instance.hasNext();
        }
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
        when(xdmItem.asString()).thenReturn(Integer.toString(1));
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(resultItem.asString()).thenReturn("foo_bar_baz-1_2_3");
        when(resultSequence.next()).thenReturn(resultItem).thenReturn(resultItem).thenReturn(resultItem).thenReturn(resultItem);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(session.submitRequest(request)).thenReturn(resultSequence);

        String result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            TransformOptions transformOptions = new TransformOptions();
            transformOptions.setUrisModule(foo);
            instance.options = transformOptions;
            instance.cs = contentSource;
            instance.res = resultSequence;
            instance.replacements = new String[]{"_", ",", "-", "\n"};
            instance.open();
            result = instance.next();
        }
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
        String result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.getProperty(foo);
        }
        assertNull(result);
    }

    @Test
    public void testGetProperty() {

        String result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            instance.properties = new Properties();
            result = instance.getProperty(foo);
        }
        assertNull(result);
    }

    @Test
    public void testGetProperty_exists() {
        String key = foo;
        String value = bar;
        String result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            Properties props = new Properties();
            props.setProperty(key, value);
            instance.properties = props;
            result = instance.getProperty(key);
        }
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
