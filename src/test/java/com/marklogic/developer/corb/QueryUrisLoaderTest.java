/*
 * * Copyright (c) 2004-2017 MarkLogic Corporation
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

import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
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
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    private static final Logger LOG = Logger.getLogger(QueryUrisLoaderTest.class.getName());

    @Test(expected = NullPointerException.class)
    public void testOpenNullPropertiesAndNullOptions() throws CorbException {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        instance.csp = contentSourcePool;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenWithBadUrisReplacePattern() {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, foo);
        instance.properties = props;
        instance.csp = contentSourcePool;
        try {
            instance.open();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testOpenBadUriCount() throws CorbException {
    		ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        XdmVariable var = mock(XdmVariable.class);
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(var).thenReturn(var).thenReturn(var);
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
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
            instance.csp = contentSourcePool;
            instance.options = transformOptions;

            instance.open();
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testOpenInlineUriModule() throws CorbException {
    		ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        AdhocQuery request = mock(AdhocQuery.class);
        XdmVariable var = mock(XdmVariable.class);
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        when(session.newAdhocQuery(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(var).thenReturn(var).thenReturn(var);
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
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
            instance.csp = contentSourcePool;
            instance.options = transformOptions;

            instance.open();
        } catch (RequestException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenNoCodeInInline() {
    		ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        XdmVariable var = mock(XdmVariable.class);
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(contentSourcePool.get()).thenReturn(contentSource);        
        when(contentSource.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(), anyString())).thenReturn(var).thenReturn(var).thenReturn(var);
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
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
            instance.csp = contentSourcePool;
            instance.options = transformOptions;

            instance.open();
        } catch (RequestException | CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenAdHocIsDirectory() {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule(ADHOC_SUFFIX);
        instance.options = transformOptions;
        instance.csp = contentSourcePool;
        try {
            instance.open();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenAdHocIsEmpty() {
    		ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            File file = File.createTempFile("adhocXQuery", "xqy");
            file.deleteOnExit();
            transformOptions.setUrisModule(file.getAbsolutePath() + ADHOC_SUFFIX);
            instance.options = transformOptions;
            instance.csp = contentSourcePool;

            instance.open();
        } catch (IOException | CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testOpen() {
        try {
            String processModuleKey1 = "PROCESS-MODULE.foo";
            String processModuleKey2 = "PROCESS-MODULE.foo-foo";
            String processModuleKey3 = "PROCESS-MODULE.foo_foo2";
            String equalsBar = "=bar";
            String keyEqualsBar = processModuleKey1 + equalsBar;
            String keyEqualsBar2 = processModuleKey2 + equalsBar;
            String keyEqualsBar3 = processModuleKey3 + equalsBar;
            ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
            ContentSource contentSource = mock(ContentSource.class);
            Session session = mock(Session.class);
            AdhocQuery request = mock(AdhocQuery.class);
            ResultSequence resultSequence = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xItem1 = mock(XdmItem.class);
            XdmItem xItem2 = mock(XdmItem.class);
            XdmItem xItem3 = mock(XdmItem.class);
            XdmItem xItemCount = mock(XdmItem.class);
            when(contentSourcePool.get()).thenReturn(contentSource);
            when(contentSource.newSession()).thenReturn(session);
            when(session.newAdhocQuery(anyString())).thenReturn(request);
            when(request.setNewStringVariable(anyString(), anyString())).thenReturn(null).thenReturn(null).thenReturn(null).thenReturn(null);
            when(session.submitRequest(request)).thenReturn(resultSequence);
            when(resultSequence.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
            when(item.asString()).thenReturn("1").thenReturn("2").thenReturn("3").thenReturn("1");
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

            try (QueryUrisLoader instance = new QueryUrisLoader()) {
                instance.properties = props;
                instance.options = transformOptions;
                instance.csp = contentSourcePool;
                instance.collection = "";
                instance.open();
                assertEquals(1, instance.getTotalCount());
                assertEquals(bar, instance.properties.getProperty(processModuleKey1));
                assertEquals(bar, instance.properties.getProperty(processModuleKey2));
                assertEquals(bar, instance.properties.getProperty(processModuleKey3));
            }
        } catch (RequestException | IOException | CorbException ex) {
            Logger.getLogger(QueryUrisLoaderTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenBadAdhocFilenameIsEmpty() {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  " + ADHOC_SUFFIX);
        instance.options = transformOptions;
        instance.csp = contentSourcePool;
        try {
            instance.open();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenMaxOptsFromModuleZero() {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  " + ADHOC_SUFFIX);
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "0");
        instance.properties = props;
        instance.options = transformOptions;
        instance.csp = contentSourcePool;
        try {
            instance.open();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenInvalidMaxOptsFromModuleZero() {
        QueryUrisLoader instance = new QueryUrisLoader();
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        ContentSource contentSource = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(contentSourcePool.get()).thenReturn(contentSource);
        when(contentSource.newSession()).thenReturn(session);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setUrisModule("  " + ADHOC_SUFFIX);
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "one");
        instance.properties = props;
        instance.options = transformOptions;
        instance.csp = contentSourcePool;
        try {
            instance.open();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        } finally {
            instance.close();
        }
        fail();
    }

    @Test
    public void testGetBatchRef() {
        String result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.getBatchRef();
        }
        assertNull(result);
    }

    @Test
    public void testGetTotalCount() {
        int result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.getTotalCount();
        }
        assertEquals(0, result);
    }

    @Test
    public void testHasNextResultSequenceIsNull() {
        boolean result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            result = instance.hasNext();
            assertFalse(result);
        } catch (CorbException ex) {
            Logger.getLogger(QueryUrisLoaderTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }

    }

    @Test
    public void testHasNextResultSequenceHasNext() {
        try {
        		ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
            ContentSource contentSource = mock(ContentSource.class);
            Session session = mock(Session.class);
            ModuleInvoke request = mock(ModuleInvoke.class);
            ResultSequence resultSequence = mock(ResultSequence.class);
            ResultItem resultItem = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(contentSourcePool.get()).thenReturn(contentSource);
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
                instance.csp = contentSourcePool;
                instance.resultSequence = resultSequence;
                instance.open();
                result = instance.hasNext();
            }
            assertTrue(result);
        } catch (RequestException | CorbException ex) {
            Logger.getLogger(QueryUrisLoaderTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testHasNextResultSequenceNotHasNext() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        when(resultSequence.hasNext()).thenReturn(false);
        boolean result;
        try (QueryUrisLoader instance = new QueryUrisLoader()) {
            instance.resultSequence = resultSequence;
            result = instance.hasNext();
            assertFalse(result);
        } catch (CorbException ex) {
            Logger.getLogger(QueryUrisLoaderTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testNext() {
        try {
        		ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
            ContentSource contentSource = mock(ContentSource.class);
            Session session = mock(Session.class);
            ModuleInvoke request = mock(ModuleInvoke.class);
            ResultSequence resultSequence = mock(ResultSequence.class);
            ResultItem resultItem = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(contentSourcePool.get()).thenReturn(contentSource);
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
                instance.csp = contentSourcePool;
                instance.resultSequence = resultSequence;
                instance.replacements = new String[]{"_", ",", "-", "\n"};
                instance.open();
                result = instance.next();
            }
            assertEquals("foo,bar,baz\n1,2,3", result);
        } catch (RequestException | CorbException ex) {
            Logger.getLogger(QueryUrisLoaderTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testNextNoQueue() throws CorbException {
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.next();
        fail();
    }

    @Test
    public void testCloseNullSession() {
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.close();
        assertTrue(instance.createQueue().isEmpty());
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

    @Test
    public void testCleanup() {
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.options = new TransformOptions();
        instance.csp = mock(ContentSourcePool.class);
        instance.setCollection(foo);
        instance.properties = new Properties();
        instance.setBatchRef(bar);
        instance.replacements = new String[]{};
        instance.cleanup();
        instance.close();
        assertNull(instance.options);
        assertNull(instance.csp);
        assertNull(instance.collection);
        assertNull(instance.properties);
        assertNull(instance.getBatchRef());
        assertNull(instance.replacements);
    }

    @Test
    public void testCollectCustomInputsBatchRefFirstParam() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn(foo);

        Properties properties = new Properties();
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setProperties(properties);
        instance.collectCustomInputs(resultSequence);

        assertTrue(instance.properties.isEmpty());
        assertNotNull(instance.getBatchRef());
    }

    @Test
    public void testCollectCustomInputsCustomPropertyFirstParam() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn(PROCESS_MODULE + '.' + foo + '=' + bar)
                .thenReturn(PROCESS_MODULE + '.' + foo + '=' + bar)
                .thenReturn(none)
                .thenReturn(none)
                .thenReturn(Integer.toString(0));

        Properties properties = new Properties();
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setProperties(properties);
        instance.collectCustomInputs(resultSequence);

        assertTrue(instance.properties.getProperty(PROCESS_MODULE + '.' + foo).equals(bar));
        assertNotNull(instance.getBatchRef());
    }

    @Test
    public void testCollectCustomInputsResultItemIsNull() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        when(resultSequence.next()).thenReturn(null);

        Properties properties = new Properties();
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setProperties(properties);
        instance.collectCustomInputs(resultSequence);

        assertTrue(instance.properties.isEmpty());
        assertNull(instance.getBatchRef());
    }

    @Test
    public void testPopulateQueueWithNullResultSequence() {
        ResultSequence resultSequence = null;

        QueryUrisLoader instance = new QueryUrisLoader();
        Queue<String> queue = instance.createAndPopulateQueue(resultSequence);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testPopulateQueueWithBlankUrls() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultItem.asString()).thenReturn("");

        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setTotalCount(1);
        Queue<String> queue = instance.createAndPopulateQueue(resultSequence);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testPopulateQueueWithValues() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultItem.asString()).thenReturn(foo);
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setTotalCount(1);
        Queue<String> queue = instance.createAndPopulateQueue(resultSequence);
        assertFalse(queue.isEmpty());
    }

    @Test
    public void testPopulateArrayQueueWithValuesAndNoRoom() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultItem.asString()).thenReturn(foo);
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setTotalCount(0);
        Queue<String> queue = instance.createAndPopulateQueue(resultSequence);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testPopulateDiskQueueWithValuesAndNoRoom() {
        ResultSequence resultSequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        when(resultSequence.hasNext()).thenReturn(true).thenReturn(false);
        when(resultSequence.next()).thenReturn(resultItem);
        when(resultItem.asString()).thenReturn(foo);

        TransformOptions options = new TransformOptions();
        options.setUseDiskQueue(true);
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setOptions(options);
        instance.setTotalCount(0);
        Queue<String> queue = instance.createAndPopulateQueue(resultSequence);
        assertFalse(queue.isEmpty());
    }

    @Test
    public void testGetPropertyNullProperties() {
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
    public void testGetPropertyExists() {
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
    public void testCreateQueueDiskQueue() {
        TransformOptions options = new TransformOptions();
        options.setUseDiskQueue(true);
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setOptions(options);
        Queue<String> queue = instance.createQueue();
        assertTrue(queue instanceof DiskQueue);
    }

    @Test
    public void testCreateQueueArrayQueue() {
        TransformOptions options = new TransformOptions();
        options.setUseDiskQueue(false);
        QueryUrisLoader instance = new QueryUrisLoader();
        instance.setOptions(options);
        Queue<String> queue = instance.createQueue();
        assertTrue(queue instanceof ArrayQueue);
    }

    @Test
    public void testCreateQueue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        Queue<String> queue = instance.createQueue();
        assertTrue(queue instanceof ArrayQueue);
    }

    @Test
    public void testGetMaxOptionsFromModule() {
        QueryUrisLoader instance = new QueryUrisLoader();
        assertEquals(10, instance.getMaxOptionsFromModule());
    }

    @Test
    public void testGetMaxOptionsFromModuleValidValue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "42");
        instance.setProperties(props);
        assertEquals(42, instance.getMaxOptionsFromModule());
    }

    @Test
    public void testGetMaxOptionsFromModuleInvalidValue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        Properties props = new Properties();
        props.setProperty(Options.MAX_OPTS_FROM_MODULE, "eleven");
        instance.setProperties(props);
        assertEquals(10, instance.getMaxOptionsFromModule());
    }

    @Test
    public void testHasNextNullQueue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        try {
            assertFalse(instance.hasNext());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testHasNextEmptyQueue() {
        QueryUrisLoader instance = new QueryUrisLoader();
        try {
            instance.createQueue();
            assertFalse(instance.hasNext());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

}
