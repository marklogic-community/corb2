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

import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractUrisLoaderTest {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    @Test
    public void testSetOptions() {
        TransformOptions options = new TransformOptions();
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setOptions(options);
        assertEquals(options, instance.options);
    }

    @Test
    public void testSetContentSource() {
        ContentSourcePool csp = mock(ContentSourcePool.class);
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setContentSourcePool(csp);
        assertEquals(csp, instance.csp);
    }

    @Test
    public void testSetCollection() {
        String collection = FOO;
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setCollection(collection);
        assertEquals(collection, instance.collection);
    }

    @Test
    public void testSetProperties() {
        Properties properties = new Properties();
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
    }

    @Test
    public void testGetBatchRef() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        assertNull(instance.getBatchRef());
    }

    @Test
    public void testGetTotalCount() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        long result = instance.getTotalCount();
        assertEquals(0, result);
    }

    @Test
    public void testSetTotalCount() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setProperties(new Properties());
        assertEquals(0, instance.getTotalCount());
        instance.setTotalCount(5);
        assertEquals(5, instance.getTotalCount());
        assertEquals(String.valueOf(5), instance.getProperty("PRE-BATCH-MODULE.URIS_TOTAL_COUNT"));
        assertEquals(String.valueOf(5), instance.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
    }

    @Test
    public void testGetProperty() {
        String key = FOO;
        String value = BAR;
        Properties props = new Properties();
        props.setProperty(key, value);
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setProperties(props);
        String result = instance.getProperty(key);
        assertEquals(value, result);
    }

    @Test
    public void testGetPropertySystemPropAndNullProperties() {
        String key = FOO;
        String value = BAR;
        System.setProperty(key, value);
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertEquals(value, result);
    }

    @Test
    public void testGetPropertyIsNull() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        String result = instance.getProperty(FOO);
        assertEquals(null, result);
    }

    @Test
    public void testCleanup() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.cleanup();
        assertNull(instance.options);
        assertNull(instance.csp);
        assertNull(instance.collection);
        assertNull(instance.properties);
        assertNull(instance.replacements);
        assertNull(instance.batchRef);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseUriReplacePatternsUneven() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo|bar");
        instance.setProperties(props);
        instance.parseUriReplacePatterns();
        fail();
    }

    @Test
    public void testParseUriReplacePatterns() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo,bar");
        instance.setProperties(props);
        instance.parseUriReplacePatterns();
        assertTrue(instance.replacements.length == 2);
    }

    @Test
    public void testGetLoaderPath() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        Properties props = new Properties();
        props.setProperty(Options.XML_FILE, FOO);
        props.setProperty(Options.LOADER_PATH, BAR);
        instance.setProperties(props);

        assertEquals(BAR, instance.getLoaderPath());
        assertEquals(FOO, instance.getLoaderPath(Options.XML_FILE));
        assertEquals(FOO, instance.getLoaderPath(Options.ZIP_FILE, Options.XML_FILE));
        assertEquals(FOO, instance.getLoaderPath(Options.XML_FILE, Options.ZIP_FILE));
        assertEquals(BAR, instance.getLoaderPath(Options.ZIP_FILE));
    }

    public static class AbstractUrisLoaderImpl extends AbstractUrisLoader {

        @Override
        public String next() {
            return "";
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void open() {
            //needed to implement interface
        }

        @Override
        public void close() {
            //needed to implement interface
        }
    }

}
