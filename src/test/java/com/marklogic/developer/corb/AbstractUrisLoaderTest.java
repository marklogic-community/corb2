/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class AbstractUrisLoaderTest {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    private AbstractUrisLoaderImpl newLoader() {
        return new AbstractUrisLoaderImpl();
    }

    private AbstractUrisLoaderImpl newLoader(Properties properties) {
        AbstractUrisLoaderImpl loader = newLoader();
        loader.setProperties(properties);
        return loader;
    }

    @Test
    void testSetOptions() {
        TransformOptions options = new TransformOptions();
        try (AbstractUrisLoader instance = newLoader()) {
            instance.setOptions(options);
            assertEquals(options, instance.options);
        }
    }

    @Test
    void testSetContentSource() {
        ContentSourcePool csp = mock(ContentSourcePool.class);
        try (AbstractUrisLoader instance = newLoader()) {
            instance.setContentSourcePool(csp);
            assertEquals(csp, instance.csp);
        }
    }

    @Test
    void testSetCollection() {
        String collection = FOO;
        try (AbstractUrisLoader instance = newLoader()) {
            instance.setCollection(collection);
            assertEquals(collection, instance.collection);
        }
    }

    @Test
    void testSetProperties() {
        Properties properties = new Properties();
        try (AbstractUrisLoader instance = newLoader()) {
            instance.setProperties(properties);
            assertEquals(properties, instance.properties);
        }
    }

    @Test
    void testGetBatchRef() {
        try (AbstractUrisLoader instance = newLoader()) {
            assertNull(instance.getBatchRef());
        }
    }

    @Test
    void testSetBatchRef() {
        AbstractUrisLoader instance = newLoader();
        instance.setBatchRef(FOO);
        assertEquals(FOO, instance.getBatchRef());
    }

    @Test
    void testGetTotalCount() {
        long result;
        try (AbstractUrisLoader instance = newLoader()) {
            result = instance.getTotalCount();
        }
        assertEquals(0, result);
    }

    @Test
    void testSetTotalCount() {
        try (AbstractUrisLoader instance = newLoader()) {
            instance.setProperties(new Properties());
            assertEquals(0, instance.getTotalCount());
            instance.setTotalCount(5);
            assertEquals(5, instance.getTotalCount());
            assertEquals(String.valueOf(5), instance.getProperty("PRE-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertEquals(String.valueOf(5), instance.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
        }
    }

    @Test
    void testSetTotalCountWithNullPropertiesAndZeroDoesNothing() {
        AbstractUrisLoader instance = newLoader();
        instance.setProperties(null);
        instance.setTotalCount(0);
        assertEquals(0, instance.getTotalCount());
        instance.setTotalCount(3);
        assertEquals(3, instance.getTotalCount());
    }

    @Test
    void testGetProperty() {
        String key = FOO;
        String value = BAR;
        Properties props = new Properties();
        props.setProperty(key, value);
        String result;
        try (AbstractUrisLoader instance = newLoader(props)) {
            result = instance.getProperty(key);
        }
        assertEquals(value, result);
    }

    @Test
    void testGetPropertySystemPropAndNullProperties() {
        String key = FOO;
        String value = BAR;
        System.setProperty(key, value);
        String result;
        try (AbstractUrisLoader instance = newLoader()) {
            result = instance.getProperty(key);
        }
        System.clearProperty(key);
        assertEquals(value, result);
    }

    @Test
    void testGetPropertyUsesPropertiesWhenSystemPropertyBlank() {
        String key = FOO;
        System.setProperty(key, "   ");
        Properties props = new Properties();
        props.setProperty(key, BAR);
        try (AbstractUrisLoader instance = newLoader(props)) {
            assertEquals(BAR, instance.getProperty(key));
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void testGetPropertyIsNull() {
        String result;
        try (AbstractUrisLoader instance = newLoader()) {
            result = instance.getProperty(FOO);
        }
        assertNull(result);
    }

    @Test
    void testCleanup() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.cleanup();
        assertNull(instance.options);
        assertNull(instance.csp);
        assertNull(instance.collection);
        assertNull(instance.properties);
        assertNull(instance.replacements);
        assertNull(instance.batchRef);
    }

    @Test
    void testParseUriReplacePatternsUneven() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo|bar");
        try (AbstractUrisLoader instance = newLoader(props)) {
            assertThrows(IllegalArgumentException.class, instance::parseUriReplacePatterns);
        }
    }

    @Test
    void testParseUriReplacePatternsBlankDoesNothing() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "   ");
        try (AbstractUrisLoader instance = newLoader(props)) {
            instance.parseUriReplacePatterns();
            assertEquals(0, instance.replacements.length);
        }
    }

    @Test
    void testParseUriReplacePatterns() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo,bar");
        try (AbstractUrisLoader instance = newLoader(props)) {
            instance.parseUriReplacePatterns();
            assertEquals(2, instance.replacements.length);
        }
    }

    @Test
    void testShouldSetBatchRef() {
        try (AbstractUrisLoader instance = newLoader()) {
            assertFalse(instance.shouldSetBatchRef());
            instance.setProperties(new Properties());
            instance.properties.setProperty(Options.LOADER_SET_URIS_BATCH_REF, "true");
            assertTrue(instance.shouldSetBatchRef());
        }
    }

    @Test
    void testGetLoaderPath() {
        Properties props = new Properties();
        props.setProperty(Options.XML_FILE, FOO);
        props.setProperty(Options.LOADER_PATH, BAR);
        try (AbstractUrisLoader instance = newLoader(props)) {
            assertEquals(BAR, instance.getLoaderPath());
            assertEquals(FOO, instance.getLoaderPath(Options.XML_FILE));
            assertEquals(FOO, instance.getLoaderPath(Options.ZIP_FILE, Options.XML_FILE));
            assertEquals(FOO, instance.getLoaderPath(Options.XML_FILE, Options.ZIP_FILE));
            assertEquals(BAR, instance.getLoaderPath(Options.ZIP_FILE));
        }
    }

    @Test
    void testGetLoaderPathFallsBackWhenAlternatesBlank() {
        Properties props = new Properties();
        props.setProperty(Options.LOADER_PATH, BAR);
        try (AbstractUrisLoader instance = newLoader(props)) {
            assertEquals(BAR, instance.getLoaderPath(Options.ZIP_FILE, Options.XML_FILE));
        }
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
