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

import com.marklogic.xcc.ContentSource;
import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractUrisLoaderTest {

    /**
     * Test of setOptions method, of class AbstractUrisLoader.
     */
    @Test
    public void testSetOptions() {
        TransformOptions options = new TransformOptions();
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setOptions(options);
        assertEquals(options, instance.options);
    }

    /**
     * Test of setContentSource method, of class AbstractUrisLoader.
     */
    @Test
    public void testSetContentSource() {
        ContentSource cs = mock(ContentSource.class);
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setContentSource(cs);
        assertEquals(cs, instance.cs);
    }

    /**
     * Test of setCollection method, of class AbstractUrisLoader.
     */
    @Test
    public void testSetCollection() {
        String collection = "foo";
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setCollection(collection);
        assertEquals(collection, instance.collection);
    }

    /**
     * Test of setProperties method, of class AbstractUrisLoader.
     */
    @Test
    public void testSetProperties() {
        Properties properties = new Properties();
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
    }

    /**
     * Test of getBatchRef method, of class AbstractUrisLoader.
     */
    @Test
    public void testGetBatchRef() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        assertNull(instance.getBatchRef());
    }

    /**
     * Test of getTotalCount method, of class AbstractUrisLoader.
     */
    @Test
    public void testGetTotalCount() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        int result = instance.getTotalCount();
        assertEquals(0, result);
    }

    /**
     * Test of getProperty method, of class AbstractUrisLoader.
     */
    @Test
    public void testGetProperty() {
        String key = "foo";
        String value = "bar";
        Properties props = new Properties();
        props.setProperty(key, value);
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.setProperties(props);
        String result = instance.getProperty(key);
        assertEquals(value, result);
    }

    @Test
    public void testGetProperty_systemPropAndNullProperties() {
        String key = "foo";
        String value = "bar";
        System.setProperty(key, value);
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertEquals(value, result);       
    }
    
    @Test
    public void testGetProperty_isNull() {
        String key = "foo";
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        String result = instance.getProperty(key);
        assertEquals(null, result);
    }

    /**
     * Test of cleanup method, of class AbstractUrisLoader.
     */
    @Test
    public void testCleanup() {
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.cleanup();
        assertNull(instance.options);
        assertNull(instance.cs);
        assertNull(instance.collection);
        assertNull(instance.properties);
        assertNull(instance.replacements);
        assertNull(instance.batchRef);
    }

    /**
     * Test of parseUriReplacePatterns method, of class AbstractUrisLoader.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testParseUriReplacePatterns_uneven() {
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

        }

        @Override
        public void close() {

        }
    }

}
