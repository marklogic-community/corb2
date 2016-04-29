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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractUrisLoaderTest {

    public AbstractUrisLoaderTest() {
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
     * Test of setOptions method, of class AbstractUrisLoader.
     */
    @Test
    public void testSetOptions() {
        System.out.println("setOptions");
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
        System.out.println("setContentSource");
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
        System.out.println("setCollection");
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
        System.out.println("setProperties");
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
        System.out.println("getBatchRef");
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        assertNull(instance.getBatchRef());
    }

    /**
     * Test of getTotalCount method, of class AbstractUrisLoader.
     */
    @Test
    public void testGetTotalCount() {
        System.out.println("getTotalCount");
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        int result = instance.getTotalCount();
        assertEquals(0, result);
    }

    /**
     * Test of getProperty method, of class AbstractUrisLoader.
     */
    @Test
    public void testGetProperty() {
        System.out.println("getProperty");
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
        System.out.println("getProperty");
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
        System.out.println("getProperty");
        String key = "foo";
        String value = "bar";
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        String result = instance.getProperty(key);
        assertEquals(null, result);
    }

    /**
     * Test of cleanup method, of class AbstractUrisLoader.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        instance.cleanup();
    }

    /**
     * Test of parseUriReplacePatterns method, of class AbstractUrisLoader.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testParseUriReplacePatterns_uneven() {
        System.out.println("parseUriReplacePatterns");
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo|bar");
        instance.setProperties(props);
        instance.parseUriReplacePatterns();
    }

    @Test
    public void testParseUriReplacePatterns() {
        System.out.println("parseUriReplacePatterns");
        AbstractUrisLoader instance = new AbstractUrisLoaderImpl();
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "foo,bar");
        instance.setProperties(props);
        instance.parseUriReplacePatterns();
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
