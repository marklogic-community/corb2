/*
 * * Copyright 2005-2015 MarkLogic Corporation
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
import java.io.BufferedReader;
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
public class FileUrisLoaderTest {

    public FileUrisLoaderTest() {
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
     * Test of setOptions method, of class FileUrisLoader.
     */
    @Test
    public void testSetOptions_null() {
        System.out.println("setOptions");
        TransformOptions options = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setOptions(options);
        assertNull(instance.options);
    }

    @Test
    public void testSetOptions() {
        System.out.println("setOptions");
        TransformOptions options = mock(TransformOptions.class);
        FileUrisLoader instance = new FileUrisLoader();
        instance.setOptions(options);
        assertEquals(options, instance.options);
    }

    /**
     * Test of setContentSource method, of class FileUrisLoader.
     */
    @Test
    public void testSetContentSource_null() {
        System.out.println("setContentSource");
        ContentSource cs = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setContentSource(cs);
        assertNull(instance.cs);
    }

    /**
     * Test of setCollection method, of class FileUrisLoader.
     */
    @Test
    public void testSetCollection_null() {
        System.out.println("setCollection");
        String collection = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setCollection(collection);
        assertNull(instance.collection);
    }

    @Test
    public void testSetCollection() {
        System.out.println("setCollection");
        String collection = "foo";
        FileUrisLoader instance = new FileUrisLoader();
        instance.setCollection(collection);
        assertEquals(collection, instance.collection);
    }

    /**
     * Test of setProperties method, of class FileUrisLoader.
     */
    @Test
    public void testSetProperties_null() {
        System.out.println("setProperties");
        Properties properties = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setProperties(properties);
        assertNull(instance.properties);
    }

    @Test
    public void testSetProperties_properties() {
        System.out.println("setProperties");
        Properties properties = new Properties();
        FileUrisLoader instance = new FileUrisLoader();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
    }

    /**
     * Test of open method, of class FileUrisLoader.
     */
    @Test
    public void testOpen() throws Exception {
        System.out.println("open");
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile("src/test/resources/uris-file.txt");
        Properties props = new Properties();
        props.setProperty("URIS-REPLACE-PATTERN", "object-id-2,test");
        instance.properties = props;
        instance.options = options;
        instance.open();
        assertNotNull(instance.br);
        assertEquals("object-id-1", instance.next());
        assertEquals("test", instance.next());
        instance.close();
    }

    @Test(expected = CorbException.class)
    public void testOpen_fileDoesNotExist() throws Exception {
        System.out.println("open");
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile("does/not/exist");
        instance.options = options;
        instance.open();
        assertNotNull(instance.br);
    }

    /**
     * Test of getBatchRef method, of class FileUrisLoader.
     */
    @Test
    public void testGetBatchRef() {
        System.out.println("getBatchRef");
        FileUrisLoader instance = new FileUrisLoader();
        assertNull(instance.getBatchRef());
    }

    /**
     * Test of getTotalCount method, of class FileUrisLoader.
     */
    @Test
    public void testGetTotalCount_defaultValue() {
        System.out.println("getTotalCount");
        FileUrisLoader instance = new FileUrisLoader();
        assertEquals(0, instance.getTotalCount());
    }

    @Test
    public void testGetTotalCount() throws CorbException {
        System.out.println("getTotalCount");
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile("src/test/resources/uris-file.txt");
        instance.options = options;
        instance.open();
        assertEquals(8, instance.getTotalCount());
    }

    /**
     * Test of hasNext method, of class FileUrisLoader.
     */
    @Test (expected = CorbException.class)
    public void testHasNext_throwException() throws Exception {
        System.out.println("hasNext");
        FileUrisLoader instance = new FileUrisLoader();
        boolean result = instance.hasNext();
    }

    @Test
    public void testHasNext() throws Exception {
        System.out.println("hasNext");
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile("src/test/resources/uris-file.txt");
        instance.options = options;
        instance.open();
        
        for (int i = 0; i < instance.getTotalCount(); i++) {
            assertTrue(instance.hasNext());
        }
        //Verify that hasNext() does not advance the buffered reader to the next line
        assertTrue(instance.hasNext());
    }
    
    /**
     * Test of next method, of class FileUrisLoader.
     */
    @Test
    public void testNext() throws Exception {
        System.out.println("next");
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile("src/test/resources/uris-file.txt");
        instance.options = options;
        instance.open();
        //Verify that hasNext() does not advance the buffered reader to the next line
        for (int i = 0; i < instance.getTotalCount(); i++) {
            assertNotNull(instance.next());
        }
        assertFalse(instance.hasNext());
        assertNull(instance.next());
    }

    /**
     * Test of close method, of class FileUrisLoader.
     */
    @Test
    public void testClose() {
        System.out.println("close");
        FileUrisLoader instance = new FileUrisLoader();
        instance.br = mock(BufferedReader.class);
        instance.close();
        assertNull(instance.br);
    }

    /**
     * Test of cleanup method, of class FileUrisLoader.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        FileUrisLoader instance = new FileUrisLoader();
        instance.br = mock(BufferedReader.class);
        instance.collection = "foo";
        instance.cs = mock(ContentSource.class);
        instance.nextLine = "foo";
        instance.options = new TransformOptions();
        instance.properties = new Properties();
        instance.replacements = new String[]{};
        instance.total = 100;
       
        instance.cleanup();
        assertNull(instance.br);
        assertNull(instance.collection);
        assertNull(instance.cs);
        assertNull(instance.options);
        assertNull(instance.replacements);
        // TODO should these  be reset? 
        //assertNull(instance.nextLine);
        //assertNull(instance.total);
    }

}
