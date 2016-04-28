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
import org.junit.*;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Praveen Venkata
 */
public class FileXMLUrisLoaderTest {

    public FileXMLUrisLoaderTest() {
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
     * Test of setOptions method, of class FileUrisXMLLoader.
     */
    @Test
    public void testSetOptions_null() {
        System.out.println("setOptions");
        TransformOptions options = null;
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setOptions(options);
        assertNull(instance.options);
        instance.close();
    }

    @Test
    public void testSetOptions() {
        System.out.println("setOptions");
        TransformOptions options = mock(TransformOptions.class);
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setOptions(options);
        assertEquals(options, instance.options);
        instance.close();
    }

    /**
     * Test of setContentSource method, of class FileUrisXMLLoader.
     */
    @Test
    public void testSetContentSource_null() {
        System.out.println("setContentSource");
        ContentSource cs = null;
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setContentSource(cs);
        assertNull(instance.cs);
        instance.close();
    }

    /**
     * Test of setCollection method, of class FileUrisXMLLoader.
     */
    @Test
    public void testSetCollection_null() {
        System.out.println("setCollection");
        String collection = null;
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setCollection(collection);
        assertNull(instance.collection);
        instance.close();
    }

    @Test
    public void testSetCollection() {
        System.out.println("setCollection");
        String collection = "foo";
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setCollection(collection);
        assertEquals(collection, instance.collection);
        instance.close();
    }

    /**
     * Test of setProperties method, of class FileUrisXMLLoader.
     */
    @Test
    public void testSetProperties_null() {
        System.out.println("setProperties");
        Properties properties = null;
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setProperties(properties);
        assertNull(instance.properties);
        instance.close();
    }

    @Test
    public void testSetProperties_properties() {
        System.out.println("setProperties");
        Properties properties = new Properties();
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
        instance.close();
    }

    /**
     * Test of open method, of class FileUrisXMLLoader.
     */
    @Test
    public void testOpen() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        TransformOptions options = new TransformOptions();
        Properties props = new Properties();
        props.setProperty("URIS-LOADER","com.marklogic.developer.corb.FileUrisXMLLoader");
        props.setProperty("XML-FILE","src/test/resources/xml-file.xml");
        props.setProperty("XML-NODE", "/root/a");
        instance.properties = props;
        instance.options = options;
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        nodes.add(instance.next());
        nodes.add(instance.next());
        nodes.add(instance.next());
        assertTrue(nodes.contains("<a>test1</a>"));
        assertTrue(nodes.contains("<a>test2</a>"));
        instance.close();
    }

    @Test(expected = CorbException.class)
    public void testOpen_fileDoesNotExist() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        Properties props = new Properties();
        props.setProperty("URIS-LOADER","com.marklogic.developer.corb.FileUrisXMLLoader");
        props.setProperty("XML-FILE","does/not/exit.xml");
        instance.properties = props;
        try {
            instance.open();
        } finally {
            instance.close();
        }
    }

    /**
     * Test of getBatchRef method, of class FileUrisXMLLoader.
     */
    @Test
    public void testGetBatchRef() {
        System.out.println("getBatchRef");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        assertNull(instance.getBatchRef());
        instance.close();
    }

    /**
     * Test of getTotalCount method, of class FileUrisXMLLoader.
     */
    @Test
    public void testGetTotalCount_defaultValue() {
        System.out.println("getTotalCount");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        assertEquals(0, instance.getTotalCount());
        instance.close();
    }

    @Test
    public void testGetTotalCount() throws CorbException {
        System.out.println("getTotalCount");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        Properties props = new Properties();
        props.setProperty("URIS-LOADER","com.marklogic.developer.corb.FileUrisXMLLoader");
        props.setProperty("XML-FILE","src/test/resources/xml-file.xml");
        props.setProperty("XML-NODE", "/root/a");
        instance.properties = props;
        instance.open();
        assertEquals(3, instance.getTotalCount());
        instance.close();
    }

    /**
     * Test of hasNext method, of class FileUrisXMLLoader.
     */
    @Test(expected = CorbException.class)
    public void testHasNext_throwException() throws Exception {
        System.out.println("hasNext");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        try {
            instance.hasNext();
        } finally {
            instance.close();
        }
    }

    @Test
    public void testHasNext() throws Exception {
        System.out.println("hasNext");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        Properties props = new Properties();
        props.setProperty("URIS-LOADER","com.marklogic.developer.corb.FileUrisXMLLoader");
        props.setProperty("XML-FILE","src/test/resources/xml-file.xml");
        instance.properties = props;
        instance.open();

        for (int i = 0; i < instance.getTotalCount(); i++) {
            assertTrue(instance.hasNext());
        }
        //Verify that hasNext() does not advance the buffered reader to the next line
        assertTrue(instance.hasNext());
        instance.close();
    }

    /**
     * Test of next method, of class FileUrisXMLLoader.
     */
    @Test
    public void testNext() throws Exception {
        System.out.println("next");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        Properties props = new Properties();
        props.setProperty("URIS-LOADER","com.marklogic.developer.corb.FileUrisXMLLoader");
        props.setProperty("XML-FILE","src/test/resources/xml-file.xml");
        props.setProperty("XML-NODE", "/root/a");
        instance.properties = props;
        instance.open();
        //Verify that hasNext() does not advance the buffered reader to the next line
        for (int i = 0; i < instance.getTotalCount(); i++) {
            assertNotNull(instance.next());
        }
        assertFalse(instance.hasNext());
        assertNull(instance.next());
        instance.close();
    }


    /**
     * Test of close method, of class FileUrisXMLLoader.
     */
    @Test
    public void testClose() {
        System.out.println("close");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.doc = mock(Document.class);
        instance.close();
        assertNull(instance.doc);
        instance.close();
    }

    /**
     * Test of cleanup method, of class FileUrisXMLLoader.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.doc = mock(Document.class);
        instance.collection = "foo";
        instance.cs = mock(ContentSource.class);
        instance.nextNode = "<test>testData</test>";
        instance.options = new TransformOptions();
        instance.properties = new Properties();
        instance.replacements = new String[]{};
        instance.total = 100;
        instance.close();
        instance.cleanup();
        assertNull(instance.doc);
        assertNull(instance.collection);
        assertNull(instance.cs);
        assertNull(instance.options);
        assertNull(instance.replacements);
    }

}
