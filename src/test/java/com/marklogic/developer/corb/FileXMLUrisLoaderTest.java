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

    private FileUrisXMLLoader getDefaultFileUrisXMLLoader() {
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        TransformOptions options = new TransformOptions();
        Properties props = new Properties();
        props.setProperty("URIS-LOADER", "com.marklogic.developer.corb.FileUrisXMLLoader");
        props.setProperty("XML-FILE", "src/test/resources/xml-file.xml");
        props.setProperty("XML-NODE", "/root/a");
        instance.properties = props;
        instance.options = options;
        return instance;
    }

    /**
     * Test of open method, of class FileUrisXMLLoader.
     */
    @Test
    public void testOpen() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        instance.close();
        assertEquals(4, nodes.size());
        assertTrue(nodes.contains("<a href=\"test1.html\">test1</a>"));
        assertTrue(nodes.contains("<a href=\"test2.html\">test2</a>"));
        assertTrue(nodes.contains("<a href=\"test3.html\">test3</a>"));
        assertTrue(nodes.contains("<a href=\"\">\n<!---->\n</a>")); //indent options result in extra carriage returns
    }

    @Test
    public void testOpenWithoutXPath() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.remove(Options.XML_NODE);

        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        instance.close();
        assertEquals(4, nodes.size());
        assertTrue(nodes.contains("<a href=\"test1.html\">test1</a>"));
        assertTrue(nodes.contains("<a href=\"test2.html\">test2</a>"));
        assertTrue(nodes.contains("<a href=\"test3.html\">test3</a>"));
        assertTrue(nodes.contains("<a href=\"\">\n<!---->\n</a>")); //indent options result in extra carriage returns
    }

    @Test
    public void testSelectRootNode() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty(Options.XML_NODE, "/");

        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        instance.close();
        assertEquals(1, nodes.size());
    }

    @Test
    public void testSelectDocumentElement() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty(Options.XML_NODE, "/*");

        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        instance.close();
        assertEquals(1, nodes.size());
    }

    @Test
    public void testSelectAttributes() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty("XML-NODE", "/root/a/@*");
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        instance.close();

        assertEquals(3, nodes.size());
        assertTrue(nodes.contains("test1.html"));
        assertTrue(nodes.contains("test2.html"));
        assertTrue(nodes.contains("test3.html"));
    }

    @Test
    public void testSelectTextNodes() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty("XML-NODE", "/root/a/text()");
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        instance.close();
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains("test1"));
        assertTrue(nodes.contains("test2"));
        assertTrue(nodes.contains("test3"));
    }

    @Test
    public void testSelectComments() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty("XML-NODE", "//comment()");
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains("http://test.com/test1.html"));
        instance.close();
    }

    @Test
    public void testSelectProcessingInstructions() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty(Options.XML_NODE, "//processing-instruction()");
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains("http://test.com/test2.html"));
        instance.close();
    }

    @Test
    public void testSelectWithUnion() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty("XML-NODE", "//comment() | //@* | /*/*/text()");
        instance.open();
        assertNotNull(instance.nodeIterator);
        ArrayList<String> nodes = new ArrayList<String>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        assertEquals(7, nodes.size());
        //comment()
        assertTrue(nodes.contains("http://test.com/test1.html"));
        //@*
        assertTrue(nodes.contains("test1.html"));
        assertTrue(nodes.contains("test2.html"));
        assertTrue(nodes.contains("test3.html"));
        //text()
        assertTrue(nodes.contains("test1"));
        assertTrue(nodes.contains("test2"));
        assertTrue(nodes.contains("test3"));
        instance.close();
    }

    @Test(expected = CorbException.class)
    public void testOpen_fileDoesNotExist() throws Exception {
        System.out.println("open");
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty("XML-FILE", "does/not/exit.xml");
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
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.open();
        assertEquals(4, instance.getTotalCount());
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
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.remove(Options.XML_NODE);
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
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
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
