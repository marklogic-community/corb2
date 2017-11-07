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

import org.junit.*;
import org.w3c.dom.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Praveen Venkata
 */
public class FileUrisXMLLoaderTest {

    private static final Logger LOG = Logger.getLogger(FileUrisXMLLoaderTest.class.getName());
    private static final String ANCHOR1 = "<a href=\"test1.html\">test1</a>";
    private static final String ANCHOR2 = "<a href=\"test2.html\">test2</a>";
    private static final String ANCHOR3 = "<a href=\"test3.html\">test3</a>";
    private static final String ANCHOR4 = "<a href=\"\"><!----></a>";
    private static final String TEST1 = "test1";
    private static final String TEST2 = "test2";
    private static final String TEST3 = "test3";
    private static final String HTML_SUFFIX = ".html";

    @Test
    public void testSetOptionsNull() {
        TransformOptions options = null;
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setOptions(options);
            assertNull(instance.options);
        }
    }

    @Test
    public void testSetOptions() {
        TransformOptions options = mock(TransformOptions.class);
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setOptions(options);
            assertEquals(options, instance.options);
        }
    }

    @Test
    public void testSetContentSourceNull() {
        ContentSourcePool csp = null;
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setContentSourcePool(csp);
            assertNull(instance.csp);
        }
    }

    @Test
    public void testSetCollectionNull() {
        String collection = null;
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setCollection(collection);
            assertNull(instance.collection);
        }
    }

    @Test
    public void testSetCollection() {
        String collection = "foo";
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setCollection(collection);
            assertEquals(collection, instance.collection);
        }
    }

    @Test
    public void testSetPropertiesNull() {
        Properties properties = null;
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setProperties(properties);
            assertNull(instance.properties);
        }
    }

    @Test
    public void testSetPropertiesProperties() {
        Properties properties = new Properties();
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.setProperties(properties);
            assertEquals(properties, instance.properties);
        }
    }

    @Test
    public void testOpen() {
        List<String> nodes;
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.open();
            assertNotNull(instance.nodeIterator);
            nodes = new ArrayList<>(4);
            while (instance.hasNext()) {
                nodes.add(instance.next());
            }
            assertEquals(4, nodes.size());
            assertTrue(nodes.contains(ANCHOR1));
            assertTrue(nodes.contains(ANCHOR2));
            assertTrue(nodes.contains(ANCHOR3));
            assertTrue(nodes.contains(ANCHOR4));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = CorbException.class)
    public void testOpenNotXMLFile() throws CorbException {
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.properties.setProperty(Options.XML_FILE, TEST1);
            instance.open();
        }
    }

    @Test
    public void testOpenWithEnvelopeNotBase64Encoded() {
        List<String> nodes;
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(true));
            instance.properties.setProperty(Options.LOADER_BASE64_ENCODE, Boolean.toString(false));

            instance.open();
            assertNotNull(instance.nodeIterator);
            nodes = new ArrayList<>(4);
            while (instance.hasNext()) {
                nodes.add(instance.next());
            }
            assertEquals(4, nodes.size());
            assertEquals(4, nodes.stream()
                    .filter(p
                            -> (p.contains(ANCHOR1) || p.contains(ANCHOR2) || p.contains(ANCHOR3) || p.contains(ANCHOR4))
                            && p.contains(FileUrisXMLLoader.LOADER_DOC))
                    .count());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithEnvelopeAndBase64Encoded() {
        List<String> nodes;
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(true));
            instance.properties.setProperty(Options.LOADER_BASE64_ENCODE, Boolean.toString(true));

            instance.open();
            assertNotNull(instance.nodeIterator);
            nodes = new ArrayList<>(4);
            while (instance.hasNext()) {
                nodes.add(instance.next());
            }
            assertEquals(4, nodes.size());


            assertEquals(4, nodes.stream()
                    .filter(p
                            -> !(p.contains(ANCHOR1) || p.contains(ANCHOR2) || p.contains(ANCHOR3) || p.contains(ANCHOR4))
                            && p.contains(FileUrisXMLLoader.LOADER_DOC))
                    .count());

        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithoutXPath() {
        List<String> nodes;
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.properties.remove(Options.XML_NODE);
            instance.open();
            assertNotNull(instance.nodeIterator);
            nodes = new ArrayList<>(4);
            while (instance.hasNext()) {
                nodes.add(instance.next());
            }
            assertEquals(4, nodes.size());
            assertTrue(nodes.contains(ANCHOR1));
            assertTrue(nodes.contains(ANCHOR2));
            assertTrue(nodes.contains(ANCHOR3));
            assertTrue(nodes.contains(ANCHOR4));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testSelectRootNode() {
        List<String> nodes = testSelectNodes("/");
        assertEquals(1, nodes.size());
    }

    @Test
    public void testSelectDocumentElement() {
        List<String> nodes = testSelectNodes("/*");
        assertEquals(1, nodes.size());
    }

    @Test
    public void testSelectDocumentElementWithEnvelope() {
        List<String> nodes = testSelectNodes("/*", true);
        assertEquals(1, nodes.size());
    }

    @Test
    public void testSelectAttributes() {
        List<String> nodes = testSelectNodes("/root/a/@*");
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(TEST1 + HTML_SUFFIX));
        assertTrue(nodes.contains(TEST2 + HTML_SUFFIX));
        assertTrue(nodes.contains(TEST3 + HTML_SUFFIX));
    }

    @Test
    public void testSelectTextNodes() {
        List<String> nodes = testSelectNodes("/root/a/text()");
        assertEquals(3, nodes.size());
        assertTrue(nodes.contains(TEST1));
        assertTrue(nodes.contains(TEST2));
        assertTrue(nodes.contains(TEST3));
    }

    @Test
    public void testSelectComments() {
        List<String> nodes = testSelectNodes("//comment()");
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains("http://test.com/test1.html"));
    }

    @Test
    public void testSelectProcessingInstructions() {
        List<String> nodes = testSelectNodes("//processing-instruction()");
        assertEquals(1, nodes.size());
        assertTrue(nodes.contains("http://test.com/test2.html"));
    }

    @Test
    public void testSelectWithUnion() {
        List<String> nodes = testSelectNodes("//comment() | //@* | /*/*/text()");
        assertEquals(7, nodes.size());
        //comment()
        assertTrue(nodes.contains("http://test.com/test1.html"));
        //@*
        assertTrue(nodes.contains(TEST1 + HTML_SUFFIX));
        assertTrue(nodes.contains(TEST2 + HTML_SUFFIX));
        assertTrue(nodes.contains(TEST3 + HTML_SUFFIX));
        //text()
        assertTrue(nodes.contains(TEST1));
        assertTrue(nodes.contains(TEST2));
        assertTrue(nodes.contains(TEST3));
    }

    @Test(expected = CorbException.class)
    public void testOpenFileDoesNotExist() throws CorbException {
        FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader();
        instance.properties.setProperty(Options.XML_FILE, "does/not/exit.xml");
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test
    public void testGetBatchRef() {
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            assertNull(instance.getBatchRef());
        }
    }

    @Test
    public void testGetTotalCountDefaultValue() {
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            assertEquals(0, instance.getTotalCount());
        }
    }

    @Test
    public void testGetTotalCount() throws CorbException {
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.open();
            assertEquals(4, instance.getTotalCount());
        }
    }

    @Test(expected = NullPointerException.class)
    public void testHasNextThrowException() throws CorbException {
        try (FileUrisXMLLoader instance = new FileUrisXMLLoader()) {
            instance.hasNext();
        }
        fail();
    }

    @Test
    public void testHasNext() {
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.properties.remove(Options.XML_NODE);
            try {
                instance.open();
                for (int i = 0; i < instance.getTotalCount(); i++) {
                    assertTrue(instance.hasNext());
                }
                //Verify that hasNext() does not advance the buffered reader to the next line
                assertTrue(instance.hasNext());
            } catch (CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
        }
    }

    @Test
    public void testNext() {
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.open();
            //Verify that hasNext() does not advance the buffered reader to the next line
            for (int i = 0; i < instance.getTotalCount(); i++) {
                assertNotNull(instance.next());
            }
            assertFalse(instance.hasNext());
            assertNull(instance.next());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    @Test
    public void testClose() {
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.doc = mock(Document.class);
        instance.close();
        assertNull(instance.doc);
        instance.close();
    }

    @Test
    public void testCleanup() {
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        instance.doc = mock(Document.class);
        instance.collection = "testCollection";
        instance.csp = mock(ContentSourcePool.class);
        instance.nextUri = "<test>testData</test>";
        instance.options = new TransformOptions();
        instance.properties = new Properties();
        instance.replacements = new String[]{};
        instance.setTotalCount(100);
        instance.close();
        instance.cleanup();
        assertNull(instance.doc);
        assertNull(instance.collection);
        assertNull(instance.csp);
        assertNull(instance.options);
        assertNull(instance.replacements);
    }

    public List<String> testSelectNodes(String xpath) {
        return testSelectNodes(xpath, false);
    }

    public List<String> testSelectNodes(String xpath, boolean useEnvelope) {
        List<String> nodes = null;
        try (FileUrisXMLLoader instance = getDefaultFileUrisXMLLoader()) {
            instance.properties.setProperty(Options.XML_NODE, xpath);
            instance.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(useEnvelope));
            instance.open();
            assertNotNull(instance.nodeIterator);
            nodes = new ArrayList<>(1);
            while (instance.hasNext()) {
                nodes.add(instance.next());
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        return nodes;
    }

    private FileUrisXMLLoader getDefaultFileUrisXMLLoader() {
        FileUrisXMLLoader instance = new FileUrisXMLLoader();
        TransformOptions options = new TransformOptions();
        Properties props = new Properties();
        props.setProperty(Options.URIS_LOADER, FileUrisLoader.class.getName());
        props.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(false));
        props.setProperty(Options.XML_FILE, "src/test/resources/xml-file.xml");
        props.setProperty(Options.XML_NODE, "/root/a");
        instance.properties = props;
        instance.options = options;
        return instance;
    }
}
