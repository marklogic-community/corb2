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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisLoaderTest {

    private static final String URIS_FILE = "src/test/resources/uris-file.txt";
    
    /**
     * Test of setOptions method, of class FileUrisLoader.
     */
    @Test
    public void testSetOptions_null() {
        TransformOptions options = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setOptions(options);
        assertNull(instance.options);
        instance.close();
    }

    @Test
    public void testSetOptions() {
        TransformOptions options = mock(TransformOptions.class);
        FileUrisLoader instance = new FileUrisLoader();
        instance.setOptions(options);
        assertEquals(options, instance.options);
        instance.close();
    }

    /**
     * Test of setContentSource method, of class FileUrisLoader.
     */
    @Test
    public void testSetContentSource_null() {
        ContentSource cs = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setContentSource(cs);
        assertNull(instance.cs);
        instance.close();
    }

    /**
     * Test of setCollection method, of class FileUrisLoader.
     */
    @Test
    public void testSetCollection_null() {
        String collection = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setCollection(collection);
        assertNull(instance.collection);
        instance.close();
    }

    @Test
    public void testSetCollection() {
        String collection = "testSetCollection";
        FileUrisLoader instance = new FileUrisLoader();
        instance.setCollection(collection);
        assertEquals(collection, instance.collection);
        instance.close();
    }

    /**
     * Test of setProperties method, of class FileUrisLoader.
     */
    @Test
    public void testSetProperties_null() {
        Properties properties = null;
        FileUrisLoader instance = new FileUrisLoader();
        instance.setProperties(properties);
        assertNull(instance.properties);
        instance.close();
    }

    @Test
    public void testSetProperties_properties() {
        Properties properties = new Properties();
        FileUrisLoader instance = new FileUrisLoader();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
        instance.close();
    }

    /**
     * Test of open method, of class FileUrisLoader.
     */
    @Test
    public void testOpen() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile(URIS_FILE);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "object-id-2,test");
        instance.properties = props;
        instance.options = options;
        instance.open();
        assertNotNull(instance.br);
        assertEquals("object-id-1", instance.next());
        assertEquals("test", instance.next());
        instance.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpen_invalidReplacePattern() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile(URIS_FILE);
        Properties props = new Properties();
        props.setProperty(Options.URIS_REPLACE_PATTERN, "object-id-2,test,unevenPattern");
        instance.properties = props;
        instance.options = options;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test(expected = CorbException.class)
    public void testOpen_fileDoesNotExist() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile("does/not/exist");
        instance.options = options;
        try {
            instance.open();
        } finally {
            instance.close();
        }
        fail();
    }

    /**
     * Test of getBatchRef method, of class FileUrisLoader.
     */
    @Test
    public void testGetBatchRef() {
        FileUrisLoader instance = new FileUrisLoader();
        assertNull(instance.getBatchRef());
        instance.close();
    }

    /**
     * Test of getTotalCount method, of class FileUrisLoader.
     */
    @Test
    public void testGetTotalCount_defaultValue() {
        FileUrisLoader instance = new FileUrisLoader();
        assertEquals(0, instance.getTotalCount());
        instance.close();
    }

    @Test
    public void testGetTotalCount() throws CorbException {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile(URIS_FILE);
        instance.options = options;
        instance.open();
        assertEquals(8, instance.getTotalCount());
        instance.close();
    }

    /**
     * Test of hasNext method, of class FileUrisLoader.
     */
    @Test(expected = CorbException.class)
    public void testHasNext_throwException() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        try {
            instance.hasNext();
        } finally {
            instance.close();
        }
        fail();
    }

    @Test
    public void testHasNext() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile(URIS_FILE);
        instance.options = options;
        instance.open();

        for (int i = 0; i < instance.getTotalCount(); i++) {
            assertTrue(instance.hasNext());
        }
        //Verify that hasNext() does not advance the buffered reader to the next line
        assertTrue(instance.hasNext());
        instance.close();
    }

    /**
     * Test of next method, of class FileUrisLoader.
     */
    @Test
    public void testNext() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        options.setUrisFile(URIS_FILE);
        instance.options = options;
        instance.open();
        //Verify that hasNext() does not advance the buffered reader to the next line
        for (int i = 0; i < instance.getTotalCount(); i++) {
            assertNotNull(instance.next());
        }
        assertFalse(instance.hasNext());
        assertNull(instance.next());
        instance.close();
    }

    @Test
    public void testNext_withEmptyLine() throws Exception {
        FileUrisLoader instance = new FileUrisLoader();
        TransformOptions options = new TransformOptions();
        File file = File.createTempFile("temp", ".txt");
        file.deleteOnExit();
        Writer writer = new FileWriter(file);
        writer.append("foo\n\nbar");
        writer.close();
        options.setUrisFile(file.getAbsolutePath());
        instance.options = options;
        instance.open();

        assertEquals("foo", instance.next());
        assertEquals("bar", instance.next());
        assertFalse(instance.hasNext());
        assertNull(instance.next());
        instance.close();
    }

    /**
     * Test of close method, of class FileUrisLoader.
     */
    @Test
    public void testClose() {
        FileUrisLoader instance = new FileUrisLoader();
        instance.br = mock(BufferedReader.class);
        instance.close();
        assertNull(instance.br);
        instance.close();
    }

    /**
     * Test of cleanup method, of class FileUrisLoader.
     */
    @Test
    public void testCleanup() {
        FileUrisLoader instance = new FileUrisLoader();
        instance.br = mock(BufferedReader.class);
        instance.collection = "testCleanupCollection";
        instance.cs = mock(ContentSource.class);
        instance.nextLine = "testCleanup";
        instance.options = new TransformOptions();
        instance.properties = new Properties();
        instance.replacements = new String[]{};
        instance.setTotalCount(100);
        instance.close();
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
