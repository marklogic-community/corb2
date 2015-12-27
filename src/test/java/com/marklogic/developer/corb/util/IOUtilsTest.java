/*
  * * Copyright (c) 2004-2015 MarkLogic Corporation
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
package com.marklogic.developer.corb.util;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class IOUtilsTest {

    final File exampleContentFile = new File("src/test/resources/test-file-1.csv");
    final String exampleContent;
   
    public IOUtilsTest() throws IOException {
        exampleContent = IOUtils.cat(new FileReader(exampleContentFile));
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
     * Test of closeQuietly method, of class IOUtils.
     */
    @Test
    public void testCloseQuietly() throws IOException {
        System.out.println("closeQuietly");
        Closeable obj = new StringReader("foo");
        IOUtils.closeQuietly(obj);
        assertTrue(true);
    }

    @Test
    public void testCloseQuietly_throws() throws IOException {
        System.out.println("closeQuietly");
        Closeable closeable = new Closeable() {
            @Override
            public void close() throws IOException {
                throw new IOException("test IO");
            }
        };
        IOUtils.closeQuietly(closeable);
        assertTrue(true); //did not throw IOException
    }

    @Test
    public void testCloseQuietly_null() throws IOException {
        System.out.println("closeQuietly");
        Closeable closeable = null;
        IOUtils.closeQuietly(closeable);
        assertTrue(true); //did not throw IOException
    }
    
      @Test (expected = IOException.class)
    public void testCopy_null_InputStream() throws Exception {
        System.out.println("copy");
        InputStream in = null;
        File out = File.createTempFile("copiedFile", "txt");
        out.deleteOnExit();
        IOUtils.copy(in, new FileOutputStream(out));
    }
    
     @Test (expected = IOException.class)
    public void testCopy_InputStream_null() throws Exception {
        System.out.println("copy");
        InputStream in = new FileInputStream(exampleContentFile);
        IOUtils.copy(in, null);
    }
    
    /**
     * Test of copy method, of class Utilities.
     * @throws java.lang.Exception
     */
    @Test
    public void testCopy_Reader_OutputStream() throws Exception {
        System.out.println("copy");
        Reader in = new FileReader(exampleContentFile);
        OutputStream out = new ByteArrayOutputStream();

        long result = IOUtils.copy(in, out);
        assertEquals(exampleContent.length(), result);
    }

    @Test (expected = IOException.class)
    public void testCopy_ReaderIsNull_OutputStream() throws IOException {
        Reader in = null;
        OutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(in, out);
    }
    
    @Test (expected = IOException.class)
    public void testCopy_Reader_OutputStreamIsNull() throws IOException {
        Reader in = new FileReader(exampleContentFile);
        OutputStream out = null;
        IOUtils.copy(in, out);
    }
    
    /**
     * Test of cat method, of class Utilities.
     * @throws java.lang.Exception
     */
    @Test
    public void testCat_Reader() throws Exception {
        System.out.println("cat");

        Reader reader = new FileReader(exampleContentFile);
        String result = IOUtils.cat(reader);
        assertEquals(exampleContent, result);
    }

    /**
     * Test of cat method, of class Utilities.
     * @throws java.lang.Exception
     */
    @Test
    public void testCat_InputStream() throws Exception {
        System.out.println("cat");

        InputStream is = new FileInputStream(exampleContentFile);
        byte[] result = IOUtils.cat(is);
        assertArrayEquals(exampleContent.getBytes(), result);
    }

    /**
     * Test of getSize method, of class Utilities.
     * @throws java.lang.Exception
     */
    @Test
    public void testGetSize_InputStream() throws Exception {
        System.out.println("getSize");
        long result = -1;
        InputStream is;
        try {
            is = new FileInputStream(exampleContentFile);
            result = IOUtils.getSize(is);    
        } catch (Exception ex) {}
        assertEquals(exampleContent.length(), result);
    }

    /**
     * Test of getSize method, of class Utilities.
     * @throws java.lang.Exception
     */
    @Test
    public void testGetSize_Reader() throws Exception {
        System.out.println("getSize");
        Reader reader = new FileReader(exampleContentFile);
        long result = IOUtils.getSize(reader);
        assertEquals(exampleContent.length(), result);
    }

    /**
     * Test of getBytes method, of class Utilities.
     * @throws java.lang.Exception
     */
    @Test
    public void testGetBytes() throws Exception {
        System.out.println("getBytes");
        byte[] result = FileUtils.getBytes(exampleContentFile);
        assertArrayEquals(exampleContent.getBytes(), result);
    }
}
