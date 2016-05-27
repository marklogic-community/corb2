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
package com.marklogic.developer.corb.util;

import static com.marklogic.developer.corb.util.IOUtils.BUFFER_SIZE;
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
        exampleContent = cat(new FileReader(exampleContentFile));
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

    @Test(expected = IOException.class)
    public void testCopy_null_InputStream() throws Exception {
        System.out.println("copy");
        InputStream in = null;
        File out = File.createTempFile("copiedFile", "txt");
        out.deleteOnExit();
        copy(in, new FileOutputStream(out));
    }

    @Test(expected = IOException.class)
    public void testCopy_InputStream_null() throws Exception {
        System.out.println("copy");
        InputStream in = new FileInputStream(exampleContentFile);
        copy(in, null);
    }

    /**
     * Test of copy method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testCopy_Reader_OutputStream() throws Exception {
        System.out.println("copy");
        Reader in = new FileReader(exampleContentFile);
        OutputStream out = new ByteArrayOutputStream();

        long result = copy(in, out);
        assertEquals(exampleContent.length(), result);
    }

    @Test(expected = IOException.class)
    public void testCopy_ReaderIsNull_OutputStream() throws IOException {
        Reader in = null;
        OutputStream out = new ByteArrayOutputStream();
        copy(in, out);
    }

    @Test(expected = IOException.class)
    public void testCopy_Reader_OutputStreamIsNull() throws IOException {
        Reader in = new FileReader(exampleContentFile);
        OutputStream out = null;
        copy(in, out);
    }

    /**
     * Test of cat method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testCat_Reader() throws Exception {
        System.out.println("cat");

        Reader reader = new FileReader(exampleContentFile);
        String result = cat(reader);
        assertEquals(exampleContent, result);
    }

    /**
     * Test of cat method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testCat_InputStream() throws Exception {
        System.out.println("cat");

        InputStream is = new FileInputStream(exampleContentFile);
        byte[] result = cat(is);
        assertArrayEquals(exampleContent.getBytes(), result);
    }

    /**
     * Test of getSize method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetSize_InputStream() throws Exception {
        System.out.println("getSize");
        long result = -1;
        InputStream is;
        try {
            is = new FileInputStream(exampleContentFile);
            result = getSize(is);
        } catch (Exception ex) {
        }
        assertEquals(exampleContent.length(), result);
    }

    /**
     * Test of getSize method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetSize_Reader() throws Exception {
        System.out.println("getSize");
        Reader reader = new FileReader(exampleContentFile);
        long result = getSize(reader);
        assertEquals(exampleContent.length(), result);
    }

    /**
     * Test of getBytes method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testGetBytes() throws Exception {
        System.out.println("getBytes");
        byte[] result = FileUtilsTest.getBytes(exampleContentFile);
        assertArrayEquals(exampleContent.getBytes(), result);
    }

    /**
     * @param r
     * @return
     * @throws IOException
     */
    public static String cat(Reader r) throws IOException {
        StringBuilder rv = new StringBuilder();
        int size;
        char[] buf = new char[BUFFER_SIZE];
        while ((size = r.read(buf)) > 0) {
            rv.append(buf, 0, size);
        }
        return rv.toString();
    }

    /**
     * @param is
     * @return
     * @throws IOException
     */
    public static byte[] cat(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
        copy(is, bos);
        return bos.toByteArray();
    }

        /**
     *
     * @param inputStream
     * @param outputStream
     * @return
     * @throws IOException
     */
    public static long copy(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("null InputStream");
        }
        if (outputStream == null) {
            throw new IOException("null OutputStream");
        }
        long totalBytes = 0;
        int len = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        int available = inputStream.available();

        while ((len = inputStream.read(buf, 0, BUFFER_SIZE)) > -1) {
            outputStream.write(buf, 0, len);
            totalBytes += len;
        }

        // caller MUST close the stream for us
        outputStream.flush();
        // check to see if we copied enough data
        if (available > totalBytes) {
            throw new IOException("expected at least " + available + " Bytes, copied only " + totalBytes);
        }
        return totalBytes;
    }

    /**
     *
     * @param source
     * @param destination
     * @return
     * @throws IOException
     */
    public static long copy(final Reader source, final OutputStream destination) throws IOException {
        if (source == null) {
            throw new IOException("null Reader");
        }
        if (destination == null) {
            throw new IOException("null OutputStream");
        }
        long totalBytes = 0;
        int len = 0;
        char[] buf = new char[BUFFER_SIZE];
        byte[] bite = null;
        while ((len = source.read(buf)) > -1) {
            bite = new String(buf).getBytes();
            // len? different for char vs byte?
            // code is broken if I use bite.length, though
            destination.write(bite, 0, len);
            totalBytes += len;
        }
        // caller MUST close the stream for us
        destination.flush();
        // check to see if we copied enough data
        if (1 > totalBytes) {
            throw new IOException("expected at least " + 1 + " Bytes, copied only " + totalBytes);
        }
        return totalBytes;
    }
    
    /**
     *
     * @param is
     * @return
     * @throws IOException
     */
    public static long getSize(InputStream is) throws IOException {
        long size = 0;
        int b = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        while ((b = is.read(buf)) > 0) {
            size += b;
        }
        return size;
    }

    /**
     *
     * @param r
     * @return
     * @throws IOException
     */
    public static long getSize(Reader r) throws IOException {
        long size = 0;
        int b = 0;
        char[] buf = new char[BUFFER_SIZE];
        while ((b = r.read(buf)) > 0) {
            size += b;
        }
        return size;
    }
}
