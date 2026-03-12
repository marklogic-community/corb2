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
package com.marklogic.developer.corb.util;

import static com.marklogic.developer.corb.util.IOUtils.BUFFER_SIZE;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class IOUtilsTest {

    private static final Logger LOG = Logger.getLogger(IOUtilsTest.class.getName());
    private static final File exampleContentFile = new File("src/test/resources/test-file-1.csv");
    private final String exampleContent;
    private static final String NULL_OUTPUTSTREAM_MSG = "null OutputStream";

    public IOUtilsTest() throws IOException {
        exampleContent = cat(new FileReader(exampleContentFile));
    }

    @Test
    void testCloseQuietly() {
        Closeable obj = new StringReader("foo");
        IOUtils.closeQuietly(obj);
    }

    @Test
    void testCloseQuietlyThrows() {
        Closeable closeable = () -> {
            throw new IOException("test IO");
        };
        IOUtils.closeQuietly(closeable);
        //did not throw IOException
    }

    @Test
    void testCloseQuietlyNull() {
        Closeable closeable = null;
        IOUtils.closeQuietly(closeable);
        //did not throw IOException
    }

    @Test
    void testCopyNullInputStream() throws IOException {
        InputStream in = null;
        File out = File.createTempFile("copiedFile", "txt");
        out.deleteOnExit();
        assertThrows(IOException.class, () -> copy(in, Files.newOutputStream(out.toPath())));
    }

    @Test
    void testCopyInputStreamNull() throws IOException {
        try (InputStream in = Files.newInputStream(exampleContentFile.toPath())) {
            assertThrows(IOException.class, () -> copy(in, null));
        }
    }

    @Test
    void testCopyReaderOutputStream() {
        try (Reader in = new FileReader(exampleContentFile)){
            OutputStream out = new ByteArrayOutputStream();
            long result = copy(in, out);
            assertEquals(exampleContent.length(), result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCopyReaderIsNullOutputStream() {
        Reader in = null;
        OutputStream out = new ByteArrayOutputStream();
        assertThrows(IOException.class, () -> copy(in, out));
    }

    @Test
    void testCopyReaderOutputStreamIsNull() throws IOException {
        try (Reader in = new FileReader(exampleContentFile)) {
            OutputStream out = null;
            assertThrows(IOException.class, () -> copy(in, out));
        }
    }

    @Test
    void testCatReader() {
        try (Reader reader = new FileReader(exampleContentFile)) {
            String result = cat(reader);
            assertEquals(exampleContent, result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCatInputStream() {
        try (InputStream is = Files.newInputStream(exampleContentFile.toPath())) {
            byte[] result = cat(is);
            assertArrayEquals(exampleContent.getBytes(), result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetSizeInputStream() {
        long result = -1;
        try (InputStream is = Files.newInputStream(exampleContentFile.toPath())) {
            result = getSize(is);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(exampleContent.length(), result);
    }

    @Test
    void testGetSizeReader() {
        try (Reader reader = new FileReader(exampleContentFile)) {
            long result = getSize(reader);
            assertEquals(exampleContent.length(), result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetBytes() {
        try {
            byte[] result = FileUtilsTest.getBytes(exampleContentFile);
            assertArrayEquals(exampleContent.getBytes(), result);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    public static String cat(Reader r) throws IOException {
        StringBuilder rv = new StringBuilder();
        int size;
        char[] buf = new char[BUFFER_SIZE];
        while ((size = r.read(buf)) > 0) {
            rv.append(buf, 0, size);
        }
        return rv.toString();
    }

    public static byte[] cat(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
        copy(is, bos);
        return bos.toByteArray();
    }

    public static long copy(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        if (inputStream == null) {
            throw new IOException("null InputStream");
        }
        if (outputStream == null) {
            throw new IOException(NULL_OUTPUTSTREAM_MSG);
        }
        long totalBytes = 0;
        int len;
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

    public static long copy(final Reader source, final OutputStream destination) throws IOException {
        if (source == null) {
            throw new IOException("null Reader");
        }
        if (destination == null) {
            throw new IOException(NULL_OUTPUTSTREAM_MSG);
        }
        long totalBytes = 0;
        int len;
        char[] buf = new char[BUFFER_SIZE];
        byte[] bite;
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

    public static long getSize(InputStream is) throws IOException {
        long size = 0;
        int b;
        byte[] buf = new byte[BUFFER_SIZE];
        while ((b = is.read(buf)) > 0) {
            size += b;
        }
        return size;
    }

    public static long getSize(Reader r) throws IOException {
        long size = 0;
        int b;
        char[] buf = new char[BUFFER_SIZE];
        while ((b = r.read(buf)) > 0) {
            size += b;
        }
        return size;
    }
}
