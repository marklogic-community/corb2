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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class IOUtils {

    public static final int BUFFER_SIZE = 32 * 1024;

    private IOUtils() {
    }

    /**
     * @param r
     * @return
     * @throws IOException
     */
    @Deprecated
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
    @Deprecated
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
    @Deprecated
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
    @Deprecated
    public static long getSize(Reader r) throws IOException {
        long size = 0;
        int b = 0;
        char[] buf = new char[BUFFER_SIZE];
        while ((b = r.read(buf)) > 0) {
            size += b;
        }
        return size;
    }

    /**
     * Tests whether the <code>InputStream</code> is a directory. A Directory
     * will be a ByteArrayInputStream and a File will be a BufferedInputStream.
     *
     * @param is
     * @return <code>true</code> if the InputStream class is
     * ByteArrayInputStream
     */
    public static final boolean isDirectory(InputStream is) {
        return is.getClass().getSimpleName().equals("ByteArrayInputStream");
    }

    /**
     * Null-safe close operation of a <code>Closeable</code> object. 
     * @param obj Closable object to be closed.
     */
    public static void closeQuietly(Closeable obj) {
        if (obj != null) {
            try {
                obj.close();
            } catch (IOException ex) {
                // Ignore
            }
        }
    }
}
