/*
  * * Copyright (c) 2004-2023 MarkLogic Corporation
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class IOUtils {

    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = Logger.getLogger(IOUtils.class.getName());

    /**
     * Default buffer size used for I/O operations.
     * <p>
     * Set to 32 KB (32,768 bytes), which provides a good balance between memory usage
     * and I/O performance for most file operations. This size is commonly used in
     * buffered I/O operations to reduce the number of system calls.
     * </p>
     */
    public static final int BUFFER_SIZE = 32 * 1024;

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private IOUtils() {
    }

    /**
     * Tests whether the {@code InputStream} is a directory. A Directory will be
     * a ByteArrayInputStream and a File will be a BufferedInputStream.
     *
     * @param is The InputStream to test
     * @return {@code true} if the InputStream class is ByteArrayInputStream, {@code false} otherwise
     */
    public static boolean isDirectory(InputStream is) {
        return is instanceof ByteArrayInputStream;
    }

    /**
     * Reads all bytes from an InputStream and returns them as a byte array.
     *
     * @param is The InputStream to read from
     * @return A byte array containing all bytes read from the InputStream
     * @throws IOException If an I/O error occurs while reading from the stream
     */
    public static byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    /**
     * Reads all bytes from an InputStream and encodes them as a Base64 string.
     *
     * @param is The InputStream to read and encode
     * @return A Base64-encoded string representation of the InputStream contents
     * @throws IOException If an I/O error occurs while reading from the stream
     */
    public static String toBase64(InputStream is) throws IOException {
        byte[] bytes = toByteArray(is);
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Null-safe close operation of a {@code Closeable} object.
     * Silently ignores any IOException that occurs during closing and logs a warning.
     *
     * @param obj Closeable object to be closed, may be null
     */
    public static void closeQuietly(Closeable obj) {
        if (obj != null) {
            try {
                obj.close();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "IOException thrown closing object", ex);
                // Ignore
            }
        }
    }
}
