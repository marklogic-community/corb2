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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class IOUtils {

    public static final int BUFFER_SIZE = 32 * 1024;

    private IOUtils() {
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
     *
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
