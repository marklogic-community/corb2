/*
  * * Copyright 2005-2015 MarkLogic Corporation
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
package com.marklogic.developer.corb.io;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class IOUtils {

    /**
     * @param r
     * @return
     * @throws IOException
     */
    public static String cat(Reader r) throws IOException {
        StringBuilder rv = new StringBuilder();
        int size;
        char[] buf = new char[FileUtils.BUFFER_SIZE];
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
        ByteArrayOutputStream bos = new ByteArrayOutputStream(FileUtils.BUFFER_SIZE);
        FileUtils.copy(is, bos);
        return bos.toByteArray();
    }
    
    private IOUtils() {}
    
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
