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
package com.marklogic.developer.corb.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.LineNumberReader;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUtils {

    public static final int BUFFER_SIZE = IOUtils.BUFFER_SIZE;

    private FileUtils() {
    }

    /**
     *
     * @param file
     * @throws IOException
     */
    public static void deleteFile(File file) throws IOException {
        if (!file.exists()) {
            return;
        }
        boolean success;
        if (!file.isDirectory()) {
            success = file.delete();
            if (!success) {
                throw new IOException("error deleting " + file.getCanonicalPath());
            }
            return;
        }
        // directory, so recurse
        File[] children = file.listFiles();
        if (children != null) {
            for (File children1 : children) {
                // recurse
                deleteFile(children1);
            }
        }
        // now this directory should be empty
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * @param path
     * @throws IOException
     */
    public static void deleteFile(String path) throws IOException {
        deleteFile(new File(path));
    }

    /**
     * @param contentFile
     * @return
     * @throws IOException
     */
    public static byte[] getBytes(File contentFile) throws IOException {
        InputStream is = null;
        ByteArrayOutputStream os = null;
        byte[] buf = new byte[BUFFER_SIZE];
        int read;
        try {
            is = new FileInputStream(contentFile);
            try {
                os = new ByteArrayOutputStream();
                while ((read = is.read(buf)) > 0) {
                    os.write(buf, 0, read);
                }
                return os.toByteArray();
            } finally {
                closeQuietly(os);
            }
        } finally {
            closeQuietly(is);
        }
    }

    /**
     * @param source
     * @param destination
     * @throws IOException
     */
    public static void copy(final File source, final File destination) throws IOException {
        InputStream inputStream = new FileInputStream(source);
        try {
            OutputStream outputStream = new FileOutputStream(destination);
            try {
                IOUtils.copy(inputStream, outputStream);
            } finally {
                closeQuietly(inputStream);
            }
        } finally {
            closeQuietly(inputStream);
        }
    }

    /**
     * @param sourceFilePath
     * @param destinationFilePath
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void copy(final String sourceFilePath, final String destinationFilePath) throws FileNotFoundException, IOException {
        InputStream inputStream = new FileInputStream(sourceFilePath);
        try {
            OutputStream outputStream = new FileOutputStream(destinationFilePath);
            try {
                IOUtils.copy(inputStream, outputStream);
            } finally {
                closeQuietly(outputStream);
            }
        } finally {
            closeQuietly(inputStream);
        }
    }

    public static void moveFile(final File source, final File dest) {
        if (!source.getAbsolutePath().equals(dest.getAbsolutePath())) {
            if (source.exists()) {
                if (dest.exists()) {
                    dest.delete();
                }
                source.renameTo(dest);
            }
        }
    }

    public static int getLineCount(final File file) throws IOException {
        if (file != null && file.exists()) {
            LineNumberReader lnr = null;
            try {
                lnr = new LineNumberReader(new FileReader(file));
                lnr.skip(Long.MAX_VALUE);
                return lnr.getLineNumber();
            } finally {
                closeQuietly(lnr);
            }
        }
        return 0;
    }

}
