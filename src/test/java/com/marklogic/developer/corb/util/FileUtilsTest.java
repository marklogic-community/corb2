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
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUtilsTest {

    private final File exampleContentFile = new File("src/test/resources/test-file-1.csv");
    private static final String TEXT_FILE_EXT = "txt";

    /**
     * Test of getBytes method, of class FileUtils.
     */
    @Test
    public void testGetBytes() throws Exception {
        byte[] result = getBytes(new File("src/test/resources/uriInputFile.txt"));
        assertArrayEquals("Hello from the URIS-FILE!".getBytes(), result);
    }

    /**
     * Test of copy method, of class Utilities.
     */
    @Test
    public void testCopy_File_File() throws Exception {

        File out = File.createTempFile("copiedFile", TEXT_FILE_EXT);
        out.deleteOnExit();
        copy(exampleContentFile, out);

        assertArrayEquals(getBytes(exampleContentFile), getBytes(out));
    }

    /**
     * Test of copy method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testCopy_String_String() throws Exception {
        String inFilePath = exampleContentFile.getAbsolutePath();
        File destFile = File.createTempFile("output", TEXT_FILE_EXT);
        destFile.deleteOnExit();
        String outFilePath = destFile.getAbsolutePath();
        copy(inFilePath, outFilePath);

        assertArrayEquals(getBytes(exampleContentFile), getBytes(destFile));
    }

    /**
     * Test of deleteFile method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testDeleteFile_File() throws Exception {
        File file = File.createTempFile("originalFile", TEXT_FILE_EXT);
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    public void testDeleteFile_FileIsNull() throws IOException {
        File file = new File("/tmp/_doesNotExit_" + Math.random());
        file.deleteOnExit();
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    public void testDeleteFile_FolderIsEmpty() throws IOException {
        File tempDirectory = createTempDirectory();
        tempDirectory.deleteOnExit();
        FileUtils.deleteFile(tempDirectory);
        assertFalse(tempDirectory.exists());
    }

    @Test
    public void testDeleteFile_FolderHasFiles() throws IOException {
        File tempDirectory = createTempDirectory();
        File tempFile = File.createTempFile("deleteFile", "bar", tempDirectory);
        tempDirectory.deleteOnExit();
        tempFile.deleteOnExit();
        FileUtils.deleteFile(tempDirectory);
        assertFalse(tempFile.exists());
        assertFalse(tempDirectory.exists());
    }

    @Test
    public void testDeleteFile_StringIsNull() throws IOException {
        String filename = "/tmp/_doesNotExist_" + Math.random();
        FileUtils.deleteFile(filename);
        assertFalse(new File(filename).exists());
    }

    @Test(expected = IOException.class)
    public void testDeleteFile_cannotDelete() throws IOException {
        File exceptionalFile = mock(File.class);
        when(exceptionalFile.exists()).thenReturn(true);
        when(exceptionalFile.isDirectory()).thenReturn(false);
        when(exceptionalFile.delete()).thenReturn(false);
        when(exceptionalFile.getCanonicalPath()).thenReturn("does/not/exist");

        FileUtils.deleteFile(exceptionalFile);
    }

    @Test
    public void testGetLineCount_null() throws IOException {
        assertEquals(0, FileUtils.getLineCount(null));
    }

    @Test
    public void testGetLineCount_fileDoesNotExist() throws IOException {
        assertEquals(0, FileUtils.getLineCount(new File("does/not/exist")));
    }

    @Test
    public void testGetLineCount() throws IOException {
        assertEquals(12, FileUtils.getLineCount(exampleContentFile));
    }

    @Test
    public void testMoveFile() throws IOException {
        File file = File.createTempFile("moveFile", TEXT_FILE_EXT);
        file.deleteOnExit();
        file.createNewFile();
        FileUtils.moveFile(file, file);
        assertTrue(file.exists());
    }

    @Test
    public void testGetFile_fromClasspath() {
        File file = FileUtils.getFile("doesNotExist");
        assertFalse(file.exists());
        file = FileUtils.getFile("selector.xqy");
        assertTrue(file.exists());
    }

    @Test
    public void testGetFile_absolutePath() throws IOException {
        File file = File.createTempFile("getFile", TEXT_FILE_EXT);
        file.deleteOnExit();
        file.createNewFile();
        File retrievedFile = FileUtils.getFile(file.getAbsolutePath());
        assertTrue(retrievedFile.exists());
    }

    @Test
    public void testGetFile_doesNotExist() {
        File file = FileUtils.getFile("doesNotExist");
        assertFalse(file.exists());
    }

    //TODO remove when we upgrade to JRE 1.7+
    public static File createTempDirectory()
            throws IOException {
        final File temp;

        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if (!(temp.delete())) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if (!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return (temp);
    }

    /**
     * Copy a file to a new location.
     *
     * @param source A file to copy.
     * @param destination The new file where it should be copied to.
     * @throws IOException
     */
    public static void copy(final File source, final File destination) throws IOException {
        InputStream inputStream = new FileInputStream(source);
        try {
            OutputStream outputStream = new FileOutputStream(destination);
            try {
                IOUtilsTest.copy(inputStream, outputStream);
            } finally {
                closeQuietly(inputStream);
            }
        } finally {
            closeQuietly(inputStream);
        }
    }

    /**
     * Copy a file to a new location.
     *
     * @param sourceFilePath Path to the existing file.
     * @param destinationFilePath Path where the file should be copied to.
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void copy(final String sourceFilePath, final String destinationFilePath) throws FileNotFoundException, IOException {
        InputStream inputStream = new FileInputStream(sourceFilePath);
        try {
            OutputStream outputStream = new FileOutputStream(destinationFilePath);
            try {
                IOUtilsTest.copy(inputStream, outputStream);
            } finally {
                closeQuietly(outputStream);
            }
        } finally {
            closeQuietly(inputStream);
        }
    }

    /**
     * Read the <code>byte[]</code> of a file.
     *
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
}
