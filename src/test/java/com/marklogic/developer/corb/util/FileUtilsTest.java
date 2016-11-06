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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
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

    @Test
    public void testGetBytes() throws IOException {
        byte[] result = getBytes(new File("src/test/resources/uriInputFile.txt"));
        assertArrayEquals("Hello from the URIS-FILE!".getBytes(), result);
    }

    @Test
    public void testCopyFileFile() throws IOException {
        File out = File.createTempFile("copiedFile", TEXT_FILE_EXT);
        out.deleteOnExit();
        copy(exampleContentFile, out);
        
        assertArrayEquals(getBytes(exampleContentFile), getBytes(out));
    }

    @Test
    public void testCopyStringString() throws IOException {
        String inFilePath = exampleContentFile.getAbsolutePath();
        File destFile = File.createTempFile("output", TEXT_FILE_EXT);
        destFile.deleteOnExit();
        String outFilePath = destFile.getAbsolutePath();
        copy(inFilePath, outFilePath);

        assertArrayEquals(getBytes(exampleContentFile), getBytes(destFile));
    }

    @Test
    public void testDeleteFileFile() throws IOException {
        File file = File.createTempFile("originalFile", TEXT_FILE_EXT);
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    public void testDeleteFileFileIsNull() throws IOException {
        File file = new File("/tmp/_doesNotExit_" + Math.random());
        file.deleteOnExit();
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    public void testDeleteFileFolderIsEmpty() throws IOException {
        File tempDirectory = createTempDirectory();
        tempDirectory.deleteOnExit();
        FileUtils.deleteFile(tempDirectory);
        assertFalse(tempDirectory.exists());
    }

    @Test
    public void testDeleteFileFolderHasFiles() throws IOException {
        File tempDirectory = createTempDirectory();
        File tempFile = File.createTempFile("deleteFile", "bar", tempDirectory);
        tempDirectory.deleteOnExit();
        tempFile.deleteOnExit();
        FileUtils.deleteFile(tempDirectory);
        assertFalse(tempFile.exists());
        assertFalse(tempDirectory.exists());
    }

    @Test
    public void testDeleteFileStringIsNull() throws IOException {
        String filename = "/tmp/_doesNotExist_" + Math.random();
        FileUtils.deleteFile(filename);
        assertFalse(new File(filename).exists());
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteFileCannotDelete() throws IOException {
        File exceptionalFile = mock(File.class);
        when(exceptionalFile.exists()).thenReturn(true);
        when(exceptionalFile.isDirectory()).thenReturn(false);
        when(exceptionalFile.delete()).thenReturn(false);
        when(exceptionalFile.getCanonicalPath()).thenReturn("does/not/exist");

        FileUtils.deleteFile(exceptionalFile);
    }

    @Test
    public void testGetLineCountNull() throws IOException {
        assertEquals(0, FileUtils.getLineCount(null));
    }

    @Test
    public void testGetLineCountFileDoesNotExist() throws IOException {
        assertEquals(0, FileUtils.getLineCount(new File("does/not/exist2")));
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
    public void testGetFileFromClasspath() {
        File file = FileUtils.getFile("doesNotExist");
        assertFalse(file.exists());
        file = FileUtils.getFile("selector.xqy");
        assertTrue(file.exists());
    }

    @Test
    public void testGetFileAbsolutePath() throws IOException {
        File file = File.createTempFile("getFile", TEXT_FILE_EXT);
        file.deleteOnExit();
        file.createNewFile();
        File retrievedFile = FileUtils.getFile(file.getAbsolutePath());
        assertTrue(retrievedFile.exists());
    }

    @Test
    public void testGetFileDoesNotExist() {
        File file = FileUtils.getFile("fileDoesNotExist");
        assertFalse(file.exists());
    }

    public static File createTempDirectory()
            throws IOException {
        return Files.createTempDirectory("temp", new FileAttribute<?>[0]).toFile(); 
    }

    /**
     * Copy a file to a new location.
     *
     * @param source A file to copy.
     * @param destination The new file where it should be copied to.
     * @throws IOException
     */
    public static void copy(final File source, final File destination) throws IOException {
        try (InputStream inputStream = new FileInputStream(source); 
                OutputStream outputStream = new FileOutputStream(destination)) {
            IOUtilsTest.copy(inputStream, outputStream);
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
        try (InputStream inputStream = new FileInputStream(sourceFilePath); 
                OutputStream outputStream = new FileOutputStream(destinationFilePath)) {    
            IOUtilsTest.copy(inputStream, outputStream);
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

        byte[] buf = new byte[BUFFER_SIZE];
        int read;
        
        try (InputStream is = new FileInputStream(contentFile); 
                ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            while ((read = is.read(buf)) > 0) {
                os.write(buf, 0, read);
            }
            return os.toByteArray();
        }   
    }
}
