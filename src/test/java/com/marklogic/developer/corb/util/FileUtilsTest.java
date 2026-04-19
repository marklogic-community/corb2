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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUtilsTest {

    private static final File exampleContentFile = new File("src/test/resources/test-file-1.csv");
    private static final String TEXT_FILE_EXT = "txt";

    // -------------------------------------------------------------------------
    // Existing tests
    // -------------------------------------------------------------------------

    @Test
    public void testGetBytes() throws IOException {
        byte[] result = getBytes(new File("src/test/resources/uriInputFile.txt"));
        assertArrayEquals("Hello from the URIS-FILE!".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void testCopyFileFile() throws IOException {
        File out = File.createTempFile("copiedFile", TEXT_FILE_EXT);
        out.deleteOnExit();
        copy(exampleContentFile, out);

        assertArrayEquals(getBytes(exampleContentFile), getBytes(out));
    }

    @Test
    void testCopyStringString() throws IOException {
        String inFilePath = exampleContentFile.getAbsolutePath();
        File destFile = File.createTempFile("output", TEXT_FILE_EXT);
        destFile.deleteOnExit();
        String outFilePath = destFile.getAbsolutePath();
        copy(inFilePath, outFilePath);

        assertArrayEquals(getBytes(exampleContentFile), getBytes(destFile));
    }

    @Test
    void testDeleteFileFile() throws IOException {
        File file = File.createTempFile("originalFile", TEXT_FILE_EXT);
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    void testDeleteFileFileIsNull() throws IOException {
        File file = new File("/tmp/_doesNotExit_" + Math.random());
        file.deleteOnExit();
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    void testDeleteFileFolderIsEmpty() throws IOException {
        File tempDirectory = createTempDirectory();
        tempDirectory.deleteOnExit();
        FileUtils.deleteFile(tempDirectory);
        assertFalse(tempDirectory.exists());
    }

    @Test
    void testDeleteFileFolderHasFiles() throws IOException {
        File tempDirectory = createTempDirectory();
        File tempFile = File.createTempFile("deleteFile", "bar", tempDirectory);
        tempDirectory.deleteOnExit();
        tempFile.deleteOnExit();
        FileUtils.deleteFile(tempDirectory);
        assertFalse(tempFile.exists());
        assertFalse(tempDirectory.exists());
    }

    @Test
    void testDeleteFileStringIsNull() throws IOException {
        String filename = "/tmp/_doesNotExist_" + Math.random();
        FileUtils.deleteFile(filename);
        assertFalse(new File(filename).exists());
    }

    @Test
    void testDeleteFileCannotDelete() throws IOException {
        File exceptionalFile = mock(File.class);
        when(exceptionalFile.exists()).thenReturn(true);
        when(exceptionalFile.isDirectory()).thenReturn(false);
        when(exceptionalFile.delete()).thenReturn(false);
        when(exceptionalFile.getCanonicalPath()).thenReturn("does/not/exist");

        assertThrows(NullPointerException.class, () -> FileUtils.deleteFile(exceptionalFile));
    }

    @Test
    void testGetLineCountNull() throws IOException {
        assertEquals(0, FileUtils.getLineCount(null));
    }

    @Test
    void testGetLineCountFileDoesNotExist() throws IOException {
        assertEquals(0, FileUtils.getLineCount(new File("does/not/exist2")));
    }

    @Test
    void testGetLineCount() throws IOException {
        assertEquals(12, FileUtils.getLineCount(exampleContentFile));
    }

    @Test
    void testMoveFile() throws IOException {
        File file = File.createTempFile("moveFile", TEXT_FILE_EXT);
        file.deleteOnExit();
        FileUtils.moveFile(file, file);
        assertTrue(file.exists());
    }

    @Test
    void testGetFileFromClasspath() {
        File file = FileUtils.getFile("doesNotExist");
        assertFalse(file.exists());
        file = FileUtils.getFile("selector.xqy");
        assertTrue(file.exists());
    }

    @Test
    void testGetFileAbsolutePath() throws IOException {
        File file = File.createTempFile("getFile", TEXT_FILE_EXT);
        file.deleteOnExit();
        File retrievedFile = FileUtils.getFile(file.getAbsolutePath());
        assertTrue(retrievedFile.exists());
    }

    @Test
    void testGetFileDoesNotExist() {
        File file = FileUtils.getFile("fileDoesNotExist");
        assertFalse(file.exists());
    }

    // -------------------------------------------------------------------------
    // deleteQuietly(Path)
    // -------------------------------------------------------------------------

    @Test
    void testDeleteQuietlyNullPath() {
        assertDoesNotThrow(() -> FileUtils.deleteQuietly(null));
    }

    @Test
    void testDeleteQuietlyNonExistentPath() {
        assertDoesNotThrow(() -> FileUtils.deleteQuietly(Paths.get("/does/not/exist/path")));
    }

    @Test
    void testDeleteQuietlyExistingFile() throws IOException {
        File file = File.createTempFile("deleteQuietly", ".tmp");
        FileUtils.deleteQuietly(file.toPath());
        assertFalse(file.exists());
    }

    @Test
    void testDeleteQuietlyIOException() throws IOException {
        // Make the child directory undeletable by removing write permission from its parent.
        // Files.delete(child) inside the file-visitor then throws AccessDeniedException,
        // which bubbles up to deleteQuietly's catch block.
        assumeTrue(filePermissionsAreEnforced(), "Skipping: file permissions not enforced (Windows or root)");
        File parent = createTempDirectory();
        File child = new File(parent, "child");
        assertTrue(child.mkdir());
        parent.setWritable(false);
        try {
            FileUtils.deleteQuietly(child.toPath()); // IOException caught quietly
        } finally {
            parent.setWritable(true);
            FileUtils.deleteQuietly(parent.toPath());
        }
    }

    // -------------------------------------------------------------------------
    // deleteFileQuietly(String, String)
    // -------------------------------------------------------------------------

    @Test
    void testDeleteFileQuietlyNullFilename() {
        assertDoesNotThrow(() -> FileUtils.deleteFileQuietly("/tmp", null));
    }

    @Test
    void testDeleteFileQuietlyExistingFile() throws IOException {
        File dir = createTempDirectory();
        File file = File.createTempFile("test", ".tmp", dir);
        assertTrue(file.exists());
        FileUtils.deleteFileQuietly(dir.getAbsolutePath(), file.getName());
        assertFalse(file.exists());
        dir.delete();
    }

    // -------------------------------------------------------------------------
    // moveFile(File, File)
    // -------------------------------------------------------------------------

    @Test
    void testMoveFileToDifferentLocation() throws IOException {
        File source = File.createTempFile("moveFrom", ".tmp");
        File dest = new File(source.getParentFile(), "moveTo_" + System.nanoTime() + ".tmp");
        dest.deleteOnExit();
        FileUtils.moveFile(source, dest);
        assertFalse(source.exists());
        assertTrue(dest.exists());
    }

    @Test
    void testMoveFileSourceDoesNotExist() {
        File source = new File("/does/not/exist/source.tmp");
        File dest = new File("/does/not/exist/dest.tmp");
        assertDoesNotThrow(() -> FileUtils.moveFile(source, dest));
    }

    @Test
    void testMoveFileRenameToFails() throws IOException {
        // When renameTo returns false the lazy-log lambda must be evaluated to cover
        // the lambda$moveFile$0 synthetic method.
        File dest = File.createTempFile("moveFileDest", ".tmp");
        dest.deleteOnExit();
        File source = mock(File.class);
        when(source.getAbsolutePath()).thenReturn("/fake/source/path.tmp");
        when(source.exists()).thenReturn(true);
        when(source.renameTo(dest)).thenReturn(false);
        assertDoesNotThrow(() -> FileUtils.moveFile(source, dest));
    }

    // -------------------------------------------------------------------------
    // FileUtils$1 (SimpleFileVisitor) -- postVisitDirectory with e != null
    // -------------------------------------------------------------------------

    @Test
    void testDeleteWithUnreadableSubdirectory() throws IOException {
        // Removing read permission on a sub-directory causes Files.newDirectoryStream to
        // throw AccessDeniedException, so postVisitDirectory is called with e != null,
        // handleException() returns TERMINATE and the walk ends early.
        assumeTrue(filePermissionsAreEnforced(), "Skipping: file permissions not enforced (Windows or root)");
        File parent = createTempDirectory();
        File inner = new File(parent, "inner");
        assertTrue(inner.mkdir());
        inner.setReadable(false);
        try {
            // Walk terminates early (TERMINATE) but should not throw
            try { FileUtils.deleteFile(parent); } catch (IOException ignored) { }
        } finally {
            inner.setReadable(true);
            FileUtils.deleteQuietly(parent.toPath());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    public static File createTempDirectory() throws IOException {
        return Files.createTempDirectory("temp").toFile();
    }

    /**
     * Returns true when file-system permission checks are actually enforced
     * (i.e. not running as root on Unix and not on Windows).
     */
    private static boolean filePermissionsAreEnforced() {
        if (System.getProperty("os.name", "").toLowerCase().contains("win")) {
            return false;
        }
        try {
            File dir = Files.createTempDirectory("permCheck").toFile();
            File child = new File(dir, "child.tmp");
            child.createNewFile();
            dir.setWritable(false);
            boolean enforced = !child.delete(); // root can still delete even without write on parent
            dir.setWritable(true);
            child.delete();
            dir.delete();
            return enforced;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Copy a file to a new location.
     *
     * @param source A file to copy.
     * @param destination The new file where it should be copied to.
     */
    public static void copy(final File source, final File destination) throws IOException {
        try (InputStream inputStream = Files.newInputStream(source.toPath());
             OutputStream outputStream = Files.newOutputStream(destination.toPath())) {
            IOUtilsTest.copy(inputStream, outputStream);
        }
    }

    /**
     * Copy a file to a new location.
     *
     * @param sourceFilePath Path to the existing file.
     * @param destinationFilePath Path where the file should be copied to.
     * @throws IOException if an I/O error occurs during copying.
     */
    public static void copy(final String sourceFilePath, final String destinationFilePath) throws IOException {
        try (InputStream inputStream = Files.newInputStream(Paths.get(sourceFilePath));
             OutputStream outputStream = Files.newOutputStream(Paths.get(destinationFilePath))) {
            IOUtilsTest.copy(inputStream, outputStream);
        }
    }

    /**
     * Read the {@code byte[]} of a file.
     *
     * @param contentFile The file to read.
     * @return A byte array containing the contents of the file.
     * @throws IOException if an I/O error occurs while reading the file.
     */
    public static byte[] getBytes(File contentFile) throws IOException {

        byte[] buf = new byte[BUFFER_SIZE];
        int read;

        try (InputStream is = Files.newInputStream(contentFile.toPath());
             ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            while ((read = is.read(buf)) > 0) {
                os.write(buf, 0, read);
            }
            return os.toByteArray();
        }
    }
}
