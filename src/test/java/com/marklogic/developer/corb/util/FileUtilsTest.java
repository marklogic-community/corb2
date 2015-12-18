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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.exceptions.base.MockitoException;
/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUtilsTest {

    
    final File exampleContentFile = new File("src/test/resources/test-file-1.csv");
    final String exampleContent;
    
    public FileUtilsTest() throws IOException {
        exampleContent = IOUtils.cat(new FileReader(exampleContentFile));
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of getBytes method, of class FileUtils.
     */
    @Test
    public void testGetBytes() throws Exception {
        System.out.println("getBytes");
        byte[] result = FileUtils.getBytes(new File("src/test/resources/uriInputFile.txt"));
        assertArrayEquals("Hello from the URIS-FILE!".getBytes(), result);
    }

    /**
     * Test of copy method, of class Utilities.
     */
    @Test
    public void testCopy_File_File() throws Exception {
        System.out.println("copy");

        File out = File.createTempFile("copiedFile", "txt");
        out.deleteOnExit();
        FileUtils.copy(exampleContentFile, out);

        Assert.assertArrayEquals(FileUtils.getBytes(exampleContentFile), FileUtils.getBytes(out));
        
    }

    /**
     * Test of copy method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testCopy_String_String() throws Exception {
        System.out.println("copy");
        String inFilePath = exampleContentFile.getAbsolutePath();
        File destFile = File.createTempFile("output", "txt");
        destFile.deleteOnExit();
        String outFilePath = destFile.getAbsolutePath();
        FileUtils.copy(inFilePath, outFilePath);
  
        Assert.assertArrayEquals(FileUtils.getBytes(exampleContentFile), FileUtils.getBytes(destFile));      
    }

    /**
     * Test of deleteFile method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testDeleteFile_File() throws Exception {
        System.out.println("deleteFile");
        File file = File.createTempFile("originalFile", "txt");
        FileUtils.deleteFile(file);
        assertFalse(file.exists());
    }

    @Test
    public void testDeleteFile_FileIsNull() throws IOException {
        File file = new File("/tmp/_doesNotExit_" + Math.random());
        FileUtils.deleteFile(file);
        file.deleteOnExit();
    }

    @Test
    public void testDeleteFile_FolderIsEmpty() throws IOException {
        Path tempPath = Files.createTempDirectory("foo");
        File tempDirectory = tempPath.toFile();
        FileUtils.deleteFile(tempPath.toFile());
        tempDirectory.deleteOnExit();
    }

    @Test
    public void testDeleteFile_FolderHasFiles() throws IOException {
        Path tempPath = Files.createTempDirectory("foo");
        File tempDirectory = tempPath.toFile();
        File.createTempFile("deleteFile", "bar", tempDirectory);
        FileUtils.deleteFile(tempDirectory);
        tempDirectory.deleteOnExit();
    }

    @Test
    public void testDeleteFile_StringIsNull() throws IOException {
        String filename = "/tmp/_doesNotExist_" + Math.random();
        FileUtils.deleteFile(filename);
    }
    
    @Test (expected = IOException.class)
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
        System.out.println("getLineCount");
        assertEquals(0, FileUtils.getLineCount(null));
    }
    
    @Test
    public void testGetLineCount_fileDoesNotExist() throws IOException {
        System.out.println("getLineCount");
        assertEquals(0, FileUtils.getLineCount(new File("does/not/exist")));
    }
    
    @Test
    public void testGetLineCount() throws IOException {
        System.out.println("getLineCount");
        assertEquals(12, FileUtils.getLineCount(exampleContentFile));
    }
}
