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
package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.TestUtils.assertEqualsNormalizeNewline;
import static org.junit.Assert.*;

import com.marklogic.xcc.types.XdmItem;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ExportBatchToFileTaskTest {

    private static final String EMPTY = "";
    private static final String TXT_EXT = ".txt";
    private static final Logger LOG = Logger.getLogger(ExportBatchToFileTaskTest.class.getName());

    @Test
    public void testGetFileNameFromURISBatchRef() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, "foo/bar/baz");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getFileName();
        assertEquals("baz", result);
    }

    @Test
    public void testGetFileNameFromEXPORTFILENAME() {
        String filename = "foo/bar";
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_NAME, filename);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getFileName();
        assertEquals(filename, result);
    }

    @Test(expected = NullPointerException.class)
    public void testGetFileNameWithEmptyExportFileName() {
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_NAME, EMPTY);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        instance.getFileName();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testGetFileNameWithEmptyUrisBatchRef() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, EMPTY);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        instance.getFileName();
        fail();
    }

    @Test(expected = NullPointerException.class)
    public void testGetPartFileNameEmptyName() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, EMPTY);
        props.setProperty(Options.EXPORT_FILE_PART_EXT, TXT_EXT);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getPartFileName();
        assertEquals("", result);
        fail();
    }

    @Test
    public void testGetPartFileNameWithExtension() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, "foo");
        props.setProperty(Options.EXPORT_FILE_PART_EXT, TXT_EXT);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getPartFileName();
        assertEquals("foo.txt", result);
    }

    @Test
    public void testWriteToFileNullSeq() {
        ResultSequence seq = null;
        File file = testWriteToFile(seq);
        assertFalse(file.exists());
    }

    @Test
    public void testWriteToFileNotSeqHasNext()  {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(false);
        File file = testWriteToFile(seq);
        assertFalse(file.exists());
    }

    @Test
    public void testWriteToFileWithMultipleItems()  {
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(seq.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(seq.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("foo").thenReturn("bar").thenReturn("baz");

        try {
            File file = testWriteToFile(seq);
            assertEquals(3, FileUtils.getLineCount(file));
            assertEqualsNormalizeNewline("foo\nbar\nbaz\n", TestUtils.readFile(file));
        } catch (IOException ex) {
            ex.printStackTrace();
            fail();
        }
    }

    @Test
    public void testWriteToFileWithFilenameAndNoExportFileDir() {
        testWriteToFileWithNullExportFileDir("myFile.txt");
    }

    @Test
    public void testWriteToFileWithRelativeFolderStructureAndNoExportFileDir() {
        testWriteToFileWithNullExportFileDir("build/testWriteToFileWithRelativeFolderStructureAndNoExportFileDir/a/b/c/myFile.txt");
        FileUtils.deleteQuietly(Paths.get("build/testWriteToFileWithRelativeFolderStructureAndNoExportFileDir"));
    }

    private void testWriteToFileWithNullExportFileDir(String exportFileName) {
        File exportFile = new File(exportFileName);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties.setProperty(EXPORT_FILE_NAME, exportFileName);
        try {
            instance.writeToExportFile("test");
            assertTrue(exportFile.exists());
            assertEqualsNormalizeNewline("test\n", TestUtils.readFile(exportFile));
        } catch (IOException ex) {
            fail();
        } finally {
            exportFile.delete();
        }
    }

    public File testWriteToFile(ResultSequence resultSequence) {
        Properties props = new Properties();
        try {
            File batchFile = File.createTempFile("test", ".txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getCanonicalPath());
            batchFile.delete();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Unable to create temp file", ex);
            fail();
        }
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.setProperties(props);
        try {
            instance.writeToFile(resultSequence);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            fail();
        }
        File exportFile = instance.getExportFile();
        exportFile.deleteOnExit();
        return exportFile;
    }

    @Test
    public void testWriteToFileWithSplitByLines() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-split.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "2");

            ExportBatchToFileTask instance = new ExportBatchToFileTask();
            instance.setProperties(props);

            // Create a mock ResultSequence with 5 items
            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            //Need one extra hasNext() because it's tested first in writeToFile(seq)
            when(seq.hasNext()).thenReturn(true, true, true, true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3", "line4", "line5");

            instance.writeToFile(seq);

            // Verify that split files were created
            File file0 = new File(tempDir, "test-split.txt");
            File file1 = new File(tempDir, "test-split1.txt");
            File file2 = new File(tempDir, "test-split2.txt");

            assertTrue("First file should exist", file0.exists());
            assertTrue("Second file should exist", file1.exists());
            assertTrue("Third file should exist", file2.exists());

            // Verify line counts
            assertEquals("First file should have 2 lines", 2, FileUtils.getLineCount(file0));
            assertEquals("Second file should have 2 lines", 2, FileUtils.getLineCount(file1));
            assertEquals("Third file should have 1 line", 1, FileUtils.getLineCount(file2));

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    public void testInsertIndexIntoFileName() {
        ExportBatchToFileTask instance = new ExportBatchToFileTask();

        // Test with extension
        assertEquals("output1.txt", instance.insertIndexIntoFileName("output.txt", 1));
        assertEquals("output2.csv", instance.insertIndexIntoFileName("output.csv", 2));

        // Test without extension
        assertEquals("output1", instance.insertIndexIntoFileName("output", 1));

        // Test with multiple dots
        assertEquals("my.output1.txt", instance.insertIndexIntoFileName("my.output.txt", 1));
    }

    @Test
    public void testGetMaxLines() {
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        Properties props = new Properties();

        // Test with valid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "100");
        instance.setProperties(props);
        assertEquals(100, instance.getMaxLines());

        // Test with no value
        props.clear();
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxLines());

        // Test with invalid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "not-a-number");
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxLines());
    }

    @Test
    public void testGetMaxSize() {
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        Properties props = new Properties();

        // Test with valid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_SIZE, "1024");
        instance.setProperties(props);
        assertEquals(1024, instance.getMaxSize());

        // Test with no value
        props.clear();
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxSize());

        // Test with invalid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_SIZE, "invalid");
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxSize());
    }

}
