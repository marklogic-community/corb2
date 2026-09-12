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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.TestUtils.assertEqualsNormalizeNewline;
import com.marklogic.xcc.types.XdmItem;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class ExportBatchToFileTaskTest {

    private static final String EMPTY = "";
    private static final String TXT_EXT = ".txt";
    private static final Logger LOG = Logger.getLogger(ExportBatchToFileTaskTest.class.getName());

    private static ExportBatchToFileTask newTask(Properties props) {
        ExportBatchToFileTask task = new ExportBatchToFileTask();
        if (props != null) {
            task.properties = props;
        }
        return task;
    }

    @Test
    void testGetFileNameFromURISBatchRef() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, "foo/bar/baz");
        String result = newTask(props).getFileName();
        assertEquals("baz", result);
    }

    @Test
    void testGetFileNameFromEXPORTFILENAME() {
        String filename = "foo/bar";
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_NAME, filename);
        String result = newTask(props).getFileName();
        assertEquals(filename, result);
    }

    @Test
    void testGetFileNameWithEmptyExportFileName() {
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_NAME, EMPTY);
        assertThrows(NullPointerException.class, () -> newTask(props).getFileName());
    }

    @Test
    void testGetFileNameWithEmptyUrisBatchRef() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, EMPTY);
        assertThrows(NullPointerException.class, () -> newTask(props).getFileName());
    }

    @Test
    void testGetPartFileNameEmptyName() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, EMPTY);
        props.setProperty(Options.EXPORT_FILE_PART_EXT, TXT_EXT);
        assertThrows(NullPointerException.class, () -> newTask(props).getPartFileName());
    }

    @Test
    void testGetPartFileNameWithExtension() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, "foo");
        props.setProperty(Options.EXPORT_FILE_PART_EXT, TXT_EXT);
        assertEquals("foo.txt", newTask(props).getPartFileName());
    }

    @Test
    void testGetPartFileNameAddsDotWhenMissing() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, "foo");
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "txt");
        assertEquals("foo.txt", newTask(props).getPartFileName());
    }

    @Test
    void testGetPartFileNameWithoutPartExtension() {
        Properties props = new Properties();
        props.setProperty(Options.URIS_BATCH_REF, "foo");
        assertEquals("foo", newTask(props).getPartFileName());
    }

    @Test
    void testGetPartExtDefaultsToDotPart() {
        Properties props = new Properties();
        assertEquals(".part", newTask(props).getPartExt());
    }

    @Test
    void testGetPartExtNormalizesExtension() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "tmp");
        assertEquals(".tmp", newTask(props).getPartExt());
    }

    @Test
    void testWriteToFileNullSeq() {
        File file = testWriteToFile(null);
        assertFalse(file.exists());
    }

    @Test
    void testWriteToFileNotSeqHasNext() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(false);
        File file = testWriteToFile(seq);
        assertFalse(file.exists());
    }

    @Test
    void testWriteToFileWithMultipleItems() {
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(seq.hasNext()).thenReturn(true, true, true, true, false);
        when(seq.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("foo", "bar", "baz");

        try {
            File file = testWriteToFile(seq);
            assertEquals(3, FileUtils.getLineCount(file));
            assertEqualsNormalizeNewline("foo\nbar\nbaz\n", TestUtils.readFile(file));
        } catch (IOException ex) {
            fail();
        }
    }

    @Test
    void testWriteToFileAppendsToExistingContent() throws IOException {
        File file = File.createTempFile("batch", ".txt");
        Files.write(file.toPath(), "start\n".getBytes(StandardCharsets.UTF_8));

        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_NAME, file.getCanonicalPath());

        ResultSequence seq = mock(ResultSequence.class);
        ResultItem item = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);
        when(seq.hasNext()).thenReturn(true, true, true, false);
        when(seq.next()).thenReturn(item);
        when(item.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("foo", "bar");

        ExportBatchToFileTask instance = newTask(props);
        instance.writeToFile(seq, file);

        assertEqualsNormalizeNewline("start\nfoo\nbar\n", TestUtils.readFile(file));
        file.delete();
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
            instance.writeToFile(resultSequence, instance.getExportFile());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage(), ex);
            fail();
        }
        File exportFile = instance.getExportFile();
        exportFile.deleteOnExit();
        return exportFile;
    }

}
