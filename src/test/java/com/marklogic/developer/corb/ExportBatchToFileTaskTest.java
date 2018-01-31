/*
  * * Copyright (c) 2004-2018 MarkLogic Corporation
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
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
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
        props.setProperty(Options.EXPORT_FILE_NAME, filename);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getFileName();
        assertEquals(filename, result);
    }

    @Test(expected = NullPointerException.class)
    public void testGetFileNameWithEmptyExportFileName() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_NAME, EMPTY);
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
        File file = testWriteToFile(seq);
        try {
            assertEquals(3, FileUtils.getLineCount(file));
            assertEquals("foo\nbar\nbaz\n", TestUtils.readFile(file));
        } catch (IOException ex) {
            fail();
        }
    }

    public File testWriteToFile(ResultSequence resultSequence) {
        Properties props = new Properties();
        try {
            File batchFile = File.createTempFile("test", "txt");
            props.setProperty(Options.EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            batchFile.delete();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Unable to create temp file", ex);
            fail();
        }
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.setProperties(props);
        try {
            instance.writeToFile(new ResultItemIterator(resultSequence));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        File exportFile = instance.getExportFile();
        exportFile.deleteOnExit();
        return exportFile;
    }

}
