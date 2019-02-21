/*
 * Copyright (c) 2004-2019 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.AbstractTask.TRUE;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.XdmItem;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ExportToFileTaskTest {

    private static final Logger LOG = Logger.getLogger(ExportToFileTaskTest.class.getName());
    public static final String FOO = "foo";
    public static final String SLASH = "/";
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        clearSystemProperties();
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    @Test
    public void testGetFileName() {
        ExportToFileTask instance = new ExportToFileTask();
        String expected = "https://github.com/marklogic-community/corb2";
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(expected, filename);
    }

    @Test
    public void testGetFileNameWithLeadingSlash() {
        ExportToFileTask instance = new ExportToFileTask();
        String expected = SLASH + FOO;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO, filename);
    }

    @Test
    public void testGetFileNameWithoutSlashAndExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String expected = FOO;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(expected, filename);
    }

    @Test
    public void testGetFileNameSlashAndExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String[] uri = {SLASH};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals("", filename);
    }

    @Test
    public void testGetFileNameExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String expected = SLASH + FOO + SLASH + FOO;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO, filename);
    }

    @Test
    public void testGetFileNameSlashBookendExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String expected = SLASH + FOO + SLASH;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO + SLASH, filename);
    }

    @Test
    public void testGetFileNameSlashBookendExportFileUriToPathTrue() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(true));
        String expected = SLASH + FOO + SLASH;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO + SLASH, filename);
    }

    @Test
    public void testGetFileNameTrailingSlashEmptyExportFileUriToPathTrue() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(true));
        String expected = FOO + SLASH;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO + SLASH, filename);
    }

    @Test(expected = NullPointerException.class)
    public void testGetFileNameNullInputURI() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.getFileName();
        fail();
    }

    @Test
    public void testWriteToFileNullSequence() {
        ResultSequence seq = null;
        File file = testWriteEmptyResults(seq);
        assertFalse(file.exists());
    }

    @Test
    public void testWriteToFileNoResults() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(Boolean.FALSE);
        File file = testWriteEmptyResults(seq);
        assertFalse(file.exists());
    }

    @Test
    public void testWriteToFile() {
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);
        when(seq.hasNext()).thenReturn(Boolean.TRUE).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);
        when(seq.next()).thenReturn(resultItem);
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("testWriteToFile");
        File file = testWriteEmptyResults(seq);
        assertTrue(file.exists());
    }

    public File testWriteEmptyResults(ResultSequence resultSequence) {
        File file = null;
        ExportToFileTask instance = new ExportToFileTask();
        String[] uri = {"/testFile"};
        try {
            instance.exportDir = tempFolder.newFolder().toString();
            instance.setInputURI(uri);
            instance.writeToFile(resultSequence);
            file = new File(instance.exportDir, instance.getFileName());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        return file;
    }

    @Test
    public void testProcessResultNoResults() {
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        String result;
        try {
            result = instance.processResult(seq);
            assertEquals(TRUE, result);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test (expected = CorbException.class)
    public void testProcessResultIOException() throws CorbException {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenThrow(IOException.class);
        ExportToFileTask instance = new ExportToFileTask();
        instance.processResult(seq);
    }

    @Test(expected = NullPointerException.class)
    public void testProcessResultNullInputUris() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(true).thenReturn(false);

        ExportToFileTask instance = new ExportToFileTask();
        try {
            instance.processResult(seq);
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testProcessResult() {
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem item = mock(XdmItem.class);
        when(seq.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(seq.next()).thenReturn(resultItem);
        when(resultItem.getItem()).thenReturn(item);
        when(item.asString()).thenReturn("item");
        ExportToFileTask instance = new ExportToFileTask();
        String[] uris = {"foo.xqy"};
        instance.inputUris = uris;
        try {
            instance.exportDir = TestUtils.createTempDirectory().toString();
            String result = instance.processResult(seq);
            assertEquals(TRUE, result);
        } catch (IOException | CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    @Test
    public void testCleanup() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = "test";
        instance.cleanup();
        assertNull(instance.exportDir);
    }

    @Test
    public void testCall() {
        ExportToFileTask instance = new ExportToFileTask();
        String[] result;
        try {
            result = instance.call();
            assertNotNull(result);
            assertTrue(result.length == 0);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

}
