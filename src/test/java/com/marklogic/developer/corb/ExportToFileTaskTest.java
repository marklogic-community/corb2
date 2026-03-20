/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.io.TempDir;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class ExportToFileTaskTest {

    private static final Logger LOG = Logger.getLogger(ExportToFileTaskTest.class.getName());
    public static final String FOO = "foo";
    public static final String SLASH = "/";
    @TempDir
    public Path tempFolder;

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    @Test
    void testGetFileName() {
        ExportToFileTask instance = new ExportToFileTask();
        String expected = "https://github.com/marklogic-community/corb2";
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(expected, filename);
    }

    @Test
    void testGetFileNameWithLeadingSlash() {
        ExportToFileTask instance = new ExportToFileTask();
        String expected = SLASH + FOO;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO, filename);
    }

    @Test
    void testGetFileNameWithoutSlashAndExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String expected = FOO;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(expected, filename);
    }

    @Test
    void testGetFileNameSlashAndExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String[] uri = {SLASH};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals("", filename);
    }

    @Test
    void testGetFileNameExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String expected = SLASH + FOO + SLASH + FOO;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO, filename);
    }

    @Test
    void testGetFileNameSlashBookendExportFileUriToPathFalse() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        String expected = SLASH + FOO + SLASH;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO + SLASH, filename);
    }

    @Test
    void testGetFileNameSlashBookendExportFileUriToPathTrue() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(true));
        String expected = SLASH + FOO + SLASH;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO + SLASH, filename);
    }

    @Test
    void testGetFileNameTrailingSlashEmptyExportFileUriToPathTrue() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(true));
        String expected = FOO + SLASH;
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(FOO + SLASH, filename);
    }

    @Test
    void testGetFileNameNullInputURI() {
        ExportToFileTask instance = new ExportToFileTask();
        assertThrows(NullPointerException.class, instance::getFileName);
    }

    @Test
    void testWriteToFileNullSequence() {
        ResultSequence seq = null;
        File file = testWriteEmptyResults(seq);
        assertFalse(file.exists());
    }

    @Test
    void testWriteToFileNoResults() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(Boolean.FALSE);
        File file = testWriteEmptyResults(seq);
        assertFalse(file.exists());
    }

    @Test
    void testWriteToFile() {
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
            instance.exportDir = tempFolder.toString();
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
    void testProcessResultNoResults() {
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

    @Test
    void testProcessResultIOException() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenThrow(new RuntimeException("boom!", new IOException()));
        ExportToFileTask instance = new ExportToFileTask();
        assertThrows(RuntimeException.class, () -> instance.processResult(seq));
    }

    @Test
    void testProcessResultNullInputUris() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(true).thenReturn(false);

        ExportToFileTask instance = new ExportToFileTask();
        assertThrows(NullPointerException.class, () -> instance.processResult(seq),
            "Expected NullPointerException when inputUris is null because it can't generate export file name");
    }

    @Test
    void testProcessResult() {
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
    void testCleanup() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = "test";
        instance.cleanup();
        assertNull(instance.exportDir);
    }

    @Test
    void testCall() {
        ExportToFileTask instance = new ExportToFileTask();
        try {
            assertThrows(CorbException.class, instance::call);
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    void testInvokeModule() {
        ExportToFileTask exportToFileTask = new ExportToFileTask();
        assertThrows(CorbException.class, exportToFileTask::invokeModule);
    }

    @Test
    void testInvokeModuleNoModuleNotRequired()  {
        ExportToFileTask exportToFileTask = new ExportToFileTask();
        exportToFileTask.properties.setProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
        try {
            String[] result = exportToFileTask.invokeModule();
            assertEquals(0, result.length);
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testInvokeModuleNoModuleRequired() {
        ExportToFileTask exportToFileTask = new ExportToFileTask();
        exportToFileTask.properties.setProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE, "true");
        assertThrows(CorbException.class, exportToFileTask::invokeModule);
    }
}
