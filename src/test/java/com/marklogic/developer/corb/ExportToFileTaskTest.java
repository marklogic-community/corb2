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
import java.nio.file.Files;
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

    private ExportToFileTask newTaskWithUri(String... uris) {
        ExportToFileTask task = new ExportToFileTask();
        task.exportDir = tempFolder.toString();
        task.setInputURI(uris);
        return task;
    }

    private File writeResultFile(ResultSequence resultSequence, String... uris) {
        ExportToFileTask task = newTaskWithUri(uris);
        try {
            task.writeToFile(resultSequence);
            return new File(task.exportDir, task.getFileName());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
            return null;
        }
    }

    @Test
    void testGetFileName() {
        ExportToFileTask instance = newTaskWithUri("https://github.com/marklogic-community/corb2");
        assertEquals("https://github.com/marklogic-community/corb2", instance.getFileName());
    }

    @Test
    void testGetFileNameWithLeadingSlash() {
        ExportToFileTask instance = newTaskWithUri(SLASH + FOO);
        assertEquals(FOO, instance.getFileName());
    }

    @Test
    void testGetFileNameWithoutSlashAndExportFileUriToPathFalse() {
        ExportToFileTask instance = newTaskWithUri(FOO);
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        assertEquals(FOO, instance.getFileName());
    }

    @Test
    void testGetFileNameSlashAndExportFileUriToPathFalse() {
        ExportToFileTask instance = newTaskWithUri(SLASH);
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        assertEquals("", instance.getFileName());
    }

    @Test
    void testGetFileNameExportFileUriToPathFalse() {
        ExportToFileTask instance = newTaskWithUri(SLASH + FOO + SLASH + FOO);
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        assertEquals(FOO, instance.getFileName());
    }

    @Test
    void testGetFileNameSlashBookendExportFileUriToPathFalse() {
        ExportToFileTask instance = newTaskWithUri(SLASH + FOO + SLASH);
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(false));
        assertEquals(FOO + SLASH, instance.getFileName());
    }

    @Test
    void testGetFileNameSlashBookendExportFileUriToPathTrue() {
        ExportToFileTask instance = newTaskWithUri(SLASH + FOO + SLASH);
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(true));
        assertEquals(FOO + SLASH, instance.getFileName());
    }

    @Test
    void testGetFileNameTrailingSlashEmptyExportFileUriToPathTrue() {
        ExportToFileTask instance = newTaskWithUri(FOO + SLASH);
        instance.properties.setProperty(Options.EXPORT_FILE_URI_TO_PATH, Boolean.toString(true));
        assertEquals(FOO + SLASH, instance.getFileName());
    }

    @Test
    void testGetFileNameNullInputURI() {
        ExportToFileTask instance = new ExportToFileTask();
        assertThrows(NullPointerException.class, instance::getFileName);
    }

    @Test
    void testGetExportFileCreatesParentDirectories() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = tempFolder.resolve("nested/path").toString();
        File exportFile = instance.getExportFile("docs/example.txt");
        assertTrue(exportFile.getParentFile().exists());
        assertEquals(new File(instance.exportDir, "docs/example.txt"), exportFile);
    }

    @Test
    void testWriteToExportFileIgnoresBlankContent() throws IOException {
        ExportToFileTask instance = newTaskWithUri("/file.txt");
        File exportFile = instance.getExportFile();
        instance.writeToExportFile("   \n\t  ");
        assertFalse(exportFile.exists());
    }

    @Test
    void testWriteToFileNullSequence() {
        ResultSequence seq = null;
        File file = writeResultFile(seq, "/testFile");
        assertFalse(file.exists());
    }

    @Test
    void testWriteToFileNoResults() {
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(Boolean.FALSE);
        File file = writeResultFile(seq, "/testFile");
        assertFalse(file.exists());
    }

    @Test
    void testWriteToFile() {
        ResultSequence seq = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);
        when(seq.hasNext()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(seq.next()).thenReturn(resultItem);
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn("testWriteToFile");
        File file = writeResultFile(seq, "/testFile");
        assertTrue(file.exists());
    }

    @Test
    void testWriteToExportFileAppendsContent() throws IOException {
        ExportToFileTask instance = newTaskWithUri("/testFile");
        File exportFile = instance.getExportFile();
        Files.write(exportFile.toPath(), "start\n".getBytes());
        instance.writeToExportFile("tail");
        assertEquals("start\ntail\n", new String(Files.readAllBytes(exportFile.toPath())));
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
        when(seq.hasNext()).thenReturn(true, true, false);
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
    void testInvokeModuleNoModuleNotRequired() {
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
