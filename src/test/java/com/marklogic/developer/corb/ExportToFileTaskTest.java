/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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

    /**
     * Test of getFileName method, of class ExportToFileTask.
     */
    @Test
    public void testGetFileName() {
        ExportToFileTask instance = new ExportToFileTask();
        String expected = "https://github.com/marklogic/corb2";
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(expected, filename);
    }
    
    @Test
    public void testGetFileName_withLeadingSlash() {
        ExportToFileTask instance = new ExportToFileTask();
        String expected = "/corb2";
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals("corb2", filename);
    }
    
    @Test (expected = NullPointerException.class)
    public void testGetFileName_nullInputURI() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.getFileName();
        fail();
    }

    /**
     * Test of writeToFile method, of class ExportToFileTask.
     */
    @Test
    public void testWriteToFile_nullSequence() throws Exception {
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = tempFolder.newFolder().toString();
        String[] uri = {"/testFile"};
        instance.setInputURI(uri);
        instance.writeToFile(seq);
        File file = new File(instance.exportDir, instance.getFileName());
        assertFalse(file.exists());
        file.delete();
    }
    
    @Test
    public void testWriteToFile_silentFail() throws Exception {
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        instance.writeToFile(seq);
    }

    /**
     * Test of processResult method, of class ExportToFileTask.
     */
    @Test
    public void testProcessResult_noResults() throws Exception {
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        String result = instance.processResult(seq);
        assertEquals(TRUE, result);
    }

    @Test (expected = NullPointerException.class)
    public void testProcessResult_nullInputUris() throws Exception {
        ResultSequence seq = mock(ResultSequence.class);
        
        when(seq.hasNext()).thenReturn(true).thenReturn(false);
        ExportToFileTask instance = new ExportToFileTask();
   
        instance.processResult(seq);
        fail();
    }
    
    @Test
    public void testProcessResult() throws Exception {
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
        instance.exportDir = TestUtils.createTempDirectory().toString();
        String result = instance.processResult(seq);
        assertEquals(TRUE, result);
    }
    
    /**
     * Test of cleanup method, of class ExportToFileTask.
     */
    @Test
    public void testCleanup() {
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = "test";
        instance.cleanup();
        assertNull(instance.exportDir);
    }

    /**
     * Test of call method, of class ExportToFileTask.
     */
    @Test
    public void testCall() throws Exception {
        ExportToFileTask instance = new ExportToFileTask();
        instance.call();
    }

}
