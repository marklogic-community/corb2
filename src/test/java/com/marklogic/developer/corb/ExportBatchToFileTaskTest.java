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
package com.marklogic.developer.corb;

import com.marklogic.xcc.ResultSequence;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ExportBatchToFileTaskTest {

    public ExportBatchToFileTaskTest() {
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
     * Test of getFileName method, of class ExportBatchToFileTask.
     */
    @Test
    public void testGetFileName_fromURISBatchRef() {
        System.out.println("getFileName");
        Properties props = new Properties();
        props.setProperty("URIS_BATCH_REF", "foo/bar/baz");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getFileName();
        assertEquals("baz", result);
    }

    @Test
    public void testGetFileName_fromEXPORTFILENAME() {
        System.out.println("getFileName");
        Properties props = new Properties();
        props.setProperty("EXPORT-FILE-NAME", "foo/bar");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getFileName();
        assertEquals("foo/bar", result);
    }

    @Test(expected = NullPointerException.class)
    public void testGetFileName_withEmptyExportFileName() {
        System.out.println("getFileName");
        Properties props = new Properties();
        props.setProperty("EXPORT-FILE-NAME", "");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        instance.getFileName();
    }

    @Test(expected = NullPointerException.class)
    public void testGetFileName_withEmptyUrisBatchRef() {
        System.out.println("getFileName");
        Properties props = new Properties();
        props.setProperty("URIS_BATCH_REF", "");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        instance.getFileName();
    }

    /**
     * Test of getPartFileName method, of class ExportBatchToFileTask.
     */
    @Test(expected = NullPointerException.class)
    public void testGetPartFileName_emptyName() {
        System.out.println("getPartFileName");
        Properties props = new Properties();
        props.setProperty("URIS_BATCH_REF", "");
        props.setProperty("EXPORT-FILE-PART-EXT", ".txt");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getPartFileName();
        assertEquals("", result);
    }

    @Test
    public void testGetPartFileName_withExtension() {
        System.out.println("getPartFileName");
        Properties props = new Properties();
        props.setProperty("URIS_BATCH_REF", "foo");
        props.setProperty("EXPORT-FILE-PART-EXT", ".txt");
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.properties = props;
        String result = instance.getPartFileName();
        assertEquals("foo.txt", result);
    }

    /**
     * Test of writeToFile method, of class ExportBatchToFileTask.
     */
    @Test
    public void testWriteToFile_nullSeq() throws Exception {
        System.out.println("writeToFile");
        ResultSequence seq = null;
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.writeToFile(seq);
    }

    @Test
    public void testWriteToFile_notSeqHasNext() throws Exception {
        System.out.println("writeToFile");
        ResultSequence seq = mock(ResultSequence.class);
        when(seq.hasNext()).thenReturn(false);
        ExportBatchToFileTask instance = new ExportBatchToFileTask();
        instance.writeToFile(seq);
    }

}
