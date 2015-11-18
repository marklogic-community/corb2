/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.AbstractTask.TRUE;
import com.marklogic.xcc.ResultSequence;
import java.io.File;
import java.io.RandomAccessFile;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author mhansen
 */
public class ExportToFileTaskTest {
    
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    public ExportToFileTaskTest() {
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
     * Test of getFileName method, of class ExportToFileTask.
     */
    @Test
    public void testGetFileName() {
        System.out.println("getFileName");
        ExportToFileTask instance = new ExportToFileTask();
        String expected = "https://github.com/marklogic/corb2";
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals(expected, filename);
    }
    
    @Test
    public void testGetFileName_withLeadingSlash() {
        System.out.println("getFileName");
        ExportToFileTask instance = new ExportToFileTask();
        String expected = "/corb2";
        String[] uri = {expected};
        instance.setInputURI(uri);
        String filename = instance.getFileName();
        assertEquals("corb2", filename);
    }
    
    @Test (expected = NullPointerException.class)
    public void testGetFileName_nullInputURI() {
        System.out.println("getFileName");
        ExportToFileTask instance = new ExportToFileTask();
        instance.getFileName();
    }

    /**
     * Test of writeToFile method, of class ExportToFileTask.
     */
    @Test
    public void testWriteToFile() throws Exception {
        System.out.println("writeToFile");
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = tempFolder.newFolder().toString();
        String[] uri = {"/testFile"};
        instance.setInputURI(uri);
        instance.writeToFile(seq);
        File file = new File(instance.exportDir, instance.getFileName());
        file.delete();
    }
    
    @Test
    public void testWriteToFile_silentFail() throws Exception {
        System.out.println("writeToFile");
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        instance.writeToFile(seq);
    }

    /**
     * Test of processResult method, of class ExportToFileTask.
     */
    @Test
    public void testProcessResult() throws Exception {
        System.out.println("processResult");
        ResultSequence seq = null;
        ExportToFileTask instance = new ExportToFileTask();
        String result = instance.processResult(seq);
        assertEquals(TRUE, result);
    }

    /**
     * Test of cleanup method, of class ExportToFileTask.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        ExportToFileTask instance = new ExportToFileTask();
        instance.exportDir = "test";
        instance.cleanup();
        assertNull(instance.exportDir);
    }

}
