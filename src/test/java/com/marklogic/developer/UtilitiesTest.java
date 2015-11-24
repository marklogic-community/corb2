/*
 */
package com.marklogic.developer;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 *
 */
public class UtilitiesTest {

    final Date exampleDate;
    final String exampleContent = "The quick brown fox jumped over the lazy dog.";
    final File exampleContentFile;

    public UtilitiesTest() throws IOException {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.set(2012, 3, 8, 11, 22, 34);
        cal.set(Calendar.MILLISECOND, 0);
        exampleDate = cal.getTime();
        
        exampleContentFile = File.createTempFile("exampleContentFile", "txt");
        PrintWriter print = null;
        try {
            print = new PrintWriter(exampleContentFile, "UTF-8");
            print.write(exampleContent);
        } catch (FileNotFoundException fileNotFoundException) {
            throw fileNotFoundException;
        } catch (UnsupportedEncodingException unsupportedEncodingException) {
            throw unsupportedEncodingException;
        } finally {
            if (print != null) {
                print.close();
            }
        }
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() throws FileNotFoundException, IOException {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of parseDateTime method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testParseDateTime() throws Exception {
        System.out.println("parseDateTime");
        Date result = Utilities.parseDateTime("2012-04-08T11:22:34+00:00");
        assertEquals(exampleDate, result);
    }

    @org.junit.Test
    public void testParseDateTime_00() throws Exception {
        System.out.println("parseDateTime");
        Date result = Utilities.parseDateTime("2012-04-08T11:22:34+0000");
        assertEquals(exampleDate, result);
    }

    @org.junit.Test(expected = ParseException.class)
    public void testParseDateTime_BadDate() throws Exception {
        System.out.println("parseDateTime");
        Utilities.parseDateTime("10/27/2010");
    }

    /**
     * Test of formatDateTime method, of class Utilities.
     * @throws java.text.ParseException
     */
    @org.junit.Test
    public void testFormatDateTime_0args() throws ParseException {
        System.out.println("formatDateTime");
        String formattedDateNow = Utilities.formatDateTime();
        String roundTrip = Utilities.formatDateTime(Utilities.parseDateTime(formattedDateNow));
        assertEquals(roundTrip, formattedDateNow);
    }

    /**
     * Test of formatDateTime method, of class Utilities.
     * @throws java.text.ParseException
     */
    @org.junit.Test
    public void testFormatDateTime_Date() throws ParseException {
        System.out.println("formatDateTime");
        String formattedExampleDate = Utilities.formatDateTime(exampleDate);
        String roundTrip = Utilities.formatDateTime(Utilities.parseDateTime(formattedExampleDate));
        assertEquals(roundTrip, formattedExampleDate);
    }

    @org.junit.Test
    public void testFormatDateTime_DateIsNull() throws ParseException {
        System.out.println("formatDateTime");
        String formattedExampleDate1 = Utilities.formatDateTime(null);
        String formattedExampleDate2 = Utilities.formatDateTime(null);
        assertTrue(formattedExampleDate1.compareTo(formattedExampleDate2) >=0);
    }
    /**
     * Test of getPathExtension method, of class Utilities.
     */
    @org.junit.Test
    public void testGetPathExtension() {
        System.out.println("getPathExtension");
        String result = Utilities.getPathExtension("dir/dir/filename.csv");
        assertEquals("csv", result);
    }

    @org.junit.Test
    public void testGetPathExtension_multipleDotsInPath() {
        System.out.println("getPathExtension");
        String result = Utilities.getPathExtension("dir/dir/file.name.csv.txt");
        assertEquals("txt", result);
    }

    @org.junit.Test
    public void testGetPathExtension_noExtension() {
        System.out.println("getPathExtension");
        String path = "dir/dir/filename";
        String result = Utilities.getPathExtension("dir/dir/filename");
        assertEquals(path, result);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @org.junit.Test
    public void testJoin_List_String() {
        System.out.println("join");
        List items = Arrays.asList(new String[]{"a", "b", "c"});
        String result = Utilities.join(items, ",");
        assertEquals("a,b,c", result);
    }

    @org.junit.Test
    public void testJoin_List_StringIsNull() {
        System.out.println("join");
        List items = null;
        String result = Utilities.join(items, ",");
        assertEquals(null, result);
    }

    @org.junit.Test
    public void testJoin_emptyList() {
        System.out.println("join");
        List<String> items = new ArrayList<String>();
        String result = Utilities.join(items, ",");
        assertEquals("", result);
    }
    
    /**
     * Test of join method, of class Utilities.
     */
    @org.junit.Test
    public void testJoin_ObjectArr_String() {
        System.out.println("join");
        Object[] items = new Object[2];
        items[0] = 2;
        items[1] = "foo";
        String delim = "|";
        String result = Utilities.join(items, delim);
        assertEquals("2|foo", result);
    }

    @org.junit.Test(expected = NullPointerException.class)
    public void testJoin_ObjectArr_StringIsNull() {
        System.out.println("join");
        Object[] items = null;
        String delim = "|";
        Utilities.join(items, delim);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @org.junit.Test
    public void testJoin_StringArr_String() {
        System.out.println("join");
        String[] items = new String[]{"a", "b", "c"};
        String delim = ",";
        String result = Utilities.join(items, delim);
        assertEquals("a,b,c", result);
    }

    @org.junit.Test(expected = NullPointerException.class)
    public void testJoin_StringArr_StringIsNull() {
        System.out.println("join");
        String[] items = null;
        String delim = ",";
        String result = Utilities.join(items, delim);
    }

    /**
     * Test of escapeXml method, of class Utilities.
     */
    @org.junit.Test
    public void testEscapeXml() {
        System.out.println("escapeXml");
        String result = Utilities.escapeXml("<b>this & that</b>");
        assertEquals("&lt;b&gt;this &amp; that&lt;/b&gt;", result);
    }

    @org.junit.Test
    public void testEscapeXml_null() {
        System.out.println("escapeXml");
        String result = Utilities.escapeXml(null);
        assertEquals("", result);
    }

    /**
     * Test of copy method, of class Utilities.
     */
    @org.junit.Test
    public void testCopy_File_File() throws Exception {
        System.out.println("copy");

        File out = File.createTempFile("copiedFile", "txt");
        Utilities.copy(exampleContentFile, out);
        String result = null;
        BufferedReader reader;
        try { 
            reader = new BufferedReader(new FileReader(out));
            result = reader.readLine();
        } catch (Exception ex) {
            
        }
        assertEquals(exampleContent, result);
        out.deleteOnExit();
    }

    @org.junit.Test (expected = IOException.class)
    public void testCopy_null_InputStream() throws Exception {
        System.out.println("copy");
        InputStream in = null;
        File out = File.createTempFile("copiedFile", "txt");
        out.deleteOnExit();
        Utilities.copy(in, new FileOutputStream(out));
    }
    
     @org.junit.Test (expected = IOException.class)
    public void testCopy_InputStream_null() throws Exception {
        System.out.println("copy");
        InputStream in = new FileInputStream(exampleContentFile);
        Utilities.copy(in, null);
    }
    
    /**
     * Test of copy method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testCopy_Reader_OutputStream() throws Exception {
        System.out.println("copy");
        Reader in = new FileReader(exampleContentFile);
        OutputStream out = new ByteArrayOutputStream();

        long result = Utilities.copy(in, out);
        assertEquals(exampleContent.length(), result);
    }

    @org.junit.Test (expected = IOException.class)
    public void testCopy_ReaderIsNull_OutputStream() throws IOException {
        Reader in = null;
        OutputStream out = new ByteArrayOutputStream();
        long result = Utilities.copy(in, out);
    }
    
    @org.junit.Test (expected = IOException.class)
    public void testCopy_Reader_OutputStreamIsNull() throws IOException {
        Reader in = new FileReader(exampleContentFile);
        OutputStream out = null;
        long result = Utilities.copy(in, out);
    }
    
    /**
     * Test of copy method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testCopy_String_String() throws Exception {
        System.out.println("copy");
        String inFilePath = exampleContentFile.getAbsolutePath();
        File destFile = File.createTempFile("output", "txt");
        String outFilePath = destFile.getAbsolutePath();
        Utilities.copy(inFilePath, outFilePath);
        BufferedReader reader;
        String result = null;
        try {
            reader = new BufferedReader(new FileReader(destFile));
            result = reader.readLine();
        } catch (Exception e){
        }
        assertEquals(exampleContent, result);
        destFile.deleteOnExit();
    }

    /**
     * Test of deleteFile method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testDeleteFile_File() throws Exception {
        System.out.println("deleteFile");
        File file = File.createTempFile("originalFile", "txt");
        Utilities.deleteFile(file);
        assertFalse(file.exists());
    }

    @org.junit.Test
    public void testDeleteFile_FileIsNull() throws IOException {
        File file = new File("/tmp/_doesNotExit_" + Math.random());
        Utilities.deleteFile(file);
        file.deleteOnExit();
    }
    
    @org.junit.Test
    public void testDeleteFile_FolderIsEmpty() throws IOException {
        Path tempPath = Files.createTempDirectory("foo");
        File tempDirectory = tempPath.toFile();
        Utilities.deleteFile(tempPath.toFile());
        tempDirectory.deleteOnExit();
    }
    
    @org.junit.Test
    public void testDeleteFile_FolderHasFiles() throws IOException {
        Path tempPath = Files.createTempDirectory("foo");
        File tempDirectory = tempPath.toFile();
        File.createTempFile("deleteFile", "bar", tempDirectory);
        Utilities.deleteFile(tempDirectory);
        tempDirectory.deleteOnExit();
    }
    
    @org.junit.Test
    public void testDeleteFile_StringIsNull() throws IOException {
        String filename = "/tmp/_doesNotExist_" + Math.random();
        Utilities.deleteFile(filename);
    }
    
    /**
     * Test of stringToBoolean method, of class Utilities.
     */
    @org.junit.Test
    public void testStringToBoolean_String() {
        System.out.println("stringToBoolean");
        assertFalse(Utilities.stringToBoolean(""));
        assertFalse(Utilities.stringToBoolean("0"));
        assertFalse(Utilities.stringToBoolean("f"));
        assertFalse(Utilities.stringToBoolean("false"));
        assertFalse(Utilities.stringToBoolean("FALSE"));
        assertFalse(Utilities.stringToBoolean("FaLsE"));
        assertFalse(Utilities.stringToBoolean("n"));
        assertFalse(Utilities.stringToBoolean("no"));
        assertFalse(Utilities.stringToBoolean(null));
    }

    @org.junit.Test
    public void testStringToBoolean_String_true() {
        System.out.println("stringToBoolean");
        assertTrue(Utilities.stringToBoolean("true"));
        assertTrue(Utilities.stringToBoolean("asdf"));
        assertTrue(Utilities.stringToBoolean("123"));
        assertTrue(Utilities.stringToBoolean("00"));
        assertTrue(Utilities.stringToBoolean("Y"));
    }

    /**
     * Test of stringToBoolean method, of class Utilities.
     */
    @org.junit.Test
    public void testStringToBoolean_String_boolean() {
        System.out.println("stringToBoolean");
        assertFalse(Utilities.stringToBoolean(null, false));
        assertTrue(Utilities.stringToBoolean(null, true));
    }

    /**
     * Test of buildModulePath method, of class Utilities.
     */
    @org.junit.Test
    public void testBuildModulePath_Class() {
        System.out.println("buildModulePath");
        String result = Utilities.buildModulePath(String.class);
        assertEquals("/java/lang/String.xqy", result);
    }

    /**
     * Test of buildModulePath method, of class Utilities.
     */
    @org.junit.Test
    public void testBuildModulePath_Package_String() {
        System.out.println("buildModulePath");
        Package modulePackage = Package.getPackage("com.marklogic.developer");
        String result = Utilities.buildModulePath(modulePackage, "Utilities");
        assertEquals("/com/marklogic/developer/Utilities.xqy", result);
    }

    @org.junit.Test
    public void testBuildModulePath_Package_String_withSuffix() {
        System.out.println("buildModulePath");
        Package _package = Package.getPackage("com.marklogic.developer");
        String result = Utilities.buildModulePath(_package, "Utilities.xqy");
        assertEquals("/com/marklogic/developer/Utilities.xqy", result);
    }

    /**
     * Test of cat method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testCat_Reader() throws Exception {
        System.out.println("cat");

        Reader reader = new FileReader(exampleContentFile);
        String result = Utilities.cat(reader);
        assertEquals(exampleContent, result);
    }

    /**
     * Test of cat method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testCat_InputStream() throws Exception {
        System.out.println("cat");

        InputStream is = new FileInputStream(exampleContentFile);
        byte[] result = Utilities.cat(is);
        assertArrayEquals(exampleContent.getBytes(), result);
    }

    /**
     * Test of getSize method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testGetSize_InputStream() throws Exception {
        System.out.println("getSize");
        long result = -1;
        InputStream is;
        try {
            is = new FileInputStream(exampleContentFile);
            result = Utilities.getSize(is);
            
        } catch (Exception ex) {}
        assertEquals(exampleContent.length(), result);
    }

    /**
     * Test of getSize method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testGetSize_Reader() throws Exception {
        System.out.println("getSize");

        Reader reader = new FileReader(exampleContentFile);
        long result = Utilities.getSize(reader);
        assertEquals(exampleContent.length(), result);
    }

    /**
     * Test of getBytes method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testGetBytes() throws Exception {
        System.out.println("getBytes");

        byte[] result = Utilities.getBytes(exampleContentFile);
        assertArrayEquals(exampleContent.getBytes(), result);
    }

    /**
     * Test of dumpHex method, of class Utilities.
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testDumpHex() throws Exception {
        System.out.println("dumpHex");
        String result = Utilities.dumpHex("abcd", "UTF-8");
        assertEquals("61 62 63 64", result);
    }

    @org.junit.Test(expected = NullPointerException.class)
    public void testDumpHex_null() throws Exception {
        System.out.println("dumpHex");
        String result = Utilities.dumpHex(null, "UTF-8");
    }

    @org.junit.Test(expected = UnsupportedEncodingException.class)
    public void testDumpHex_unsupportedEncoding() throws Exception {
        System.out.println("dumpHex");
        String result = Utilities.dumpHex("abcd", "does not exist");
    }
}
