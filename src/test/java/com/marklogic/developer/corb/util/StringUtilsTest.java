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
package com.marklogic.developer.corb.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class StringUtilsTest {

    public StringUtilsTest() {
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

    @Test
    public void testStringToBoolean_String_empty() {
        System.out.println("stringToBoolean");
        boolean result = StringUtils.stringToBoolean("");
        assertFalse(result);
    }

    /**
     * Test of getPathExtension method, of class Utilities.
     */
    @org.junit.Test
    public void testGetPathExtension() {
        System.out.println("getPathExtension");
        String result = StringUtils.getPathExtension("dir/dir/filename.csv");
        assertEquals("csv", result);
    }

    @org.junit.Test
    public void testGetPathExtension_multipleDotsInPath() {
        System.out.println("getPathExtension");
        String result = StringUtils.getPathExtension("dir/dir/file.name.csv.txt");
        assertEquals("txt", result);
    }

    @org.junit.Test
    public void testGetPathExtension_noExtension() {
        System.out.println("getPathExtension");
        String path = "dir/dir/filename";
        String result = StringUtils.getPathExtension("dir/dir/filename");
        assertEquals(path, result);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @org.junit.Test
    public void testJoin_List_String() {
        System.out.println("join");
        List items = Arrays.asList(new String[]{"a", "b", "c"});
        String result = StringUtils.join(items, ",");
        assertEquals("a,b,c", result);
    }

    @org.junit.Test
    public void testJoin_List_StringIsNull() {
        System.out.println("join");
        List items = null;
        String result = StringUtils.join(items, ",");
        assertEquals(null, result);
    }

    @org.junit.Test
    public void testJoin_emptyList() {
        System.out.println("join");
        List<String> items = new ArrayList<String>();
        String result = StringUtils.join(items, ",");
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
        String result = StringUtils.join(items, delim);
        assertEquals("2|foo", result);
    }

    @org.junit.Test(expected = NullPointerException.class)
    public void testJoin_ObjectArr_StringIsNull() {
        System.out.println("join");
        Object[] items = null;
        String delim = "|";
        StringUtils.join(items, delim);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @org.junit.Test
    public void testJoin_StringArr_String() {
        System.out.println("join");
        String[] items = new String[]{"a", "b", "c"};
        String delim = ",";
        String result = StringUtils.join(items, delim);
        assertEquals("a,b,c", result);
    }

    @org.junit.Test(expected = NullPointerException.class)
    public void testJoin_StringArr_StringIsNull() {
        System.out.println("join");
        String[] items = null;
        String delim = ",";
        String result = StringUtils.join(items, delim);
    }

    /**
     * Test of escapeXml method, of class Utilities.
     */
    @org.junit.Test
    public void testEscapeXml() {
        System.out.println("escapeXml");
        String result = StringUtils.escapeXml("<b>this & that</b>");
        assertEquals("&lt;b&gt;this &amp; that&lt;/b&gt;", result);
    }

    @org.junit.Test
    public void testEscapeXml_null() {
        System.out.println("escapeXml");
        String result = StringUtils.escapeXml(null);
        assertEquals("", result);
    }

    /**
     * Test of stringToBoolean method, of class Utilities.
     */
    @org.junit.Test
    public void testStringToBoolean_String() {
        System.out.println("stringToBoolean");
        assertFalse(StringUtils.stringToBoolean(""));
        assertFalse(StringUtils.stringToBoolean("0"));
        assertFalse(StringUtils.stringToBoolean("f"));
        assertFalse(StringUtils.stringToBoolean("false"));
        assertFalse(StringUtils.stringToBoolean("FALSE"));
        assertFalse(StringUtils.stringToBoolean("FaLsE"));
        assertFalse(StringUtils.stringToBoolean("n"));
        assertFalse(StringUtils.stringToBoolean("no"));
        assertFalse(StringUtils.stringToBoolean(null));
    }

    @org.junit.Test
    public void testStringToBoolean_String_true() {
        System.out.println("stringToBoolean");
        assertTrue(StringUtils.stringToBoolean("true"));
        assertTrue(StringUtils.stringToBoolean("asdf"));
        assertTrue(StringUtils.stringToBoolean("123"));
        assertTrue(StringUtils.stringToBoolean("00"));
        assertTrue(StringUtils.stringToBoolean("Y"));
    }

    /**
     * Test of stringToBoolean method, of class Utilities.
     */
    @org.junit.Test
    public void testStringToBoolean_String_boolean() {
        System.out.println("stringToBoolean");
        assertFalse(StringUtils.stringToBoolean(null, false));
        assertTrue(StringUtils.stringToBoolean(null, true));
    }

    /**
     * Test of buildModulePath method, of class Utilities.
     */
    @org.junit.Test
    public void testBuildModulePath_Class() {
        System.out.println("buildModulePath");
        String result = StringUtils.buildModulePath(String.class);
        assertEquals("/java/lang/String.xqy", result);
    }

    /**
     * Test of buildModulePath method, of class Utilities.
     */
    @org.junit.Test
    public void testBuildModulePath_Package_String() {
        System.out.println("buildModulePath");
        Package modulePackage = Package.getPackage("com.marklogic.developer.corb.util");
        String result = StringUtils.buildModulePath(modulePackage, "Utilities");
        assertEquals("/com/marklogic/developer/corb/util/Utilities.xqy", result);
    }

    @org.junit.Test
    public void testBuildModulePath_Package_String_withSuffix() {
        System.out.println("buildModulePath");
        Package _package = Package.getPackage("com.marklogic.developer.corb.util");
        String result = StringUtils.buildModulePath(_package, "Utilities.xqy");
        assertEquals("/com/marklogic/developer/corb/util/Utilities.xqy", result);
    }

    /**
     * Test of dumpHex method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testDumpHex() throws Exception {
        System.out.println("dumpHex");
        String result = StringUtils.dumpHex("abcd", "UTF-8");
        assertEquals("61 62 63 64", result);
    }

    @org.junit.Test(expected = NullPointerException.class)
    public void testDumpHex_null() throws Exception {
        System.out.println("dumpHex");
        String result = StringUtils.dumpHex(null, "UTF-8");
    }

    @org.junit.Test(expected = UnsupportedEncodingException.class)
    public void testDumpHex_unsupportedEncoding() throws Exception {
        System.out.println("dumpHex");
        String result = StringUtils.dumpHex("abcd", "does not exist");
    }
    
    @Test
    public void testIsBlank() {
        System.out.println("isBlank");
        assertTrue(StringUtils.isBlank(""));
        assertTrue(StringUtils.isBlank("  "));
        assertTrue(StringUtils.isBlank(" \n"));
        assertTrue(StringUtils.isBlank(null));
    }
    
    @Test
    public void testTrim() {
        System.out.println("trim");
        assertEquals("foo", StringUtils.trim("  foo  "));
    }
    
    @Test
    public void testTrim_null() {
        System.out.println("trim");
        assertNull(StringUtils.trim(null));
    }
}
