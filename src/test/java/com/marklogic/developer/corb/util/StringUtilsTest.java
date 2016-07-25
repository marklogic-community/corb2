/*
  * * Copyright (c) 2004-2016 MarkLogic Corporation
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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class StringUtilsTest {

    private static final String ADHOC_SUFFIX = "|ADHOC";
    private static final String INLINE_JAVASCRIPT_CODE = "var i = 0; return i;";
    private static final String UTILITIES_FILENAME = "Utilities.xqy";
    private static final String SLASH = "/";
    private static final String ABSOLUTE_UTILITIES_FILE = SLASH + UTILITIES_FILENAME;
    private static final String INLINE_JAVASCRIPT_PREFIX = "INLINE-JAVASCRIPT|";
    private static final String INLINE_XQUERY_PREFIX = "INLINE-XQUERY|";
    private static final String DELIM = ",";
    private static final String A_B_C = "a,b,c";
    private static final String FOO = "foo";
    
    @Test
    public void testStringToBoolean_String_empty() {
        boolean result = StringUtils.stringToBoolean("");
        assertFalse(result);
    }

    /**
     * Test of getPathExtension method, of class Utilities.
     */
    @Test
    public void testGetPathExtension() {
        String result = StringUtils.getPathExtension("dirA/dirB/filename.csv");
        assertEquals("csv", result);
    }

    @Test
    public void testGetPathExtension_multipleDotsInPath() {
        String result = StringUtils.getPathExtension("dir1/dir2/file.name.csv.txt");
        assertEquals("txt", result);
    }

    @Test
    public void testGetPathExtension_noExtension() {
        String path = "dir/dir/filename";
        String result = StringUtils.getPathExtension(path);
        assertEquals(path, result);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @Test
    public void testJoin_List_String() {
        List<String> items = Arrays.asList(new String[]{"a", "b", "c"});
        String result = StringUtils.join(items, DELIM);
        assertEquals(A_B_C, result);
    }

    @Test
    public void testJoin_List_StringIsNull() {
        List<String> items = null;
        String result = StringUtils.join(items, DELIM);
        assertEquals(null, result);
    }

    @Test
    public void testJoin_emptyList() {
        List<String> items = new ArrayList<String>();
        String result = StringUtils.join(items, DELIM);
        assertEquals("", result);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @Test
    public void testJoin_ObjectArr_String() {
        Object[] items = new Object[2];
        items[0] = 2;
        items[1] = FOO;
        String delim = "|";
        String result = StringUtils.join(items, delim);
        assertEquals("2|foo", result);
    }

    @Test(expected = NullPointerException.class)
    public void testJoin_ObjectArr_StringIsNull() {
        Object[] items = null;
        StringUtils.join(items, DELIM);
    }

    /**
     * Test of join method, of class Utilities.
     */
    @Test
    public void testJoin_StringArr_String() {
        String[] items = new String[]{"a", "b", "c"};
        String delim = DELIM;
        String result = StringUtils.join(items, delim);
        assertEquals(A_B_C, result);
    }

    @Test(expected = NullPointerException.class)
    public void testJoin_StringArr_StringIsNull() {
        String[] items = null;
        String delim = DELIM;
        StringUtils.join(items, delim);
    }

    /**
     * Test of stringToBoolean method, of class Utilities.
     */
    @Test
    public void testStringToBoolean_String() {
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

    @Test
    public void testStringToBoolean_String_true() {
        assertTrue(StringUtils.stringToBoolean("true"));
        assertTrue(StringUtils.stringToBoolean("asdf"));
        assertTrue(StringUtils.stringToBoolean("123"));
        assertTrue(StringUtils.stringToBoolean("00"));
        assertTrue(StringUtils.stringToBoolean("Y"));
    }

    /**
     * Test of stringToBoolean method, of class Utilities.
     */
    @Test
    public void testStringToBoolean_String_boolean() {
        assertFalse(StringUtils.stringToBoolean(null, false));
        assertTrue(StringUtils.stringToBoolean(null, true));
    }

    /**
     * Test of buildModulePath method, of class Utilities.
     */
    @Test
    public void testBuildModulePath_Class() {
        String result = StringUtils.buildModulePath(String.class);
        assertEquals("/java/lang/String.xqy", result);
    }

    /**
     * Test of buildModulePath method, of class Utilities.
     */
    @Test
    public void testBuildModulePath_Package_String() {
        Package modulePackage = this.getClass().getPackage();
        String result = StringUtils.buildModulePath(modulePackage, "Utilities");
        assertEquals("/com/marklogic/developer/corb/util/Utilities.xqy", result);
    }

    @Test
    public void testBuildModulePath_Package_String_withSuffix() {
        Package modulePackage = this.getClass().getPackage();
        String result = StringUtils.buildModulePath(modulePackage, UTILITIES_FILENAME);
        assertEquals("/com/marklogic/developer/corb/util/" + UTILITIES_FILENAME, result);
    }

    @Test
    public void testBuildModulePath() {
        assertEquals(ABSOLUTE_UTILITIES_FILE, StringUtils.buildModulePath(SLASH, ABSOLUTE_UTILITIES_FILE));
        assertEquals(ABSOLUTE_UTILITIES_FILE, StringUtils.buildModulePath(SLASH, UTILITIES_FILENAME));
        assertEquals("/foo/Utilities.xqy", StringUtils.buildModulePath("/foo", UTILITIES_FILENAME));
        assertEquals("/foo/Utilities.xqy", StringUtils.buildModulePath("/foo", ABSOLUTE_UTILITIES_FILE));
        assertEquals("/foo/Utilities.xqy", StringUtils.buildModulePath("/foo/", UTILITIES_FILENAME));
        assertEquals("/foo//", StringUtils.buildModulePath("/foo/", SLASH));
    }

    /**
     * Test of dumpHex method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testDumpHex() throws Exception {
        String result = StringUtils.dumpHex("abcd", "UTF-8");
        assertEquals("61 62 63 64", result);
    }

    @Test(expected = NullPointerException.class)
    public void testDumpHex_null() throws Exception {
        StringUtils.dumpHex(null, "UTF-8");
        fail();
    }

    @Test(expected = UnsupportedEncodingException.class)
    public void testDumpHex_unsupportedEncoding() throws Exception {
        StringUtils.dumpHex(FOO, "does not exist");
        fail();
    }

    @Test
    public void testIsBlank() {
        assertTrue(StringUtils.isBlank(""));
        assertTrue(StringUtils.isBlank("  "));
        assertTrue(StringUtils.isBlank(" \n"));
        assertTrue(StringUtils.isBlank(null));
    }

    @Test
    public void testTrim() {
        assertEquals(FOO, StringUtils.trim("  foo  "));
    }

    @Test
    public void testTrim_blank() {
        assertEquals("", StringUtils.trim("    "));
    }

    @Test
    public void testTrim_null() {
        assertNull(StringUtils.trim(null));
    }

    @Test
    public void testTrimToEmpty() {
        assertEquals(FOO, StringUtils.trimToEmpty("  foo  "));
    }

    @Test
    public void testTrimToEmpty_null() {
        assertEquals("", StringUtils.trimToEmpty(null));
    }

    @Test
    public void testTrimToEmpty_blank() {
        assertEquals("", StringUtils.trimToEmpty("   "));
    }

    @Test
    public void testIsAdhoc() {
        assertTrue(StringUtils.isAdhoc("/myModule.xqy|ADHOC"));
        assertTrue(StringUtils.isAdhoc("/myModule.xqy|adhoc"));
    }

    @Test
    public void testIsAdhoc_doesNotMatch() {
        assertFalse(StringUtils.isAdhoc("/myModule.xqy"));
        assertFalse(StringUtils.isAdhoc("adhoc.xqy"));
        assertFalse(StringUtils.isAdhoc("/myModule.xqy|adhoc "));
    }

    @Test
    public void testIsJavaScriptModule() {
        assertTrue(StringUtils.isJavaScriptModule("/myModule.js"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.sjs"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.JS"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.SJS"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.js|adhoc"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.JS|ADHOC"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.JS|ADHOC"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.sjs|adhoc"));
        assertTrue(StringUtils.isJavaScriptModule("/myModule.sjs|ADHOC"));
        assertTrue(StringUtils.isJavaScriptModule("INLINE-JAVASCRIPT|var foo = 1; return foo;"));
        assertTrue(StringUtils.isJavaScriptModule("INLINE-JAVASCRIPT|var foo = 1; return foo;|ADHOC"));
        assertTrue(StringUtils.isJavaScriptModule("INLINE-JavaScript|var foo = 1; return foo;"));
        assertTrue(StringUtils.isJavaScriptModule("inline-JavaScript|var foo = 1; return foo;"));
    }

    @Test
    public void testIsJavaScript_doesNotMatch() {
        assertFalse(StringUtils.isJavaScriptModule(null));
        assertFalse(StringUtils.isJavaScriptModule(""));
        assertFalse(StringUtils.isJavaScriptModule("/myModule.xqy"));
        assertFalse(StringUtils.isJavaScriptModule("adhoc.xqy|ADHOC"));
        assertFalse(StringUtils.isJavaScriptModule("/myModule.js.xqy"));
        assertFalse(StringUtils.isJavaScriptModule("/myModule.js.xqy|ADHOC"));
        assertFalse(StringUtils.isJavaScriptModule("/myModule.jsx"));
        assertFalse(StringUtils.isJavaScriptModule(INLINE_XQUERY_PREFIX + "for $i in (1 to 5) return $i"));
        assertFalse(StringUtils.isJavaScriptModule(INLINE_XQUERY_PREFIX + "var foo = 1; return foo;|ADHOC"));
    }

    /**
     * Test of isInlineModule method, of class StringUtils.
     */
    @Test
    public void testIsInlineModule() {
        String code = INLINE_JAVASCRIPT_CODE;
        assertTrue(StringUtils.isInlineModule(INLINE_JAVASCRIPT_PREFIX + code));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code));
        assertTrue(StringUtils.isInlineModule("INLINE-JavaScript|" + code));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code));
        assertTrue(StringUtils.isInlineModule(INLINE_JAVASCRIPT_PREFIX+ code + ADHOC_SUFFIX));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code + ADHOC_SUFFIX));
    }

    @Test
    public void testIsInlineModule_false() {
        String code = INLINE_JAVASCRIPT_CODE;
        assertFalse(StringUtils.isInlineModule("INLINE-JAVASCRIPT" + code)); //missing the |
        assertFalse(StringUtils.isInlineModule("INLINE-RUBY|" + code)); //wrong language
    }

    /**
     * Test of inlineModuleLanguage method, of class StringUtils.
     */
    @Test
    public void testInlineModuleLanguage_JAVASCRIPT() {
        String code = INLINE_JAVASCRIPT_CODE;
        String value = INLINE_JAVASCRIPT_PREFIX + code + ADHOC_SUFFIX;
        String result = StringUtils.inlineModuleLanguage(value);
        assertEquals("JAVASCRIPT", result);
    }

    @Test
    public void testInlineModuleLanguage_XQUERY() {
        String code = "for $i in (1 to 10) return $i";
        String value = INLINE_XQUERY_PREFIX + code;
        String result = StringUtils.inlineModuleLanguage(value);
        assertEquals("XQUERY", result);
    }

    @Test
    public void testInlineModuleLanguage_null() {
        String result = StringUtils.inlineModuleLanguage(null);
        assertEquals("", result);
    }

    /**
     * Test of inlineModuleCode method, of class StringUtils.
     */
    @Test
    public void testInlineModuleCode() {
        String code = INLINE_JAVASCRIPT_CODE;
        String value = INLINE_JAVASCRIPT_PREFIX + code + ADHOC_SUFFIX;

        String result = StringUtils.getInlineModuleCode(value);
        assertEquals(code, result);
    }

    @Test
    public void testInlineModuleCode_badLanguage() {
        String code = "for $i in (1 to 10) return $i";
        String value = "INLINE-JAVA|" + code + ADHOC_SUFFIX;

        String result = StringUtils.getInlineModuleCode(value);
        assertNotEquals(code, result);
    }

    @Test
    public void testInlineModuleCode_null() {

        String result = StringUtils.getInlineModuleCode(null);
        assertEquals("", result);
    }
}
