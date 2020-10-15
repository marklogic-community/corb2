/*
  * * Copyright (c) 2004-2020 MarkLogic Corporation
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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class StringUtilsTest {

    private static final Logger LOG = Logger.getLogger(StringUtilsTest.class.getName());
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
    public void testAnyIsNull() {
        String missing = null;
        assertTrue(StringUtils.anyIsNull("a", missing, "c"));
    }

    @Test
    public void testAnyIsNullWithoutNull() {
        assertFalse(StringUtils.anyIsNull("a", "b"));
    }

    @Test
    public void testAnyIsNullWithSingleNullString() {
        String missing = null;
        assertTrue(StringUtils.anyIsNull(missing));
    }

    @Test
    public void testEncodeIfNecessary() {
        assertEquals("foo%2Bbar", StringUtils.urlEncodeIfNecessary("foo+bar"));
        assertEquals("foo+bar", StringUtils.urlEncodeIfNecessary("foo bar"));
        assertEquals("foo+%2Bbar", StringUtils.urlEncodeIfNecessary("foo +bar"));
    }

    @Test
    public void testStringToBooleanStringIsEmpty() {
        boolean result = StringUtils.stringToBoolean("");
        assertFalse(result);
    }

    @Test
    public void testGetPathExtension() {
        String result = StringUtils.getPathExtension("dirA/dirB/filename.csv");
        assertEquals("csv", result);
    }

    @Test
    public void testGetPathExtensionMultipleDotsInPath() {
        String result = StringUtils.getPathExtension("dir1/dir2/file.name.csv.txt");
        assertEquals("txt", result);
    }

    @Test
    public void testGetPathExtensionNoExtension() {
        String path = "dir/dir/filename";
        String result = StringUtils.getPathExtension(path);
        assertEquals(path, result);
    }

    @Test
    public void testJoinListString() {
        List<String> items = Arrays.asList("a", "b", "c");
        String result = StringUtils.join(items, DELIM);
        assertEquals(A_B_C, result);
    }

    @Test
    public void testJoinListStringIsNull() {
        List<String> items = null;
        String result = StringUtils.join(items, DELIM);
        assertEquals(null, result);
    }

    @Test
    public void testJoinEmptyList() {
        List<String> items = new ArrayList<>(0);
        String result = StringUtils.join(items, DELIM);
        assertEquals("", result);
    }

    @Test
    public void testJoinObjectArrString() {
        Object[] items = new Object[2];
        items[0] = 2;
        items[1] = FOO;
        String delim = "|";
        String result = StringUtils.join(items, delim);
        assertEquals("2|foo", result);
    }

    @Test(expected = NullPointerException.class)
    public void testJoinObjectArrStringIsNull() {
        Object[] items = null;
        StringUtils.join(items, DELIM);
    }

    @Test
    public void testJoinStringArrString() {
        String[] items = new String[]{"a", "b", "c"};
        String result = StringUtils.join(items, DELIM);
        assertEquals(A_B_C, result);
    }

    @Test(expected = NullPointerException.class)
    public void testJoinStringArrStringIsNull() {
        String[] items = null;
        StringUtils.join(items, DELIM);
    }

    @Test
    public void testStringToBooleanString() {
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
    public void testStringToBooleanStringisTrue() {
        assertTrue(StringUtils.stringToBoolean("true"));
        assertTrue(StringUtils.stringToBoolean("asdf"));
        assertTrue(StringUtils.stringToBoolean("123"));
        assertTrue(StringUtils.stringToBoolean("00"));
        assertTrue(StringUtils.stringToBoolean("Y"));
    }

    @Test
    public void testStringToBooleanStringBoolean() {
        assertFalse(StringUtils.stringToBoolean(null, false));
        assertTrue(StringUtils.stringToBoolean(null, true));
    }

    @Test
    public void testBuildModulePathClass() {
        String result = StringUtils.buildModulePath(String.class);
        assertEquals("/java/lang/String.xqy", result);
    }

    @Test
    public void testBuildModulePathPackageString() {
        Package modulePackage = this.getClass().getPackage();
        String result = StringUtils.buildModulePath(modulePackage, "Utilities");
        assertEquals("/com/marklogic/developer/corb/util/Utilities.xqy", result);
    }

    @Test
    public void testBuildModulePathPackageStringWithSuffix() {
        Package modulePackage = this.getClass().getPackage();
        String result = StringUtils.buildModulePath(modulePackage, UTILITIES_FILENAME);
        assertEquals("/com/marklogic/developer/corb/util/" + UTILITIES_FILENAME, result);
    }

    @Test
    public void testBuildModulePath() {
        String fooUtilities = "/foo/Utilities.xqy";
        assertEquals(ABSOLUTE_UTILITIES_FILE, StringUtils.buildModulePath(SLASH, ABSOLUTE_UTILITIES_FILE));
        assertEquals(ABSOLUTE_UTILITIES_FILE, StringUtils.buildModulePath(SLASH, UTILITIES_FILENAME));
        assertEquals(fooUtilities, StringUtils.buildModulePath("/foo", UTILITIES_FILENAME));
        assertEquals(fooUtilities, StringUtils.buildModulePath("/foo", ABSOLUTE_UTILITIES_FILE));
        assertEquals(fooUtilities, StringUtils.buildModulePath("/foo/", UTILITIES_FILENAME));
        assertEquals("/foo//", StringUtils.buildModulePath("/foo/", SLASH));
    }

    @Test
    public void testDumpHex() {
        try {
            String result = StringUtils.dumpHex("abcd", "UTF-8");
            assertEquals("61 62 63 64", result);
        } catch (UnsupportedEncodingException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testDumpHexNull() {
        try {
            StringUtils.dumpHex(null, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = UnsupportedEncodingException.class)
    public void testDumpHexUnsupportedEncoding() throws UnsupportedEncodingException {
        StringUtils.dumpHex(FOO, "does not exist");
        fail();
    }

    @Test
    public void testGetXccUri() {
        String user = "user";
        String host = "host";
        String password = "pass+word";
        String port = "8000";
        String db = "db name ";
        String uri = StringUtils.getXccUri("", user, password, host, port, db, "auto");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri(null, user, password, host, port, db, "auto");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri("", user, password, host, port, db, "always");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri("", user, password, host, port, db, "maybe");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri("", user, password, host, port, db, "never");
        assertEquals("xcc://user:pass+word@host:8000/db name ", uri);
        uri = StringUtils.getXccUri("xcc", user, password, host, port, db, "never");
        assertEquals("xcc://user:pass+word@host:8000/db name ", uri);
        uri = StringUtils.getXccUri("xccs", user, password, host, port, db, "never");
        assertEquals("xccs://user:pass+word@host:8000/db name ", uri);
        uri = StringUtils.getXccUri("xccs", user, password, host, port, "", "never");
        assertEquals("xccs://user:pass+word@host:8000", uri);
        uri = StringUtils.getXccUri("xccs", user, password, host, port, null, "never");
        assertEquals("xccs://user:pass+word@host:8000", uri);
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
    public void testTrimBlank() {
        assertEquals("", StringUtils.trim("    "));
    }

    @Test
    public void testTrimNull() {
        assertNull(StringUtils.trim(null));
    }

    @Test
    public void testTrimToEmpty() {
        assertEquals(FOO, StringUtils.trimToEmpty("  foo  "));
    }

    @Test
    public void testTrimToEmptyNull() {
        assertEquals("", StringUtils.trimToEmpty(null));
    }

    @Test
    public void testTrimToEmptyBlank() {
        assertEquals("", StringUtils.trimToEmpty("   "));
    }

    @Test
    public void testIsAdhoc() {
        assertTrue(StringUtils.isAdhoc("/myModule.xqy|ADHOC"));
        assertTrue(StringUtils.isAdhoc("/myModule.xqy|adhoc"));
    }

    @Test
    public void testIsAdhocDoesNotMatch() {
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
    public void testIsJavaScriptDoesNotMatch() {
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

    @Test
    public void testIsInlineModule() {
        String code = INLINE_JAVASCRIPT_CODE;
        assertTrue(StringUtils.isInlineModule(INLINE_JAVASCRIPT_PREFIX + code));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code));
        assertTrue(StringUtils.isInlineModule("INLINE-JavaScript|" + code));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code));
        assertTrue(StringUtils.isInlineModule(INLINE_JAVASCRIPT_PREFIX + code + ADHOC_SUFFIX));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code + ADHOC_SUFFIX));
    }

    @Test
    public void testIsInlineModuleFalse() {
        String code = INLINE_JAVASCRIPT_CODE;
        assertFalse(StringUtils.isInlineModule("INLINE-JAVASCRIPT" + code)); //missing the |
        assertFalse(StringUtils.isInlineModule("INLINE-RUBY|" + code)); //wrong language
    }

    @Test
    public void testInlineModuleLanguageJavaScript() {
        String value = INLINE_JAVASCRIPT_PREFIX + INLINE_JAVASCRIPT_CODE + ADHOC_SUFFIX;
        String result = StringUtils.inlineModuleLanguage(value);
        assertEquals("JAVASCRIPT", result);
    }

    @Test
    public void testInlineModuleLanguageXQuery() {
        String code = "for $i in (1 to 10) return $i";
        String value = INLINE_XQUERY_PREFIX + code;
        String result = StringUtils.inlineModuleLanguage(value);
        assertEquals("XQUERY", result);
    }

    @Test
    public void testInlineModuleLanguageNull() {
        String result = StringUtils.inlineModuleLanguage(null);
        assertEquals("", result);
    }

    @Test
    public void testInlineModuleCode() {
        String code = INLINE_JAVASCRIPT_CODE;
        String value = INLINE_JAVASCRIPT_PREFIX + code + ADHOC_SUFFIX;

        String result = StringUtils.getInlineModuleCode(value);
        assertEquals(code, result);
    }

    @Test
    public void testInlineModuleCodeBadLanguage() {
        String code = "for $i in (1 to 10) return $i";
        String value = "INLINE-JAVA|" + code + ADHOC_SUFFIX;

        String result = StringUtils.getInlineModuleCode(value);
        assertNotEquals(code, result);
    }

    @Test
    public void testInlineModuleCodeNull() {
        String result = StringUtils.getInlineModuleCode(null);
        assertEquals("", result);
    }

    @Test
    public void testParsePortRangesSingleNumber() {
        Set<Integer> result = StringUtils.parsePortRanges("80");
        assertEquals(1, result.size());
        assertEquals(80, result.toArray()[0]);
    }

    @Test
    public void testParsePortRanges() {
        Set<Integer> result = StringUtils.parsePortRanges("80,443, 8000-8002, 8003 -8005, 8006- 8008, 8010 - 8009,443 ");
        assertEquals(13, result.size());
        Integer[] ports = result.toArray(new Integer[result.size()]);
        assertEquals(443, ports[1].intValue());
        assertEquals(8010, ports[12].intValue());
    }

    @Test(expected = NumberFormatException.class)
    public void testParsePortRangesWithNegativeNumber() {
        StringUtils.parsePortRanges("-8002");
        fail();
    }

    @Test
    public void testParsePortRangesWithBlankValue() {
        Set<Integer> result = StringUtils.parsePortRanges("  ");
        assertEquals(0, result.size());
    }

    @Test(expected = NumberFormatException.class)
    public void testParsePortRangesNoNumbers() {
        StringUtils.parsePortRanges("1 to 5");
        fail();
    }

    @Test
    public void testUrlEncode() {
        assertEquals("a+b", StringUtils.urlEncode("a b"));
    }

    @Test
    public void testUrlDecode() {
        assertEquals("a b", StringUtils.urlDecode("a+b"));
    }

    @Test
    public void testIsUrlEncoded(){
        assertTrue(StringUtils.isUrlEncoded("this%20that"));
        assertFalse(StringUtils.isUrlEncoded("this that"));
        assertFalse(StringUtils.isUrlEncoded("my milk is 2%"));
    }
}
