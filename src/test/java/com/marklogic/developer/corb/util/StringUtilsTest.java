/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.*;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class StringUtilsTest {

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
    void testAnyIsNull() {
        String missing = null;
        assertTrue(StringUtils.anyIsNull("a", missing, "c"));
    }

    @Test
    void testAnyIsNullWithoutNull() {
        assertFalse(StringUtils.anyIsNull("a", "b"));
    }

    @Test
    void testAnyIsNullWithSingleNullString() {
        String missing = null;
        assertTrue(StringUtils.anyIsNull(missing));
    }

    @Test
    void testEncodeIfNecessary() {
        assertEquals("foo%2Bbar", StringUtils.urlEncodeIfNecessary("foo+bar"));
        assertEquals("foo+bar", StringUtils.urlEncodeIfNecessary("foo bar"));
        assertEquals("foo+%2Bbar", StringUtils.urlEncodeIfNecessary("foo +bar"));
    }

    @Test
    void testStringToBooleanStringIsEmpty() {
        boolean result = StringUtils.stringToBoolean("");
        assertFalse(result);
    }

    @Test
    void testGetPathExtension() {
        String result = StringUtils.getPathExtension("dirA/dirB/filename.csv");
        assertEquals("csv", result);
    }

    @Test
    void testGetPathExtensionMultipleDotsInPath() {
        String result = StringUtils.getPathExtension("dir1/dir2/file.name.csv.txt");
        assertEquals("txt", result);
    }

    @Test
    void testGetPathExtensionNoExtension() {
        String path = "dir/dir/filename";
        String result = StringUtils.getPathExtension(path);
        assertEquals(path, result);
    }

    @Test
    void testJoinListString() {
        List<String> items = Arrays.asList("a", "b", "c");
        String result = StringUtils.join(items, DELIM);
        assertEquals(A_B_C, result);
    }

    @Test
    void testJoinListStringIsNull() {
        List<String> items = null;
        String result = StringUtils.join(items, DELIM);
        assertNull(result);
    }

    @Test
    void testJoinEmptyList() {
        List<String> items = new ArrayList<>(0);
        String result = StringUtils.join(items, DELIM);
        assertEquals("", result);
    }

    @Test
    void testJoinObjectArrString() {
        Object[] items = new Object[2];
        items[0] = 2;
        items[1] = FOO;
        String delim = "|";
        String result = StringUtils.join(items, delim);
        assertEquals("2|foo", result);
    }

    @Test
    void testJoinObjectArrStringIsNull() {
        Object[] items = null;
        assertThrows(NullPointerException.class, () -> StringUtils.join(items, DELIM));
    }

    @Test
    void testJoinStringArrString() {
        String[] items = new String[]{"a", "b", "c"};
        String result = StringUtils.join(items, DELIM);
        assertEquals(A_B_C, result);
    }

    @Test
    void testJoinStringArrStringIsNull() {
        String[] items = null;
        assertThrows(NullPointerException.class, () -> StringUtils.join(items, DELIM));
    }

    @Test
    void testStringToBooleanString() {
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
    void testStringToBooleanStringisTrue() {
        assertTrue(StringUtils.stringToBoolean("true"));
        assertTrue(StringUtils.stringToBoolean("asdf"));
        assertTrue(StringUtils.stringToBoolean("123"));
        assertTrue(StringUtils.stringToBoolean("00"));
        assertTrue(StringUtils.stringToBoolean("Y"));
    }

    @Test
    void testStringToBooleanStringBoolean() {
        assertFalse(StringUtils.stringToBoolean(null, false));
        assertTrue(StringUtils.stringToBoolean(null, true));
    }

    @Test
    void testBuildModulePathClass() {
        String result = StringUtils.buildModulePath(String.class);
        assertEquals("/java/lang/String.xqy", result);
    }

    @Test
    void testBuildModulePathPackageString() {
        Package modulePackage = this.getClass().getPackage();
        String result = StringUtils.buildModulePath(modulePackage, "Utilities");
        assertEquals("/com/marklogic/developer/corb/util/Utilities.xqy", result);
    }

    @Test
    void testBuildModulePathPackageStringWithSuffix() {
        Package modulePackage = this.getClass().getPackage();
        String result = StringUtils.buildModulePath(modulePackage, UTILITIES_FILENAME);
        assertEquals("/com/marklogic/developer/corb/util/" + UTILITIES_FILENAME, result);
    }

    @Test
    void testBuildModulePath() {
        String fooUtilities = "/foo/Utilities.xqy";
        assertEquals(ABSOLUTE_UTILITIES_FILE, StringUtils.buildModulePath(SLASH, ABSOLUTE_UTILITIES_FILE));
        assertEquals(ABSOLUTE_UTILITIES_FILE, StringUtils.buildModulePath(SLASH, UTILITIES_FILENAME));
        assertEquals(fooUtilities, StringUtils.buildModulePath("/foo", UTILITIES_FILENAME));
        assertEquals(fooUtilities, StringUtils.buildModulePath("/foo", ABSOLUTE_UTILITIES_FILE));
        assertEquals(fooUtilities, StringUtils.buildModulePath("/foo/", UTILITIES_FILENAME));
        assertEquals("/foo//", StringUtils.buildModulePath("/foo/", SLASH));
    }

    @Test
    void testDumpHex() {
        try {
            String result = StringUtils.dumpHex("abcd", "UTF-8");
            assertEquals("61 62 63 64", result);
        } catch (UnsupportedEncodingException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testDumpHexNull() {
        assertThrows(NullPointerException.class, () -> StringUtils.dumpHex(null, "UTF-8"));
    }

    @Test
    void testDumpHexUnsupportedEncoding() {
        assertThrows(UnsupportedEncodingException.class, () -> StringUtils.dumpHex(FOO, "does not exist"));
    }

    @Test
    void testEncodeForHtml() {
        String result = StringUtils.encodeForHtml("<div>\"Hello & welcome\"</div>");
        assertEquals("&lt;div&gt;&quot;Hello &amp; welcome&quot;&lt;/div&gt;", result);
    }

    @Test
    void testGetXccUri() {
        Map<String, String> uriParams = new HashMap<>();
        uriParams.put(XCC_USERNAME, "user");
        uriParams.put(XCC_PASSWORD, "pass+word");
        uriParams.put(XCC_HOSTNAME, "host");
        uriParams.put(XCC_PORT, "8000");
        uriParams.put(XCC_DBNAME, "db name ");
        uriParams.put(XCC_PROTOCOL, "");
        String uri = StringUtils.getXccUri(uriParams ,"auto");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri(uriParams, "always");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri(uriParams, "maybe");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);
        uri = StringUtils.getXccUri(uriParams, "never");
        assertEquals("xcc://user:pass+word@host:8000/db name ", uri);

        uriParams.put(XCC_PROTOCOL, null);
        uri = StringUtils.getXccUri(uriParams, "auto");
        assertEquals("xcc://user:pass%2Bword@host:8000/db+name+", uri);

        uriParams.put(XCC_PROTOCOL, "xcc");
        uri = StringUtils.getXccUri(uriParams, "never");
        assertEquals("xcc://user:pass+word@host:8000/db name ", uri);
        uriParams.put(XCC_PROTOCOL, "xccs");
        uri = StringUtils.getXccUri(uriParams, "never");
        assertEquals("xccs://user:pass+word@host:8000/db name ", uri);
        uriParams.put(XCC_DBNAME, "");
        uri = StringUtils.getXccUri(uriParams, "never");
        assertEquals("xccs://user:pass+word@host:8000", uri);
        uriParams.put(XCC_DBNAME, null);
        uri = StringUtils.getXccUri(uriParams, "never");
        assertEquals("xccs://user:pass+word@host:8000", uri);
    }
    @Test
    void testGetXccUriDeprecated() {
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
    void testIsBlank() {
        assertTrue(StringUtils.isBlank(""));
        assertTrue(StringUtils.isBlank("  "));
        assertTrue(StringUtils.isBlank(" \n"));
        assertTrue(StringUtils.isBlank(null));
    }

    @Test
    void testTrim() {
        assertEquals(FOO, StringUtils.trim("  foo  "));
    }

    @Test
    void testTrimBlank() {
        assertEquals("", StringUtils.trim("    "));
    }

    @Test
    void testTrimNull() {
        assertNull(StringUtils.trim(null));
    }

    @Test
    void testTrimToEmpty() {
        assertEquals(FOO, StringUtils.trimToEmpty("  foo  "));
    }

    @Test
    void testTrimToEmptyNull() {
        assertEquals("", StringUtils.trimToEmpty(null));
    }

    @Test
    void testTrimToEmptyBlank() {
        assertEquals("", StringUtils.trimToEmpty("   "));
    }

    @Test
    void testIsAdhoc() {
        assertTrue(StringUtils.isAdhoc("/myModule.xqy|ADHOC"));
        assertTrue(StringUtils.isAdhoc("/myModule.xqy|adhoc"));
    }

    @Test
    void testIsAdhocDoesNotMatch() {
        assertFalse(StringUtils.isAdhoc("/myModule.xqy"));
        assertFalse(StringUtils.isAdhoc("adhoc.xqy"));
        assertFalse(StringUtils.isAdhoc("/myModule.xqy|adhoc "));
    }

    @Test
    void testIsJavaScriptModule() {
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
    void testIsJavaScriptDoesNotMatch() {
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
    void testIsInlineModule() {
        String code = INLINE_JAVASCRIPT_CODE;
        assertTrue(StringUtils.isInlineModule(INLINE_JAVASCRIPT_PREFIX + code));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code));
        assertTrue(StringUtils.isInlineModule("INLINE-JavaScript|" + code));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code));
        assertTrue(StringUtils.isInlineModule(INLINE_JAVASCRIPT_PREFIX + code + ADHOC_SUFFIX));
        assertTrue(StringUtils.isInlineModule(INLINE_XQUERY_PREFIX + code + ADHOC_SUFFIX));

    }

    @Test
    void testIsInlineModuleFalse() {
        String code = INLINE_JAVASCRIPT_CODE;
        assertFalse(StringUtils.isInlineModule("INLINE-JAVASCRIPT" + code)); //missing the |
        assertFalse(StringUtils.isInlineModule("INLINE-RUBY|" + code)); //wrong language
    }

    @Test
    void testInlineModuleLanguageJavaScript() {
        String value = INLINE_JAVASCRIPT_PREFIX + INLINE_JAVASCRIPT_CODE + ADHOC_SUFFIX;
        String result = StringUtils.inlineModuleLanguage(value);
        assertEquals("JAVASCRIPT", result);
    }

    @Test
    void testInlineModuleLanguageXQuery() {
        String code = "for $i in (1 to 10) return $i";
        String value = INLINE_XQUERY_PREFIX + code;
        String result = StringUtils.inlineModuleLanguage(value);
        assertEquals("XQUERY", result);
    }

    @Test
    void testInlineModuleLanguageNull() {
        String result = StringUtils.inlineModuleLanguage(null);
        assertEquals("", result);
    }

    @Test
    void testInlineModuleLanguageNotSupportedLang() {
        String inline = "INLINE-PYTHON|1+1";
        String result = StringUtils.inlineModuleLanguage(inline);
        assertEquals("", result);
        result = StringUtils.getInlineModuleCode("INLINE-PYTHON|1+1");
        assertEquals("", result);
    }

    @Test
    void testInlineModuleCode() {
        String code = INLINE_JAVASCRIPT_CODE;
        String value = INLINE_JAVASCRIPT_PREFIX + code + ADHOC_SUFFIX;

        String result = StringUtils.getInlineModuleCode(value);
        assertEquals(code, result);
    }

    @Test
    void testInlineModuleCodeBadLanguage() {
        String code = "for $i in (1 to 10) return $i";
        String value = "INLINE-JAVA|" + code + ADHOC_SUFFIX;

        String result = StringUtils.getInlineModuleCode(value);
        assertNotEquals(code, result);
    }

    @Test
    void testInlineModuleCodeNull() {
        String result = StringUtils.getInlineModuleCode(null);
        assertEquals("", result);
    }

    @Test
    void testParsePortRangesSingleNumber() {
        Set<Integer> result = StringUtils.parsePortRanges("80");
        assertEquals(1, result.size());
        assertEquals(80, result.toArray()[0]);
    }

    @Test
    void testParsePortRangesIncompleteRange() {
        Set<Integer> result = StringUtils.parsePortRanges("80-");
        assertEquals(0, result.size());
    }

    @Test
    void testParsePortRanges() {
        Set<Integer> result = StringUtils.parsePortRanges("80,443, 8000-8002, 8003 -8005, 8006- 8008, 8010 - 8009,443 ");
        assertEquals(13, result.size());
        Integer[] ports = result.toArray(new Integer[result.size()]);
        assertEquals(443, ports[1].intValue());
        assertEquals(8010, ports[12].intValue());
    }

    @Test
    void testParsePortRangesWithNegativeNumber() {
        assertThrows(NumberFormatException.class, () -> StringUtils.parsePortRanges("-8002"));
    }

    @Test
    void testParsePortRangesWithBlankValue() {
        Set<Integer> result = StringUtils.parsePortRanges("  ");
        assertEquals(0, result.size());
    }

    @Test
    void testParsePortRangesNoNumbers() {
        assertThrows(NumberFormatException.class, () -> StringUtils.parsePortRanges("1 to 5"));
    }

    @Test
    void testUrlEncode() {
        assertEquals("a+b", StringUtils.urlEncode("a b"));
    }

    @Test
    void testUrlDecode() {
        assertEquals("a b", StringUtils.urlDecode("a+b"));
    }

    @Test
    void testIsUrlEncoded(){
        assertTrue(StringUtils.isUrlEncoded("this%20that"));
        assertFalse(StringUtils.isUrlEncoded("this that"));
        assertFalse(StringUtils.isUrlEncoded("my milk is 2%"));
    }

    @Test
    void testRoundTripByteArrayToHexStringAndHexStringToByteArray() {
        String input = "hello world";
        String hex = StringUtils.byteArrayToHexString(input.getBytes(StandardCharsets.UTF_8));
        byte[] bytes = StringUtils.hexStringToByteArray(hex);
        String output = new String(bytes, StandardCharsets.UTF_8);
        assertEquals(input, output);
    }

    @Test
    void testByteArrayToHexString() {
        String input = "xyz";
        String hex = StringUtils.byteArrayToHexString(input.getBytes(StandardCharsets.UTF_8));
        assertEquals("78797a", hex);
    }

    @Test
    void testHexStringToByteArray() {
        byte[] bytes = StringUtils.hexStringToByteArray("70617373776F7264");
        byte[] expected = "password".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, bytes);
    }

}
