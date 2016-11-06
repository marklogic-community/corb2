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
package com.marklogic.developer.corb;

import static java.util.logging.Level.SEVERE;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class StreamingXPathTest {

    private StreamingXPath xpathInstance;
    private static final Logger LOG = Logger.getLogger(StreamingXPathTest.class.getName());

    @Before
    public void setUp() throws CorbException {
        xpathInstance = new StreamingXPath();
    }

    public StreamingXPathTest() {
    }

    private void testNormalizeAxes(String xpath, String expectedResult) {
        String pattern = xpathInstance.normalizeAxes(xpath);
        assertEquals(expectedResult, pattern);
    }

    @Test
    public void testNormalizeAxesChild() {
        testNormalizeAxes("/*/child::foo", "/*/foo");
    }

    @Test
    public void testNormalizeAxesDescendant() {
        testNormalizeAxes("/*/descendant::foo", "/*//foo");
    }

    @Test
    public void testNormalizeAxesSelf() {
        testNormalizeAxes("/*/self::foo", "/*|foo");
    }

    @Test
    public void testNormalizeAxesElementNameMatchPattern() {
        testNormalizeAxes("a:baz", "//a:baz");
    }

    public void testValidateAxis() {
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisAncestor() throws CorbException {
        xpathInstance.validateAxis("/b/ancestor::a");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisAncestorOrSelf() throws CorbException {
        xpathInstance.validateAxis("/b/ancestor-or-self::a");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisAttribute() throws CorbException {
        xpathInstance.validateAxis("/a/attribute::b");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisDescendantOrSelf() throws CorbException {
        xpathInstance.validateAxis("/a/descendant-or-self::b");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisFollowing() throws CorbException {
        xpathInstance.validateAxis("/a/following::a");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisFollowingSibling() throws CorbException {
        xpathInstance.validateAxis("/a/following-sibling::b");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisNamespace() throws CorbException {
        xpathInstance.validateAxis("/a/namespace::b");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisParent() throws CorbException {
        xpathInstance.validateAxis("//b/parent::a");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisParent2() throws CorbException {
        xpathInstance.validateAxis("//b/..");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisPreceding() throws CorbException {
        xpathInstance.validateAxis("//b/preceding::a");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisPrecedingSibling() throws CorbException {
        xpathInstance.validateAxis("//b/preceding-sibling::a");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisSibling() throws CorbException {
        xpathInstance.validateAxis("//b/sibling::c");
    }

    @Test(expected = CorbException.class)
    public void testValidateAxisSelf() throws CorbException {
        xpathInstance.validateAxis("//b/self::b");
    }

    @Test
    public void testMatches() {
        assertTrue(xpathInstance.matches("/a/b"));
        assertTrue(xpathInstance.matches("/a/c:b"));
    }

    @Test
    public void testMatchesIsFalse() {
        assertFalse(xpathInstance.matches("/a"));
        assertFalse(xpathInstance.matches("/a/b/c"));
    }

    @Test
    public void testGetRegexPathPattern() {
        assertEquals("^/[^/]+/[^/]+", xpathInstance.getRegexPathPattern());
    }

    @Test
    public void testParseRegex() {
        testParseXPathToRegex("/*//foo:bar[position()=1]", "^/[^/]+/[^/]*/?bar");
    }

    @Test
    public void testParseRegexElementAnyLevel() {
        testParseXPathToRegex("//foo:bar", "^/[^/]*/?bar");
    }

    @Test
    public void testParseRegexWithNamespacePrefixes() {
        testParseXPathToRegex("/foo:bar/foo:baz", "^/bar/baz");
    }

    @Test
    public void testParseRegexWithoutNamespacePrefixes() {
        testParseXPathToRegex("/foo/bar/baz", "^/foo/bar/baz");
    }

    @Test
    public void testParseRegexWithElementWildcard() {
        testParseXPathToRegex("/*/bar/*", "^/[^/]+/bar/[^/]+");
    }

    @Test
    public void testParseRegexRelativePath() {
        testParseXPathToRegex("foo:bar", "^/[^/]*/?bar");
    }

    @Test
    public void testParseRegexWithPredicates() {
        testParseXPathToRegex("/foo:bar[@baz='1' and @bar='2']", "^/bar");
        testParseXPathToRegex("/foo:bar[@baz='1' and @bar='2']/baz", "^/bar/baz");
    }

    private void testParseXPathToRegex(String xpath, String expectedResult) {
        try {
            String pattern = xpathInstance.parseXPathToRegex(xpath);
            assertEquals(expectedResult, pattern);
        } catch (CorbException ex) {
            LOG.log(SEVERE, null, ex);
            fail();
        }
    }
}
