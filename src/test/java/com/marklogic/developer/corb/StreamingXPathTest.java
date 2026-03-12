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
package com.marklogic.developer.corb;

import static java.util.logging.Level.SEVERE;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class StreamingXPathTest {

    private static StreamingXPath xpathInstance;
    private static final Logger LOG = Logger.getLogger(StreamingXPathTest.class.getName());

    @BeforeEach
    void setUp() throws CorbException {
        xpathInstance = new StreamingXPath();
    }

    private void testNormalizeAxes(String xpath, String expectedResult) {
        String pattern = xpathInstance.normalizeAxes(xpath);
        assertEquals(expectedResult, pattern);
    }

    @Test
    void testNormalizeAxesChild() {
        testNormalizeAxes("/*/child::foo", "/*/foo");
    }

    @Test
    void testNormalizeAxesDescendant() {
        testNormalizeAxes("/*/descendant::foo", "/*//foo");
    }

    @Test
    void testNormalizeAxesSelf() {
        testNormalizeAxes("/*/self::foo", "/*|foo");
    }

    @Test
    void testNormalizeAxesElementNameMatchPattern() {
        testNormalizeAxes("a:baz", "//a:baz");
    }

    @Test
    void testValidateAxis() {
        try {
            xpathInstance.validateAxis("/*/*");
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testValidateAxisAncestor() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("/b/ancestor::a"));
    }

    @Test
    void testValidateAxisAncestorOrSelf() {
        assertThrows(CorbException.class, () ->  xpathInstance.validateAxis("/b/ancestor-or-self::a"));
    }

    @Test
    void testValidateAxisAttribute() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("/a/attribute::b"));
    }

    @Test
    void testValidateAxisDescendantOrSelf() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("/a/descendant-or-self::b"));
    }

    @Test
    void testValidateAxisFollowing() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("/a/following::a"));
    }

    @Test
    void testValidateAxisFollowingSibling() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("/a/following-sibling::b"));
    }

    @Test
    void testValidateAxisNamespace() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("/a/namespace::b"));
    }

    @Test
    void testValidateAxisParent() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("//b/parent::a"));
    }

    @Test
    void testValidateAxisParent2() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("//b/.."));
    }

    @Test
    void testValidateAxisPreceding() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("//b/preceding::a"));
    }

    @Test
    void testValidateAxisPrecedingSibling() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("//b/preceding-sibling::a"));
    }

    @Test
    void testValidateAxisSibling() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("//b/sibling::c"));
    }

    @Test
    void testValidateAxisSelf() {
        assertThrows(CorbException.class, () -> xpathInstance.validateAxis("//b/self::b"));
    }

    @Test
    void testMatches() {
        assertTrue(xpathInstance.matches("/a/b"));
        assertTrue(xpathInstance.matches("/a/c:b"));
    }

    @Test
    void testMatchesIsFalse() {
        assertFalse(xpathInstance.matches("/a"));
        assertFalse(xpathInstance.matches("/a/b/c"));
    }

    @Test
    void testGetRegexPathPattern() {
        assertEquals("^/[^/]+/[^/]+", xpathInstance.getRegexPathPattern());
    }

    @Test
    void testParseRegex() {
        testParseXPathToRegex("/*//foo:bar[position()=1]", "^/[^/]+/[^/]*/?bar");
    }

    @Test
    void testParseRegexElementAnyLevel() {
        testParseXPathToRegex("//foo:bar", "^/[^/]*/?bar");
    }

    @Test
    void testParseRegexWithNamespacePrefixes() {
        testParseXPathToRegex("/foo:bar/foo:baz", "^/bar/baz");
    }

    @Test
    void testParseRegexWithoutNamespacePrefixes() {
        testParseXPathToRegex("/foo/bar/baz", "^/foo/bar/baz");
    }

    @Test
    void testParseRegexWithElementWildcard() {
        testParseXPathToRegex("/*/bar/*", "^/[^/]+/bar/[^/]+");
    }

    @Test
    void testParseRegexRelativePath() {
        testParseXPathToRegex("foo:bar", "^/[^/]*/?bar");
    }

    @Test
    void testParseRegexWithPredicates() {
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
