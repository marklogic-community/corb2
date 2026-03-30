/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingJsonPathTest {

    @Test
    void matchesDefaultImmediateChildPath() throws Exception {
        StreamingJsonPath path = new StreamingJsonPath();
        assertTrue(path.matches("/items"));
        assertTrue(path.matches("/*"));
        assertFalse(path.matches("/"));
        assertFalse(path.matches("/items/*"));
    }

    @Test
    void matchesExplicitWildcardPath() throws Exception {
        StreamingJsonPath path = new StreamingJsonPath("/items/*/uri");
        assertTrue(path.matches("/items/*/uri"));
        assertFalse(path.matches("/items/uri"));
    }

    @Test
    void matchesDescendantPath() throws Exception {
        StreamingJsonPath path = new StreamingJsonPath("//uri");
        assertTrue(path.matches("/items/*/uri"));
        assertTrue(path.matches("/groups/nested/*/uri"));
        assertFalse(path.matches("/items/*/type"));
    }

    @Test
    void relativeExpressionBehavesAsDescendant() throws Exception {
        StreamingJsonPath path = new StreamingJsonPath("uri");
        assertTrue(path.matches("/items/*/uri"));
    }

    @Test
    void rejectsUnsupportedBracketSyntax() {
        assertThrows(CorbException.class, () -> new StreamingJsonPath("$.items[*]"));
        assertThrows(CorbException.class, () -> new StreamingJsonPath("/items[0]"));
    }
}
