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

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class NumberUtilsTest {

    @Test
    void testToIntString() {
        int result = NumberUtils.toInt("6");
        assertEquals(6, result);
    }

    @Test
    void testToIntStringInvalid() {
        int result = NumberUtils.toInt("six");
        assertEquals(0, result);
    }

    @Test
    void testToIntStringInt() {
        int result = NumberUtils.toInt("7", -1);
        assertEquals(7, result);
    }

    @Test
    void testToIntStringIntInvalid() {
        int result = NumberUtils.toInt("seven", -1);
        assertEquals(-1, result);
    }

    @Test
    public void testParseSize() {
        assertEquals(100L, NumberUtils.parseSize("100"));
        assertEquals(2048L, NumberUtils.parseSize("2 kb"));
        assertEquals(2048L, NumberUtils.parseSize("2KB"));
        assertEquals(2048L, NumberUtils.parseSize("2KiB"));
        assertEquals(1048576L, NumberUtils.parseSize("1M"));
        assertEquals(1048576L, NumberUtils.parseSize("1MB"));
        assertEquals(1048576L, NumberUtils.parseSize("1MiB"));
        assertEquals(1073741824L, NumberUtils.parseSize("1G"));
        assertEquals(1536, NumberUtils.parseSize("1.5kb"));
    }

    @Test
    void testParseSizeInvalidUnit() {
        assertThrows(NumberFormatException.class, () -> NumberUtils.parseSize("5x"));
    }

    @Test
    void testParseSizeInvalidValue() {
        assertThrows(NumberFormatException.class, () -> NumberUtils.parseSize("M"));
    }

}
