/*
  * * Copyright (c) 2004-2018 MarkLogic Corporation
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

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class NumberUtilsTest {

    @Test
    public void testToIntString() {
        int result = NumberUtils.toInt("6");
        assertEquals(6, result);
    }

    @Test
    public void testToIntStringInvalid() {
        int result = NumberUtils.toInt("six");
        assertEquals(0, result);
    }

    @Test
    public void testToIntStringInt() {
        int result = NumberUtils.toInt("7", -1);
        assertEquals(7, result);
    }

    @Test
    public void testToIntStringIntInvalid() {
        int result = NumberUtils.toInt("seven", -1);
        assertEquals(-1, result);
    }
}
