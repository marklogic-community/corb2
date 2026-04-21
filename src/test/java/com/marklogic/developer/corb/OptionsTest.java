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

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Properties;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class OptionsTest {

    /**
     * Ensure that each Option has a @Usage annotation, used to generate commandline usage message
     */
    @Test
    void testUsage() {
        for (java.lang.reflect.Field field : Options.class.getDeclaredFields()) {
            //Verify that all of the public String constants have usage annotations
            if (String.class.equals(field.getType()) && (field.getModifiers() & (Modifier.PROTECTED | Modifier.PRIVATE)) == 0) {
                Usage usage = field.getAnnotation(Usage.class);
                assertNotNull(usage);
            }
        }
    }

    @Test
    void testStaticFields() {
        for (java.lang.reflect.Field field : Options.class.getDeclaredFields()) {
            if (String.class.equals(field.getType())) {
                assertTrue(Modifier.isStatic(field.getModifiers()));
            }
        }
    }

    @Test
    void testFindOption() {
        String key = "foo";
        String value = "value";
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertEquals(value, Options.findOption(properties, "foo"));
    }

    @Test
    void testFindOptionWithEmptyStringValue() {
        String key = "foo";
        String value = "";
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertEquals(value, Options.findOption(properties, "foo"));
    }

    @Test
    void testFindOptionMissing() {
        Properties properties = new Properties();
        assertNull(Options.findOption(properties, "foo"));
    }

    @Test
    void testFindOptionNullKey() {
        Properties properties = new Properties();
        assertNull(Options.findOption(properties, null));
    }
}
