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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class OptionsTest {

    @BeforeEach
    void setUp() {
        clearOptionVariants("PROCESS-MODULE", "PROCESS_MODULE");
    }

    @AfterEach
    void tearDown() {
        clearOptionVariants("PROCESS-MODULE", "PROCESS_MODULE");
    }

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
        assertEquals("value", Options.findOption(propertiesWith("foo", "value"), "foo"));
    }

    @Test
    void testFindOptionWithEmptyStringValue() {
        assertEquals("", Options.findOption(propertiesWith("foo", ""), "foo"));
    }

    @Test
    void testFindOptionMissing() {
        assertNull(Options.findOption(new Properties(), "foo"));
    }

    @Test
    void testFindOptionNullKey() {
        assertNull(Options.findOption(new Properties(), null));
    }

    @Test
    void testFindOptionMatchesKebabAndSnakeCaseVariants() {
        Properties properties = propertiesWith("PROCESS_MODULE", "from-property-file");

        assertEquals("from-property-file", Options.findOption(properties, "PROCESS-MODULE"));
        assertEquals("from-property-file", Options.findOption(properties, "PROCESS_MODULE"));
    }

    @Test
    void testFindOptionPrefersSystemPropertyOverPropertiesFile() {
        Properties properties = propertiesWith("PROCESS_MODULE", "from-property-file");
        System.setProperty("PROCESS-MODULE", "from-system");

        assertEquals("from-system", Options.findOption(properties, "PROCESS-MODULE"));
    }

    private Properties propertiesWith(String key, String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return properties;
    }

    private void clearOptionVariants(String... optionNames) {
        for (String optionName : optionNames) {
            System.clearProperty(optionName);
            System.clearProperty(optionName.replace("-", "_"));
            System.clearProperty(optionName.replace("_", "-"));
        }
    }
}
