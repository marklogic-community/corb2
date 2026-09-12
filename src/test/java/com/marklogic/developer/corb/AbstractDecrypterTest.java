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

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractDecrypterTest {

    private static final String FOUR_SPACES = "    ";
    private static final String VALUE = "val";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    private AbstractDecrypterImpl newDecrypter() {
        return new AbstractDecrypterImpl();
    }

    @Test
    void testInitNullProperties() {
        AbstractDecrypter instance = newDecrypter();
        try {
            instance.init(null);
        } catch (IOException | ClassNotFoundException ex) {
            fail(ex);
        }
        assertNotNull(instance.properties);
    }

    @Test
    void testInit() {
        Properties props = new Properties();
        AbstractDecrypter instance = newDecrypter();
        try {
            instance.init(props);
        } catch (IOException | ClassNotFoundException ex) {
            fail(ex);
        }
        assertSame(props, instance.properties);
    }

    @Test
    void testDecryptNotEncrypted() {
        String property = "unencryptedProp";
        AbstractDecrypter instance = newDecrypter();
        String result = instance.decrypt(property, VALUE);
        assertEquals(VALUE.toUpperCase(Locale.ENGLISH), result);
    }

    @Test
    void testDecryptEncrypted() {
        String property = "encryptedProp";
        String value = "ENC(" + VALUE + ')';
        AbstractDecrypter instance = newDecrypter();
        String result = instance.decrypt(property, value);
        assertEquals(VALUE.toUpperCase(Locale.ENGLISH), result);
    }

    @Test
    void testDecryptEncryptedStripsWrapperWithoutTrimmingInnerContent() {
        String property = "encryptedProp";
        String value = "ENC(  " + VALUE + "  )";
        AbstractDecrypter instance = newDecrypter();
        String result = instance.decrypt(property, value);
        assertEquals(("  " + VALUE + "  ").toUpperCase(Locale.ENGLISH), result);
    }

    @Test
    void testDecryptNullValue() {
        AbstractDecrypter instance = newDecrypter();
        assertThrows(NullPointerException.class, () -> instance.decrypt("key", null));
    }

    @Test
    void testDoDecrypt() {
        String property = "key";
        AbstractDecrypter instance = newDecrypter();
        String result = instance.doDecrypt(property, VALUE);
        assertEquals(VALUE.toUpperCase(Locale.ENGLISH), result);
    }

    @Test
    void testGetPropertyNullProperties() {
        String key = "testProperty";
        AbstractDecrypter instance = newDecrypter();
        assertNull(instance.getProperty(key));
    }

    @Test
    void testGetPropertyUsesSystemPropertyWhenPresent() {
        String key = "testGetSystemProperty";
        System.setProperty(key, "  system-value  ");
        AbstractDecrypter instance = newDecrypter();
        instance.properties = new Properties();
        instance.properties.setProperty(key, "property-value");

        assertEquals("system-value", instance.getProperty(key));
    }

    @Test
    void testGetPropertyFallsBackToPropertiesWhenSystemValueBlank() {
        String key = "testGetBlankProperty";
        System.setProperty(key, FOUR_SPACES);
        AbstractDecrypter instance = newDecrypter();
        instance.properties = new Properties();
        instance.properties.setProperty(key, "  property-value  ");

        assertEquals("property-value", instance.getProperty(key));
    }

    @Test
    void testGetPropertyReturnsEmptyStringForBlankPropertyValue() {
        String key = "testGetBlankPropertyValue";
        AbstractDecrypter instance = newDecrypter();
        instance.properties = new Properties();
        instance.properties.setProperty(key, "      ");

        assertEquals("", instance.getProperty(key));
    }

    @Test
    void testGetPropertyNoMatchingValues() {
        String key = "testMissingProperty";
        AbstractDecrypter instance = newDecrypter();
        instance.properties = new Properties();

        assertNull(instance.getProperty(key));
    }

    private static class AbstractDecrypterImpl extends AbstractDecrypter {

        @Override
        public void init_decrypter() {
            // required to satisfy the interface
        }

        @Override
        public String doDecrypt(String property, String value) {
            return value.toUpperCase(Locale.ENGLISH);
        }
    }
}
