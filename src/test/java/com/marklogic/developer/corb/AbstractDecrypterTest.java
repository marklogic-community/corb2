/*
 * * Copyright (c) 2004-2021 MarkLogic Corporation
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

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractDecrypterTest {

    private static final String FOUR_SPACES = "    ";
    private static final String VALUE = "val";
    private static final String LOCALHOST = "localhost";
    private static final String USER = "user";
    private static final String PASS = "pass";
    private static final String PORT = "8003";
    private static final Logger LOG = Logger.getLogger(AbstractDecrypterTest.class.getName());

    @Test
    public void testInitNullProperties()  {
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        try {
            instance.init(null);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertNotNull(instance.properties);
    }

    @Test
    public void testInit() {
        Properties props = new Properties();
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        try {
            instance.init(props);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(props, instance.properties);
    }

    @Test
    public void testDecryptNotEncrypted() {
        String property = "unencryptedProp";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.decrypt(property, VALUE);
        assertEquals(VALUE.toUpperCase(), result);
    }

    @Test
    public void testDecryptEncrypted() {
        String property = "encryptedProp";
        String value = "ENC("+ VALUE + ')';
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.decrypt(property, value);
        assertEquals(VALUE.toUpperCase(), result);
    }

    @Test
    public void testDoDecrypt() {
        String property = "key";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.doDecrypt(property, VALUE);
        assertEquals(VALUE.toUpperCase(), result);
    }

    @Test
    public void testGetPropertyNullProperties() {
        String key = "testProperty";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetProperty() {
        String key = "testGetProperty";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetPropertyBlankSystemProperty() {
        String key = "testGetSystemProperty";
        System.setProperty(key, FOUR_SPACES);
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetPropertyBlankPropertiesProperty() {
        String key = "testGetBlankProperty";
        System.setProperty(key, FOUR_SPACES);
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        instance.properties.setProperty(key, "      ");
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertEquals("", result);
    }

    private static class AbstractDecrypterImpl extends AbstractDecrypter {

        @Override
        public void init_decrypter() throws IOException, ClassNotFoundException {
            //required to satisfy the interface
        }

        @Override
        public String doDecrypt(String property, String value) {
            return value.toUpperCase(Locale.ENGLISH);
        }
    }

}
