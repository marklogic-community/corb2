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

import java.io.IOException;
import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractDecrypterTest {

    private static final String FOUR_SPACES = "    ";
    
    /**
     * Test of init method, of class AbstractDecrypter.
     */
    @Test
    public void testInit_nullProperties() throws Exception {
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.init(null);
        assertNotNull(instance.properties);
    }

    @Test
    public void testInit() throws Exception {
        Properties props = new Properties();
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.init(props);
        assertEquals(props, instance.properties);
    }

    /**
     * Test of getConnectionURI method, of class AbstractDecrypter.
     */
    @Test
    public void testGetConnectionURI() {
        String uri = "xcc://user:pass@localhost:8003/dbname";
        String username = "";
        String password = "";
        String host = "";
        String port = "";
        String dbname = "";
        AbstractDecrypter instance = new AbstractDecrypterImpl();

        String result = instance.getConnectionURI(uri, username, password, host, port, dbname);
        assertEquals(uri.toUpperCase(), result);
    }

    @Test
    public void testGetConnectionURI_constructUrl() {
        String uri = null;
        String username = "user";
        String password = "pass";
        String host = "localhost";
        String port = "8003";
        String dbname = "db";
        AbstractDecrypter instance = new AbstractDecrypterImpl();

        String result = instance.getConnectionURI(uri, username, password, host, port, dbname);
        assertEquals("xcc://USER:PASS@LOCALHOST:8003/DB", result);
    }

    @Test
    public void testGetConnectionURI_constructUrl_dbIsNull() {
        String uri = null;
        String username = "user";
        String password = "pass";
        String host = "localhost";
        String port = "8003";
        String dbname = null;
        AbstractDecrypter instance = new AbstractDecrypterImpl();

        String result = instance.getConnectionURI(uri, username, password, host, port, dbname);
        assertEquals("xcc://USER:PASS@LOCALHOST:8003", result);
    }

    @Test
    public void testGetConnectionURI_constructUrl_dbIsBlank() {
        String uri = null;
        String username = "user";
        String password = "pass";
        String host = "localhost";
        String port = "8003";
        String dbname = "";
        AbstractDecrypter instance = new AbstractDecrypterImpl();

        String result = instance.getConnectionURI(uri, username, password, host, port, dbname);
        assertEquals("xcc://USER:PASS@LOCALHOST:8003/", result);
    }
    /**
     * Test of decrypt method, of class AbstractDecrypter.
     */
    @Test
    public void testDecrypt_notEncrypted() {
        String property = "prop";
        String value = "val";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.decrypt(property, value);
        assertEquals("VAL", result);
    }

    @Test
    public void testDecrypt_encrypted() {
        String property = "prop";
        String value = "ENC(val)";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.decrypt(property, value);
        assertEquals("VAL", result);
    }

    /**
     * Test of doDecrypt method, of class AbstractDecrypter.
     */
    @Test
    public void testDoDecrypt() {
        String property = "key";
        String value = "val";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.doDecrypt(property, value);
        assertEquals(value.toUpperCase(), result);
    }

    /**
     * Test of getProperty method, of class AbstractDecrypter.
     */
    @Test
    public void testGetProperty_nullProperties() {
        String key = "testProperty";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetProperty() throws IOException, ClassNotFoundException {
        String key = "testProperty";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetProperty_blankSystemProperty() throws IOException, ClassNotFoundException {
        String key = "testGetProperty";
        System.setProperty(key, FOUR_SPACES);
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetProperty_blankPropertiesProperty() throws IOException, ClassNotFoundException {
        String key = "testGetProperty";
        System.setProperty(key, FOUR_SPACES);
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String expResult = "";
        instance.properties = new Properties();
        instance.properties.setProperty(key, "      ");
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertEquals(expResult, result);
    }

    private static class AbstractDecrypterImpl extends AbstractDecrypter {

        @Override
        public void init_decrypter() throws IOException, ClassNotFoundException {

        }

        @Override
        public String doDecrypt(String property, String value) {
            return value.toUpperCase();
        }
    }

}
