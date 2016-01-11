/*
 * * Copyright (c) 2004-2015 MarkLogic Corporation
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractDecrypterTest {

    public AbstractDecrypterTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of init method, of class AbstractDecrypter.
     */
    @Test
    public void testInit_nullProperties() throws Exception {
        System.out.println("init");
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.init(null);
        assertNotNull(instance.properties);
    }

    @Test
    public void testInit() throws Exception {
        System.out.println("init");
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
        System.out.println("getConnectionURI");
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
        System.out.println("getConnectionURI");
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
        System.out.println("getConnectionURI");
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
        System.out.println("getConnectionURI");
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
        System.out.println("decrypt");
        String property = "prop";
        String value = "val";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String result = instance.decrypt(property, value);
        assertEquals("VAL", result);
    }

    @Test
    public void testDecrypt_encrypted() {
        System.out.println("decrypt");
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
        System.out.println("doDecrypt");
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
        System.out.println("getProperty");
        String key = "testProperty";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        String expResult = null;
        String result = instance.getProperty(key);
        assertEquals(expResult, result);
    }

    @Test
    public void testGetProperty() throws IOException, ClassNotFoundException {
        System.out.println("getProperty");
        String key = "testProperty";
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetProperty_blankSystemProperty() throws IOException, ClassNotFoundException {
        System.out.println("getProperty");
        String key = "testGetProperty";
        System.setProperty(key, "    ");
        AbstractDecrypter instance = new AbstractDecrypterImpl();
        instance.properties = new Properties();
        String result = instance.getProperty(key);
        System.clearProperty(key);
        assertNull(result);
    }

    @Test
    public void testGetProperty_blankPropertiesProperty() throws IOException, ClassNotFoundException {
        System.out.println("getProperty");
        String key = "testGetProperty";
        System.setProperty(key, "    ");
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
