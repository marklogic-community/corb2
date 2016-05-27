/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.io.IOException;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hanse, MarkLogic Corporation
 */
public class HostKeyDecrypterTest {

    @Before
    public void setUp() {
        clearSystemProperties();
    }

    @After
    public void tearDown() {
        clearSystemProperties();
    }

    /**
     * Test of init_decrypter method, of class HostKeyDecrypter.
     */
    //@Test
    public void testInit_decrypter() throws Exception {
        System.out.println("init_decrypter");
        HostKeyDecrypter instance = new HostKeyDecrypter();
        try {
            instance.init_decrypter();
        } catch (IOException ex) {
            //travis-ci throws an IOException
        }
    }

    /**
     * Test of xor method, of class HostKeyDecrypter.
     */
    @Test
    public void testXor() {
        System.out.println("xor");
        byte[] byteOne = {1, 0, 1, 0, 1};
        byte[] byteTwo = {1, 1, 0, 0};
        byte[] expResult = {0, 1, 1, 0, 1};
        byte[] result = HostKeyDecrypter.xor(byteOne, byteTwo);
        assertTrue(Arrays.equals(expResult, result));
    }

    /**
     * Test of getSHA256Hash method, of class HostKeyDecrypter.
     */
    @Test
    public void testGetSHA256Hash() throws Exception {
        System.out.println("getSHA256Hash");
        byte[] expected = {-75, -44, 4, 92, 63, 70, 111, -87, 31, -30, -52, 106, -66, 121, 35, 42, 26, 87, -51, -15, 4, -9, -94, 110, 113, 110, 10, 30, 39, -119, -33, 120};
        byte[] input = {'A', 'B', 'C'};
        byte[] result = HostKeyDecrypter.getSHA256Hash(input);
        System.out.println(Arrays.toString(result));
        assertTrue(Arrays.equals(expected, result));
    }

    @Test
    public void testGetOperatingSystemType() {
        System.out.println("getOperatingSystemType");
        assertEquals(HostKeyDecrypter.OSType.Mac, HostKeyDecrypter.getOperatingSystemType("Darwin"));
        assertEquals(HostKeyDecrypter.OSType.Windows, HostKeyDecrypter.getOperatingSystemType("Windows 95"));
        assertEquals(HostKeyDecrypter.OSType.Windows, HostKeyDecrypter.getOperatingSystemType("windows xp"));
        assertEquals(HostKeyDecrypter.OSType.Linux, HostKeyDecrypter.getOperatingSystemType("unix"));
        assertEquals(HostKeyDecrypter.OSType.Linux, HostKeyDecrypter.getOperatingSystemType("Red Had Linux"));
        assertEquals(HostKeyDecrypter.OSType.Linux, HostKeyDecrypter.getOperatingSystemType("AIX/OS2"));
        assertEquals(HostKeyDecrypter.OSType.Other, HostKeyDecrypter.getOperatingSystemType("ReactOS"));
        assertEquals(HostKeyDecrypter.OSType.Other, HostKeyDecrypter.getOperatingSystemType(null));
    }

    /**
     * Test of main method, of class HostKeyDecrypter.
     */
    @Test
    public void testMain_usage_nullArgs() throws Exception {
        System.out.println("main");
        String[] args = null;
        HostKeyDecrypter.main(args);
    }

    @Test
    public void testMain_usage_decryptWithoutValue() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt"};
        HostKeyDecrypter.main(args);
    }

    @Test
    public void testMain_usage_unrecognizedMethod() throws Exception {
        System.out.println("main");
        String[] args = {"foo"};
        HostKeyDecrypter.main(args);
    }
    
    @Test
    public void testDoDecrypt() {
        HostKeyDecrypter decrypter = new HostKeyDecrypter();
        String value = "bar";
        String result = decrypter.decrypt("foo", value);
        assertEquals(value, result);
    }

    /* Illegal key size thrown if JCE is not loaded
    
    @Test
    public void testMain() throws Exception {
        System.out.println("main");
        String[] args = {"encrypt", "foo"};
        HostKeyDecrypter.main(args);
    }
    
    @Test
    public void testMain_test() throws Exception {
        System.out.println("main");
        String[] args = {"test"};
        HostKeyDecrypter.main(args);
    }
     */
}
