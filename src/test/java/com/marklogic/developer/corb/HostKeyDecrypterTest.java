/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
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

import com.marklogic.developer.TestHandler;
import com.marklogic.developer.corb.HostKeyDecrypter.OSType;

import static com.marklogic.developer.corb.TestUtils.assertEqualsNormalizeNewline;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class HostKeyDecrypterTest {

    private final TestHandler testLogger = new TestHandler();
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private PrintStream systemOut = System.out;
    private PrintStream systemErr = System.err;
    private static final String USAGE = HostKeyDecrypter.USAGE + '\n';
    private static final Logger LOG = Logger.getLogger(HostKeyDecrypterTest.class.getName());
    private static final Logger HOST_KEY_DECRYPTER_LOG = Logger.getLogger(HostKeyDecrypter.class.getName());
    private static final String METHOD_GET_SN = "getSN";

    @Before
    public void setUp() {
        clearSystemProperties();
        HOST_KEY_DECRYPTER_LOG.addHandler(testLogger);
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void tearDown() {
        clearSystemProperties();
        System.setOut(systemOut);
        System.setErr(systemErr);
    }

    @Test
    public void testInitDecrypter() {
        HostKeyDecrypter instance = new HostKeyDecrypter();
        try {
            instance.init_decrypter();
            List<LogRecord> records = testLogger.getLogRecords();
            int lastMessage = records.size() - 1;
            assertTrue("Initialized HostKeyDecrypter".equals(records.get(lastMessage).getMessage()));
        } catch (RuntimeException ex) {
            //CircleCI throws and can't execute
            LOG.log(Level.SEVERE, null, ex);
        } catch (IOException | ClassNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testXor() {
        byte[] byteOne = {1, 0, 1, 0, 1};
        byte[] byteTwo = {1, 1, 0, 0};
        byte[] expResult = {0, 1, 1, 0, 1};
        byte[] result = HostKeyDecrypter.xor(byteOne, byteTwo);
        assertTrue(Arrays.equals(expResult, result));
    }

    @Test
    public void testGetSHA256Hash() {
        byte[] expected = {-75, -44, 4, 92, 63, 70, 111, -87, 31, -30, -52, 106, -66, 121, 35, 42, 26, 87, -51, -15, 4, -9, -94, 110, 113, 110, 10, 30, 39, -119, -33, 120};
        byte[] input = {'A', 'B', 'C'};
        try {
            byte[] result = HostKeyDecrypter.getSHA256Hash(input);
            assertTrue(Arrays.equals(expected, result));
        } catch (NoSuchAlgorithmException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    @Test
    public void testOTHERGetSN() {
        byte[] expected = {45, 32, 67, 34, 67, 23, 21, 45, 7, 89, 3, 27, 39, 62, 15};
        assertTrue(Arrays.equals(expected, HostKeyDecrypter.OSType.OTHER.getSN()));
    }

    @Test
    public void testGetOperatingSystemType() {
        assertEquals(HostKeyDecrypter.OSType.MAC, HostKeyDecrypter.getOperatingSystemType("Darwin"));
        assertEquals(HostKeyDecrypter.OSType.WINDOWS, HostKeyDecrypter.getOperatingSystemType("Windows 95"));
        assertEquals(HostKeyDecrypter.OSType.WINDOWS, HostKeyDecrypter.getOperatingSystemType("windows xp"));
        assertEquals(HostKeyDecrypter.OSType.LINUX, HostKeyDecrypter.getOperatingSystemType("unix"));
        assertEquals(HostKeyDecrypter.OSType.LINUX, HostKeyDecrypter.getOperatingSystemType("Red Had Linux"));
        assertEquals(HostKeyDecrypter.OSType.LINUX, HostKeyDecrypter.getOperatingSystemType("AIX/OS2"));
        assertEquals(HostKeyDecrypter.OSType.OTHER, HostKeyDecrypter.getOperatingSystemType("ReactOS"));
        assertEquals(HostKeyDecrypter.OSType.OTHER, HostKeyDecrypter.getOperatingSystemType(null));
    }

    @Test
    public void testGetValueFollowingMarker() {
        try {
            Class<?> clazz = HostKeyDecrypter.OSType.class;
            Method method = clazz.getDeclaredMethod(METHOD_GET_SN, String.class, String.class, OSType.class);
            method.setAccessible(true);
            method.invoke(clazz, "fileDoesNotExistXYZ", "Serial Number", OSType.MAC);
            fail();
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            Logger.getLogger(HostKeyDecrypterTest.class.getName()).log(Level.SEVERE, null, ex);
            assertTrue(ex.getCause() instanceof RuntimeException);
        }
    }

    @Test
    public void testMainUsageNullArgs() {
        String[] args = null;
        try {
            HostKeyDecrypter.main(args);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        assertEqualsNormalizeNewline(USAGE, outContent.toString());
    }

    @Test
    public void testMainUsageEncryptWithoutValue() {
        String[] args = {"encrypt"};
        try {
            HostKeyDecrypter.main(args);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        assertEqualsNormalizeNewline(USAGE, outContent.toString());
    }

    @Test
    public void testMainUsageUnrecognizedMethod() {
        String[] args = {"foo"};
        try {
            HostKeyDecrypter.main(args);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        assertEqualsNormalizeNewline(USAGE, outContent.toString());
    }

    @Test
    public void testMainNullArgs() {
        String[] args = null;
        try {
            HostKeyDecrypter.main(args);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        assertEqualsNormalizeNewline(USAGE, outContent.toString());
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
        String[] args = {"encrypt", "foo"};
        HostKeyDecrypter.main(args);
    }

    @Test
    public void testMain_test() throws Exception {
        String[] args = {"test"};
        HostKeyDecrypter.main(args);
    }
     */
}
