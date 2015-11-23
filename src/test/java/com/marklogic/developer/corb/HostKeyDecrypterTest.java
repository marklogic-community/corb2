/*
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author mhansen
 */
public class HostKeyDecrypterTest {
    
    public HostKeyDecrypterTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
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
    @Test
    public void testInit_decrypter() throws Exception {
        System.out.println("init_decrypter");
        HostKeyDecrypter instance = new HostKeyDecrypter();
        instance.init_decrypter();
    }

    /**
     * Test of xor method, of class HostKeyDecrypter.
     */
    @Test
    public void testXor() {
        System.out.println("xor");
        byte[] byteOne =   {1,0,1,0,1};
        byte[] byteTwo =   {1,1,0,0};
        byte[] expResult = {0,1,1,0,1};
        byte[] result = HostKeyDecrypter.xor(byteOne, byteTwo);
        assertTrue(Arrays.equals(expResult,result));
    }

    /**
     * Test of getSHA256Hash method, of class HostKeyDecrypter.
     */
    @Test
    public void testGetSHA256Hash() throws Exception {
        System.out.println("getSHA256Hash");
        byte[] expected = {-75, -44, 4, 92, 63, 70, 111, -87, 31, -30, -52, 106, -66, 121, 35, 42, 26, 87, -51, -15, 4, -9, -94, 110, 113, 110, 10, 30, 39, -119, -33, 120};
        byte[] input = {'A','B','C'};
        byte[] result = HostKeyDecrypter.getSHA256Hash(input);
        System.out.println(Arrays.toString(result));
        assertTrue(Arrays.equals(expected, result));
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
    public void testMain_usage_unrecognizedMethod() throws Exception {
        System.out.println("main");
        String[] args = {"foo"};
        HostKeyDecrypter.main(args);
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
