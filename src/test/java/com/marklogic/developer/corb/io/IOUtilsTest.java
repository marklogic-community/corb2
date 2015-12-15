/*
  * * Copyright 2005-2015 MarkLogic Corporation
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
package com.marklogic.developer.corb.io;

import java.io.Closeable;
import java.io.IOException;
import java.io.StringReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class IOUtilsTest {

    public IOUtilsTest() {
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
     * Test of closeQuietly method, of class IOUtils.
     */
    @Test
    public void testCloseQuietly() throws IOException {
        System.out.println("closeQuietly");
        Closeable obj = new StringReader("foo");
        IOUtils.closeQuietly(obj);
        assertTrue(true);
    }

    @Test
    public void testCloseQuietly_throws() throws IOException {
        System.out.println("closeQuietly");
        Closeable closeable = new Closeable() {
            @Override
            public void close() throws IOException {
                throw new IOException("test IO");
            }
        };
        IOUtils.closeQuietly(closeable);
        assertTrue(true); //did not throw IOException
    }
    
        @Test
    public void testCloseQuietly_null() throws IOException {
        System.out.println("closeQuietly");
        Closeable closeable = null;
        IOUtils.closeQuietly(closeable);
        assertTrue(true); //did not throw IOException
    }
}
