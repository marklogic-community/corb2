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
package com.marklogic.developer.corb;

import com.marklogic.xcc.ResultSequence;
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
public class TransformTest {
    
    public TransformTest() {
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
     * Test of processResult method, of class Transform.
     */
    @Test
    public void testProcessResult() throws Exception {
        System.out.println("processResult");
        ResultSequence seq = null;
        Transform instance = new Transform();
        String result = instance.processResult(seq);
        assertEquals(Transform.TRUE, result);
    }

    /**
     * Test of call method, of class Transform.
     */
    @Test
    public void testCall() throws Exception {
        System.out.println("call");
        Transform instance = new Transform();
        String[] result = instance.call();
        assertNull(result);
    }
    
}
