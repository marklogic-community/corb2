/*
 * * Copyright (c) 2004-2022 MarkLogic Corporation
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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TransformTest {

    @Test
    public void testProcessResult()  {
        try {
            ResultSequence seq = null;
            Transform instance = new Transform();
            String result = instance.processResult(seq);
            assertEquals(Transform.TRUE, result);
        } catch (CorbException ex) {
            Logger.getLogger(TransformTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCall()  {
        try {
            Transform instance = new Transform();
            String[] result = instance.call();
            assertNotNull(result);
            assertTrue(result.length == 0);
        } catch (Exception ex) {
            Logger.getLogger(TransformTest.class.getName()).log(Level.SEVERE, null, ex);
            fail();
        }
    }

}
