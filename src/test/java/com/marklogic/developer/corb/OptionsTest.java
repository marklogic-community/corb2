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

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class OptionsTest {

    /**
     * Ensure that each Option has a @Usage annotation, used to generate
     * commandline usage message
     */
    @Test
    public void testUsage() {
        for (java.lang.reflect.Field field : Options.class.getDeclaredFields()) {
            //Verify that all of the String constants
            if (String.class.isInstance(field.getType())) {
                Usage usage = field.getAnnotation(Usage.class);
                System.out.println(field.getName());
                assertNotNull(usage);
            }
        }
    }

    @Test
    public void testStaticFields() {
        for (java.lang.reflect.Field field : Options.class.getDeclaredFields()) {
            if (String.class.isInstance(field.getType())) {
                assertTrue(java.lang.reflect.Modifier.isStatic(field.getModifiers()));
            }
        }
    }
}
