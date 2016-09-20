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
import java.io.File;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ModuleExecutorIT {
    
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void testRunMain() throws Exception {
        clearSystemProperties();
        String[] args = {};
        System.setProperty(Options.XCC_CONNECTION_URI, ModuleExecutorTest.XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, "INLINE-JAVASCRIPT|var uri = '/a/b/c'; uri;");
        System.setProperty(Options.EXPORT_FILE_NAME, ModuleExecutorTest.EXPORT_FILE_NAME);
        exit.expectSystemExit();
        ModuleExecutor.main(args);
        String result = TestUtils.readFile(new File(ModuleExecutorTest.EXPORT_FILE_NAME));
        assertEquals("/a/b/c\n", result);
    }

    @Test
    public void testRunInline() throws Exception {
        clearSystemProperties();
        String[] args = {};
        System.setProperty(Options.XCC_CONNECTION_URI, ModuleExecutorTest.XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, "INLINE-JAVASCRIPT|var uri = '/d/e/f'; uri;");
        System.setProperty(Options.EXPORT_FILE_NAME, ModuleExecutorTest.EXPORT_FILE_NAME);
        ModuleExecutor executor = new ModuleExecutor();
        executor.init(args);
        executor.run();
        String reportPath = executor.getProperty(Options.EXPORT_FILE_NAME);
        File report = new File(reportPath);
        boolean fileExists = report.exists();
        assertTrue(fileExists);
        String result = TestUtils.readFile(report);
        assertEquals("/d/e/f\n", result);
    }

}
