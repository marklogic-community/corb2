/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class ModuleExecutorIT {

    private static final Logger LOG = Logger.getLogger(ModuleExecutorIT.class.getName());

    private void clearSystemProperties() {
		TestUtils.clearSystemProperties();
	    System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "0");
	    System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
	}

    @Test
    void testRunMain() {
        clearSystemProperties();
        String[] args = {};
        String exportFileName = "testRunMain.txt";
        System.setProperty(Options.XCC_CONNECTION_URI, ModuleExecutorTest.XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, "INLINE-JAVASCRIPT|var uri = '/a/b/c'; uri;");
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        File report = new File(exportFileName);
        report.deleteOnExit();

        int exitCode = ModuleExecutor.run(args);
        assertEquals(0, exitCode);
    }

    @Test
    void testRunInline() {
        clearSystemProperties();
        String[] args = {};
        String exportFileName = "testRunInline.txt";
        System.setProperty(Options.XCC_CONNECTION_URI, ModuleExecutorTest.XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, "INLINE-JAVASCRIPT|var uri = '/d/e/f'; uri;");
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        ModuleExecutor executor = new ModuleExecutor();
        try {
            executor.init(args);
            executor.run();
            String reportPath = executor.getProperty(Options.EXPORT_FILE_NAME);
            File report = new File(reportPath);
            report.deleteOnExit();
            boolean fileExists = report.exists();
            assertTrue(fileExists);
            String result = TestUtils.readFile(report);
            assertEquals("/d/e/f\n", result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

}
