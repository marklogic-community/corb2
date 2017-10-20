/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
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
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ModuleExecutorIT {

    private static final Logger LOG = Logger.getLogger(ModuleExecutorIT.class.getName());
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    private void clearSystemProperties() {
		TestUtils.clearSystemProperties();
	    System.setProperty(Options.XCC_CONNECTION_RETRY_LIMIT, "0");
	    System.setProperty(Options.XCC_CONNECTION_RETRY_INTERVAL, "0");
	}
    
    @Test
    public void testRunMain() {
        clearSystemProperties();
        String[] args = {};
        String exportFileName = "testRunMain.txt";
        System.setProperty(Options.XCC_CONNECTION_URI, ModuleExecutorTest.XCC_CONNECTION_URI);
        System.setProperty(Options.PROCESS_MODULE, "INLINE-JAVASCRIPT|var uri = '/a/b/c'; uri;");
        System.setProperty(Options.EXPORT_FILE_NAME, exportFileName);
        File report = new File(exportFileName);
        report.deleteOnExit();
        exit.expectSystemExit();
        ModuleExecutor.main(args);       
    }

    @Test
    public void testRunInline() {
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
            
            boolean fileExists = report.exists();
            assertTrue(fileExists);
            String result = TestUtils.readFile(report);
            assertEquals("/d/e/f\n", result);
            report.delete();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

}
