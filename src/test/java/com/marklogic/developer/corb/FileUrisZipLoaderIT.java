/*
  * * Copyright (c) 2004-2023 MarkLogic Corporation
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

import static com.marklogic.developer.corb.FileUrisZipLoaderTest.TEST_ZIP_FILE;
import static com.marklogic.developer.corb.FileUrisZipLoaderTest.TEST_ZIP_FILE_PATH;
import static com.marklogic.developer.corb.FileUrisZipLoaderTest.pack;
import static com.marklogic.developer.corb.ManagerIT.SLASH;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisZipLoaderIT {

    private static final Logger LOG = Logger.getLogger(FileUrisZipLoaderIT.class.getName());
    private File tempDir;

    @Before
    public void setUp() throws IOException {
        tempDir = TestUtils.createTempDirectory();
        tempDir.deleteOnExit();
        try {
            Files.deleteIfExists(TEST_ZIP_FILE_PATH);
            pack(FileUrisDirectoryLoaderTest.TEST_DIR, TEST_ZIP_FILE);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testFileUrisZipLoader() {
        String exportFilename = "testFileUrisZipLoader.txt";
        Properties properties = getDefaultProperties();
        properties.setProperty(Options.EXPORT_FILE_NAME, exportFilename);
        Manager manager = new Manager();

        try {
            manager.init(properties);
            manager.run();

            String exportFilePath = tempDir.getPath() + SLASH + exportFilename;
            FileUrisDirectoryLoaderIT.verifyLoaderReport(exportFilePath);

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(Options.XCC_CONNECTION_URI, ManagerTest.XCC_CONNECTION_URI);
        properties.setProperty(Options.EXPORT_FILE_DIR, tempDir.getPath());
        properties.setProperty(Options.ZIP_FILE, TEST_ZIP_FILE);
        //properties.setProperty(Options.FILE_LOADER_PATH, TEST_ZIP_FILE); //TODO: try ZIP_FILE, then try FILE_LOADER_PATH
        properties.setProperty(Options.PROCESS_TASK, ManagerTest.PROCESS_TASK);
        properties.setProperty(Options.THREAD_COUNT, Integer.toString(8));
        properties.setProperty(Options.URIS_LOADER, FileUrisZipLoader.class.getName());
        properties.setProperty(Options.PROCESS_MODULE, FileUrisDirectoryLoaderIT.LOADER_PROCESS_MODULE);
        return properties;
    }
}
