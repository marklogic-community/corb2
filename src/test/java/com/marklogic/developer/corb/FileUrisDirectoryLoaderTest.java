/*
  * * Copyright (c) 2004-2017 MarkLogic Corporation
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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisDirectoryLoaderTest {

    private static final Logger LOG = Logger.getLogger(FileUrisDirectoryLoaderTest.class.getName());
    public static final String TEST_DIR = "src/test/resources/loader";
    public static final int TEST_ZIP_FILE_COUNT = 11;

    public FileUrisDirectoryLoaderTest() {
    }

    @Test
    public void testCountFiles() throws Exception {
        Path dir = Paths.get(TEST_DIR);
        FileUrisDirectoryLoader loader = new FileUrisDirectoryLoader();
        assertEquals(TEST_ZIP_FILE_COUNT, loader.fileCount(dir));
    }

    @Test
    public void testOpen() {
        Properties properties = new Properties();
        properties.setProperty(Options.LOADER_PATH, TEST_DIR);
        try (FileUrisDirectoryLoader loader = new FileUrisDirectoryLoader()) {
            loader.properties = properties;
            loader.open();
            assertEquals(TEST_ZIP_FILE_COUNT, loader.getTotalCount());
            while(loader.hasNext()) {
                assertNotNull(loader.next());
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testHasNextWhenIteratorIsNull() {
        try (FileUrisDirectoryLoader loader = new FileUrisDirectoryLoader()) {
            assertFalse(loader.hasNext());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCloseWhenNotOpen() {
        try (FileUrisDirectoryLoader loader = new FileUrisDirectoryLoader()) {
            loader.close();
        }
    }

}
