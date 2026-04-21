/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class FileUrisLoaderTest {

    private static final Logger LOG = Logger.getLogger(FileUrisLoaderTest.class.getName());
    private static final String URIS_FILE = "src/test/resources/uris-file.txt";

    @Test
    void testSetOptionsNull() {
        TransformOptions options = null;
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setOptions(options);
            assertNull(instance.options);
        }
    }

    @Test
    void testSetOptions() {
        TransformOptions options = mock(TransformOptions.class);
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setOptions(options);
            assertEquals(options, instance.options);
        }
    }

    @Test
    void testSetContentSourceNull() {
        ContentSourcePool csp = null;
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setContentSourcePool(csp);
            assertNull(instance.csp);
        }
    }

    @Test
    void testSetCollectionNull() {
        String collection = null;
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setCollection(collection);
            assertNull(instance.collection);
        }
    }

    @Test
    void testSetCollection() {
        String collection = "testSetCollection";
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setCollection(collection);
            assertEquals(collection, instance.collection);
        }
    }

    @Test
    void testSetPropertiesNull() {
        Properties properties = null;
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setProperties(properties);
            assertNull(instance.properties);
        }
    }

    @Test
    void testSetPropertiesProperties() {
        Properties properties = new Properties();
        try (FileUrisLoader instance = new FileUrisLoader()) {
            instance.setProperties(properties);
            assertEquals(properties, instance.properties);
        }
    }

    @Test
    void testOpen() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile(URIS_FILE);
            Properties props = new Properties();
            props.setProperty(Options.URIS_REPLACE_PATTERN, "object-id-2,test");
            instance.properties = props;
            instance.options = options;
            try {
                instance.open();
                assertNotNull(instance.bufferedReader);
                assertEquals("object-id-1", instance.next());
                assertEquals("test", instance.next());
            } catch (CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }

        }
    }

    @Test
    void testOpenInvalidReplacePattern() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile(URIS_FILE);
            Properties props = new Properties();
            props.setProperty(Options.URIS_REPLACE_PATTERN, "object-id-2,test,unevenPattern");
            instance.properties = props;
            instance.options = options;
            assertThrows(IllegalArgumentException.class, instance::open);
        }
    }

    @Test
    void testOpenFileDoesNotExist() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile("does/not/exist");
            instance.options = options;
            assertThrows(CorbException.class, instance::open);
        }
    }

    @Test
    void testGetBatchRef() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            assertNull(instance.getBatchRef());
        }
    }

    @Test
    void testGetTotalCountDefaultValue() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            assertEquals(0, instance.getTotalCount());
        }
    }

    @Test
    void testGetTotalCount() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile(URIS_FILE);
            instance.options = options;
            try {
                instance.open();
            } catch (CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
            assertEquals(8, instance.getTotalCount());
        }
    }

    @Test
    void testGetTotalCountAsModuleVariables() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile(URIS_FILE);
            instance.options = options;
            instance.setProperties(new Properties());
            try {
                instance.open();
            } catch (CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
            assertEquals(String.valueOf(8), instance.getProperty("PRE-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertEquals(String.valueOf(8), instance.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
        }
    }

    @Test
    void testHasNextThrowException() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            assertThrows(CorbException.class, instance::hasNext);
        }
    }

    @Test
    void testHasNext() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile(URIS_FILE);
            instance.options = options;
            try {
                instance.open();
                for (int i = 0; i < instance.getTotalCount(); i++) {
                    assertTrue(instance.hasNext());
                }
                //Verify that hasNext() does not advance the buffered reader to the next line
                assertTrue(instance.hasNext());
            } catch (CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
        }
    }

    @Test
    void testNext() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            options.setUrisFile(URIS_FILE);
            instance.options = options;
            try {
                instance.open();
                //Verify that hasNext() does not advance the buffered reader to the next line
                for (int i = 0; i < instance.getTotalCount(); i++) {
                    assertNotNull(instance.next());
                }
                assertFalse(instance.hasNext());
                assertNull(instance.next());
            } catch (CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

    @Test
    void testNextWithEmptyLine() {
        try (FileUrisLoader instance = new FileUrisLoader()) {
            TransformOptions options = new TransformOptions();
            try {
                File file = File.createTempFile("temp", ".txt");
                file.deleteOnExit();
                try (Writer writer = new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8)) {
                    writer.append("foo\n\nbar");
                }
                options.setUrisFile(file.getAbsolutePath());
                instance.options = options;
                instance.open();

                assertEquals("foo", instance.next());
                assertEquals("bar", instance.next());
                assertFalse(instance.hasNext());
                assertNull(instance.next());
            } catch (IOException | CorbException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
        }
    }

    @Test
    void testClose() {
        FileUrisLoader instance = new FileUrisLoader();
        instance.bufferedReader = mock(BufferedReader.class);
        instance.close();
        assertNull(instance.bufferedReader);
        instance.close();
    }

    @Test
    void testCleanup() {
        FileUrisLoader instance = new FileUrisLoader();
        instance.bufferedReader = mock(BufferedReader.class);
        instance.collection = "testCleanupCollection";
        instance.csp = mock(ContentSourcePool.class);
        instance.nextLine = "testCleanup";
        instance.options = new TransformOptions();
        instance.properties = new Properties();
        instance.replacements = new String[]{};
        instance.setTotalCount(100);
        instance.close();
        instance.cleanup();
        assertNull(instance.bufferedReader);
        assertNull(instance.collection);
        assertNull(instance.csp);
        assertNull(instance.options);
        assertNull(instance.replacements);
        assertNull(instance.nextLine);
        assertEquals(0, instance.getTotalCount());
    }

}
