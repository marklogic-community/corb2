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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class FileUrisZipLoaderTest {

    private static final Logger LOG = Logger.getLogger(FileUrisZipLoaderTest.class.getName());
    public static final String TEST_ZIP_FILE = "src/test/resources/loader.zip";
    public static final Path TEST_ZIP_FILE_PATH = Paths.get(TEST_ZIP_FILE);
    public static final String PDF_COMMENT = "Portable Document Format Entry";

    @BeforeAll
    static void setUp() {
        try {
            Files.deleteIfExists(TEST_ZIP_FILE_PATH);
            pack(FileUrisDirectoryLoaderTest.TEST_DIR, TEST_ZIP_FILE);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @AfterAll
    static void tearDown() throws IOException {
        Files.deleteIfExists(TEST_ZIP_FILE_PATH);
    }

    @Test
    void testOpen() {
        try (FileUrisZipLoader instance = getDefaultFileUrisZipLoader()) {
            instance.open();
            assertNotNull(instance.zipFile);
            List<String> nodes = readAllEntries(instance);
            assertEquals(FileUrisDirectoryLoaderTest.TEST_ZIP_FILE_COUNT, nodes.size());
            assertTrue(nodes.stream().anyMatch(output -> output.contains("Portable Document Format Entry")));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testOpenSkipsDirectoriesAndLoadsOnlyFiles() {
        try (FileUrisZipLoader instance = getDefaultFileUrisZipLoader()) {
            instance.open();
            assertTrue(instance.getTotalCount() > 0);
            List<String> entries = readAllEntries(instance);
            assertFalse(entries.stream().anyMatch(output -> output.contains("<directory")));
            assertEquals(instance.getTotalCount(), entries.size());
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testOpenNotZip() {
        try (FileUrisZipLoader instance = getDefaultFileUrisZipLoader()) {
            instance.properties.setProperty(Options.ZIP_FILE, TEST_ZIP_FILE + ".notZip");
            assertThrows(CorbException.class, instance::open);
        }
    }

    @Test
    void testGetMetadataWithFile() {
        try (FileUrisZipLoader loader = new FileUrisZipLoader()) {
            assertThrows(UnsupportedOperationException.class, () -> loader.getMetadata(new File(TEST_ZIP_FILE)));
        }
    }

    @Test
    void testGetMetadataForPDF() {
        String pdfFilename = "docs/simple document.pdf";
        try (FileUrisZipLoader loader = new FileUrisZipLoader()) {
            try (ZipFile zipFile = new ZipFile(TEST_ZIP_FILE)) {
                loader.zipFile = zipFile;
                Map<String, String> metadata = loader.getMetadata(zipFile.getEntry(pdfFilename.replace("/", File.separator)));
                assertEquals(PDF_COMMENT, metadata.get(FileUrisZipLoader.META_COMMENT));
                assertEquals(pdfFilename.replace("/", File.separator), metadata.get(FileUrisZipLoader.META_FILENAME));
                assertEquals(zipFile.getName(), metadata.get(FileUrisZipLoader.META_SOURCE));
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
        }
    }

    @Test
    void testGetMetadataWithoutCommentOmitsComment() {
        try (FileUrisZipLoader loader = new FileUrisZipLoader()) {
            try (ZipFile zipFile = new ZipFile(TEST_ZIP_FILE)) {
                loader.zipFile = zipFile;
                ZipEntry entry = zipFile.stream().filter(z -> !z.isDirectory() && !z.getName().endsWith(".pdf")).findFirst().orElse(null);
                assertNotNull(entry);
                Map<String, String> metadata = loader.getMetadata(entry);
                assertFalse(metadata.containsKey(FileUrisZipLoader.META_COMMENT));
                assertEquals(entry.getName(), metadata.get(FileUrisZipLoader.META_FILENAME));
                assertEquals(zipFile.getName(), metadata.get(FileUrisZipLoader.META_SOURCE));
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
                fail();
            }
        }
    }

    @Test
    void testGetMetadataWithoutZipFileOmitsSource() {
        try (FileUrisZipLoader loader = new FileUrisZipLoader()) {
            Map<String, String> metadata = loader.getMetadata(new ZipEntry("docs/simple document.pdf"));
            assertNull(metadata.get(FileUrisZipLoader.META_SOURCE));
            assertEquals("docs/simple document.pdf", metadata.get(FileUrisZipLoader.META_FILENAME));
            assertFalse(metadata.containsKey(FileUrisZipLoader.META_COMMENT));
        }
    }

    private List<String> readAllEntries(FileUrisZipLoader instance) throws CorbException {
        List<String> nodes = new ArrayList<>();
        while (instance.hasNext()) {
            nodes.add(instance.next());
        }
        return nodes;
    }

    public static FileUrisZipLoader getDefaultFileUrisZipLoader() {
        FileUrisZipLoader instance = new FileUrisZipLoader();
        TransformOptions options = new TransformOptions();
        Properties props = new Properties();
        props.setProperty(Options.URIS_LOADER, FileUrisZipLoader.class.getName());
        props.setProperty(Options.ZIP_FILE, TEST_ZIP_FILE);
        instance.properties = props;
        instance.options = options;
        return instance;
    }

    public static void pack(String sourceDirPath, String zipFilePath) throws IOException {
        Path p = Files.createFile(Paths.get(zipFilePath));
        try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(p))) {
            Path pp = Paths.get(sourceDirPath);

            Files.walk(pp)
                    .filter(path -> {
                        try {
                            return !(Files.isDirectory(path) || Files.isHidden(path));
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    })
                    .forEach(path -> {
                        ZipEntry zipEntry = new ZipEntry(pp.relativize(path).toString());
                        if (zipEntry.getName().endsWith(".pdf")) {
                            zipEntry.setComment(PDF_COMMENT);
                        }
                        try {
                            zs.putNextEntry(zipEntry);
                            zs.write(Files.readAllBytes(path));
                            zs.closeEntry();
                        } catch (IOException ex) {
                            LOG.log(Level.SEVERE, "Problem adding entry to zip file", ex);
                        }
                    });
        }
    }

}
