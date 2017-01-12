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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisZipLoaderTest {

    private static final Logger LOG = Logger.getLogger(FileUrisZipLoaderTest.class.getName());
    private static final String TEST_ZIP_FILE = "src/test/resources/loader.zip";
    private static final Path TEST_ZIP_FILE_PATH = Paths.get(TEST_ZIP_FILE);

    public FileUrisZipLoaderTest() {
    }

    @Before
    public void setUp() {
        try {
            Files.deleteIfExists(TEST_ZIP_FILE_PATH);
            pack("src/test/resources/loader", "src/test/resources/loader.zip");
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(TEST_ZIP_FILE_PATH);
    }

    @Test
    public void testOpen() throws Exception {
        List<String> nodes;
        try (FileUrisZipLoader instance = getDefaultFileUrisZipLoader()) {
            instance.open();
            assertNotNull(instance.zipFile);
            nodes = new ArrayList<>();
            while (instance.hasNext()) {
                String output = instance.next();
                if (output.contains(".pdf")) {
                    assertTrue(output.contains("Portable Document Format Entry"));
                }
                nodes.add(output);
            }
            assertEquals(11, nodes.size());
           

        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private FileUrisZipLoader getDefaultFileUrisZipLoader() {
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
                        if (zipEntry.getName().endsWith(".pdf")){
                            zipEntry.setComment("Portable Document Format Entry");
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
