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

import static com.marklogic.developer.corb.Options.METADATA;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.XML_FILE;
import static com.marklogic.developer.corb.Options.XML_SCHEMA;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisStreamingXMLLoaderTest {

    public static final String BUU_DIR = "src/test/resources/streamingXMLUrisLoader/";
    public static final String BUU_FILENAME = "EDI.ICF15T.D150217.T113100716.T";
    public static final String BUU_SCHEMA = "BenefitEnrollment.xsd";
    public static final String NOT_BUU_SCHEMA = "Not" + BUU_SCHEMA;
    public static final int BUU_CHILD_ELEMENTS = 6;
    public static final int BUU_CHILD_XML_NODES = 5;
    private static final Logger LOG = Logger.getLogger(FileUrisStreamingXMLLoaderTest.class.getName());

    @Test
    public void testOpenWithDefaultXPath() {
        testOpen(null, BUU_CHILD_ELEMENTS);
    }

    @Test
    public void testOpenWithDescendantXpath() {
        testOpen("//BenefitEnrollmentMaintenance", 5);
    }

    @Test
    public void testOpenSingleFileWithElementNameOnly() {
        testOpen("BenefitEnrollmentMaintenance", 5);
    }

    @Test
    public void testOpenWithElementWildcard() {
        testOpen("/*/BenefitEnrollmentMaintenance", 5);
    }

    @Test
    public void testOpenWithMultipleWildcardSteps() {
        testOpen("/*/*/*/MemberInformation", 15);
    }

    @Test
    public void testOpenWithNamespace() {
        testOpen("/*/buu:FileInformation", 1);
    }

    @Test
    public void testOpenWithUnknownNamespace() {
        testOpen("/*/xyz:FileInformation", 1);
    }

    @Test
    public void testOpenUnindentedDefaultXPath() {
        testOpen(getUnindentedFileUrisXMLLoader(), "/*/*", 7);
    }

    @Test
    public void testOpenUnindentedSelectFoo() {
        testOpen(getUnindentedFileUrisXMLLoader(), "/doc/foo", 4);
        testOpen(getUnindentedFileUrisXMLLoader(), "/*/foo", 4);
        testOpen(getUnindentedFileUrisXMLLoader(), "//foo", 4);
    }

    @Test (expected = CorbException.class)
    public void testOpenUnindentedSelectFollowingSibling() throws CorbException {
        try (FileUrisStreamingXMLLoader loader = getUnindentedFileUrisXMLLoader()) {
            loader.properties.setProperty(Options.XML_NODE, "/doc/foo/following-sibling::foo");
            loader.open();
            loader.getTotalCount();
        }
    }

    @Test
    public void testOpenUnindentedSelectWithNamespacePrefix() {
        testOpen(getUnindentedFileUrisXMLLoader(), "/doc/f:foo", 4);
    }

    @Test
    public void testOpenUnindentedSelectBazElements() {
        testOpen(getUnindentedFileUrisXMLLoader(), "/doc/bar/baz", 1);
        testOpen(getUnindentedFileUrisXMLLoader(), "/doc/*/baz", 1);
        testOpen(getUnindentedFileUrisXMLLoader(), "/*/bar/baz", 1);
        testOpen(getUnindentedFileUrisXMLLoader(), "/*/*/baz", 1);
        testOpen(getUnindentedFileUrisXMLLoader(), "/*//baz", 1);
        testOpen(getUnindentedFileUrisXMLLoader(), "//bar/baz", 1);
        testOpen(getUnindentedFileUrisXMLLoader(), "//*/baz", 1);
    }

    @Test(expected = CorbException.class)
    public void testOpenWithSchemaInvalidContent() throws CorbException {
        FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader();
        loader.properties.setProperty(XML_SCHEMA, BUU_DIR + NOT_BUU_SCHEMA);
        loader.open();
    }

    public void testOpen(String XPath, int expectedItems) {
        FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader();
        testOpen(loader, XPath, expectedItems);
    }

    public void testOpen(String XPath, String metaXPath, int expectedItems) {
        FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader();
        testOpen(loader, XPath, null, expectedItems);
    }

    public void testOpen(FileUrisStreamingXMLLoader loader, String XPath, int expectedItems) {
        testOpen(loader, XPath, null, expectedItems);
    }

    public void testOpen(FileUrisStreamingXMLLoader loader, String XPath, String metaXPath, int expectedItems) {

        if (XPath != null) {
            loader.properties.setProperty(Options.XML_NODE, XPath);
        }
        if (metaXPath != null) {
            loader.properties.setProperty(Options.XML_METADATA, XPath);
        }
        try {
            loader.open();
            assertEquals(expectedItems, loader.getTotalCount());
            assertTrue(loader.hasNext());
            for (int i = 0; i < expectedItems; i++) {
                assertNotNull(loader.next());
            }
            loader.close();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenDefault() {
        try (FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader()) {
            loader.open();
            assertEquals(BUU_CHILD_ELEMENTS, loader.getTotalCount());
            assertEquals(String.valueOf(BUU_CHILD_ELEMENTS), loader.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertTrue(loader.hasNext());
            for (int i = 0; i < BUU_CHILD_ELEMENTS; i++) {
                String content = loader.next();
                assertNotNull(content);
                assertTrue(content.contains(FileUrisStreamingXMLLoader.LOADER_DOC));
                assertTrue(content.contains(String.format("%s=\"false\"", FileUrisStreamingXMLLoader.BASE64_ENCODED)));
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithoutEnvelope() {
        try (FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader();) {
            loader.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(false));
            loader.open();
            assertEquals(BUU_CHILD_ELEMENTS, loader.getTotalCount());
            assertEquals(String.valueOf(BUU_CHILD_ELEMENTS), loader.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertTrue(loader.hasNext());
            for (int i = 0; i < BUU_CHILD_ELEMENTS; i++) {
                String content = loader.next();
                assertNotNull(content);
                assertFalse(content.contains(FileUrisStreamingXMLLoader.LOADER_DOC));
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithoutEnvelopeWithMetadata() {
        try (FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader()) {
            loader.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(false));
            loader.properties.setProperty(Options.XML_NODE, "/BenefitEnrollmentRequest/BenefitEnrollmentMaintenance");
            loader.properties.setProperty(Options.XML_METADATA, "/BenefitEnrollmentRequest/FileInformation");
            loader.open();
            assertEquals(BUU_CHILD_XML_NODES, loader.getTotalCount());
            assertEquals(String.valueOf(BUU_CHILD_XML_NODES), loader.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertTrue(loader.hasNext());
            for (int i = 0; i < BUU_CHILD_XML_NODES; i++) {
                String content = loader.next();
                assertNotNull(content);
                assertFalse(content.contains(FileUrisStreamingXMLLoader.LOADER_DOC));
            }

            assertNotNull(loader.customMetadata);
            String metadata = loader.properties.getProperty(PRE_BATCH_MODULE+'.'+METADATA);
            assertNotNull(metadata);
            assertFalse(metadata.contains(FileUrisXMLLoader.LOADER_DOC));
            assertTrue(metadata.contains("InterchangeReceiverID"));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithEnvelopeWithMetadata() {
        try (FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader()) {
            loader.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(true));
            loader.properties.setProperty(Options.XML_NODE, "/BenefitEnrollmentRequest/BenefitEnrollmentMaintenance");
            loader.properties.setProperty(Options.XML_METADATA, "/BenefitEnrollmentRequest/FileInformation");
            loader.properties.setProperty(Options.METADATA_TO_PROCESS_MODULE, Boolean.toString(true));
            loader.open();
            assertEquals(BUU_CHILD_XML_NODES, loader.getTotalCount());
            assertEquals(String.valueOf(BUU_CHILD_XML_NODES), loader.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertTrue(loader.hasNext());
            for (int i = 0; i < BUU_CHILD_XML_NODES; i++) {
                String content = loader.next();
                assertNotNull(content);
                assertTrue(content.contains(FileUrisStreamingXMLLoader.LOADER_DOC));
            }

            assertNotNull(loader.customMetadata);
            String metadata = loader.properties.getProperty(POST_BATCH_MODULE+'.'+METADATA);
            assertNotNull(metadata);
            metadata = loader.properties.getProperty(PROCESS_MODULE+'.'+METADATA);
            assertNotNull(metadata);
            assertTrue(metadata.contains(FileUrisXMLLoader.LOADER_DOC));
            assertTrue(metadata.contains("InterchangeReceiverID"));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithEnvelopeAndBase64Encoded() {
        try (FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader()) {
            loader.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(true));
            loader.properties.setProperty(Options.LOADER_BASE64_ENCODE, Boolean.toString(true));
            loader.open();
            assertEquals(BUU_CHILD_ELEMENTS, loader.getTotalCount());
            assertTrue(loader.hasNext());
            for (int i = 0; i < BUU_CHILD_ELEMENTS; i++) {
                String content = loader.next();
                assertNotNull(content);
                assertTrue(content.contains(FileUrisStreamingXMLLoader.LOADER_DOC));
                assertTrue(content.contains(String.format("%s=\"true\"", FileUrisStreamingXMLLoader.BASE64_ENCODED)));
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testOpenWithEnvelopeAndBase64EncodedWithMetadata() {
        try (FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader()) {
            loader.properties.setProperty(Options.LOADER_USE_ENVELOPE, Boolean.toString(true));
            loader.properties.setProperty(Options.LOADER_BASE64_ENCODE, Boolean.toString(true));
            loader.properties.setProperty(Options.XML_NODE, "/BenefitEnrollmentRequest/BenefitEnrollmentMaintenance");
            loader.properties.setProperty(Options.XML_METADATA, "/BenefitEnrollmentRequest/FileInformation");
            loader.open();
            assertEquals(BUU_CHILD_XML_NODES, loader.getTotalCount());
            assertEquals(String.valueOf(BUU_CHILD_XML_NODES), loader.getProperty("POST-BATCH-MODULE.URIS_TOTAL_COUNT"));
            assertTrue(loader.hasNext());
            for (int i = 0; i < BUU_CHILD_XML_NODES; i++) {
                String content = loader.next();
                assertNotNull(content);
                assertTrue(content.contains(FileUrisStreamingXMLLoader.LOADER_DOC));
                assertTrue(content.contains(String.format("%s=\"true\"", FileUrisStreamingXMLLoader.BASE64_ENCODED)));
            }

            assertNotNull(loader.customMetadata);
            String metadata = loader.properties.getProperty(PRE_BATCH_MODULE+'.'+METADATA);
            assertNotNull(metadata);
            assertTrue(metadata.contains(FileUrisXMLLoader.LOADER_DOC));
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCleanupWithoutTempDir() {
        FileUrisStreamingXMLLoader loader = new FileUrisStreamingXMLLoader();
        loader.cleanup();
    }

    @Test
    public void testCloseWhenNotOpen() {
        FileUrisStreamingXMLLoader loader = new FileUrisStreamingXMLLoader();
        loader.close();
    }

    @Test
    public void testGetTempDir() {
        Properties properties = new Properties();
        properties.setProperty(Options.TEMP_DIR, BUU_DIR);
        Path tempDir = null;
        try (FileUrisStreamingXMLLoader loader = new FileUrisStreamingXMLLoader()) {
            loader.properties = properties;
            tempDir = loader.getTempDir();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        assertEquals(0, tempDir.getParent().compareTo(Paths.get(BUU_DIR)));
    }

    @Test(expected = InvalidParameterException.class)
    public void testGetTempDirWhenFileSpecified() {
        Properties properties = new Properties();
        properties.setProperty(Options.TEMP_DIR, BUU_DIR + BUU_SCHEMA);
        try (FileUrisStreamingXMLLoader loader = new FileUrisStreamingXMLLoader()) {
            loader.properties = properties;
            loader.getTempDir();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
        fail();
    }

    public FileUrisStreamingXMLLoader getDefaultLargeFileUrisXMLLoader() {
        FileUrisStreamingXMLLoader loader = new FileUrisStreamingXMLLoader();
        loader.properties = new Properties();
        loader.properties.setProperty(XML_FILE, BUU_DIR + BUU_FILENAME);
        loader.properties.setProperty(XML_SCHEMA, BUU_DIR + BUU_SCHEMA);
        return loader;
    }

    public FileUrisStreamingXMLLoader getUnindentedFileUrisXMLLoader() {
        FileUrisStreamingXMLLoader loader = new FileUrisStreamingXMLLoader();
        loader.properties = new Properties();
        loader.properties.setProperty(XML_FILE, BUU_DIR + "unindented.xml");
        return loader;
    }
}
