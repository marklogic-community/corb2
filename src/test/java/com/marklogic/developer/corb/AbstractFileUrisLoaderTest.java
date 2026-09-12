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
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.LOADER_BASE64_ENCODE;
import static com.marklogic.developer.corb.Options.LOADER_PATH;
import static com.marklogic.developer.corb.Options.LOADER_USE_ENVELOPE;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

class AbstractFileUrisLoaderTest {

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    private AbstractFileUrisLoaderImpl newLoader() {
        return new AbstractFileUrisLoaderImpl();
    }

    private static final class AbstractFileUrisLoaderImpl extends AbstractFileUrisLoader {
        @Override
        public void open() throws CorbException {
        }

        @Override
        public boolean hasNext() throws CorbException {
            return false;
        }

        @Override
        public String next() throws CorbException {
            return null;
        }

        @Override
        public void close() {
        }
    }

    @Test
    void testGetMetadataIncludesSourceAndLastModified() throws Exception {
        File tempFile = File.createTempFile("corb-loader", ".txt");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), "hello".getBytes(StandardCharsets.UTF_8));

        try (AbstractFileUrisLoader loader = newLoader()) {
            Properties props = new Properties();
            props.setProperty(LOADER_PATH, tempFile.getParent());
            loader.setProperties(props);

            Map<String, String> metadata = loader.getMetadata(tempFile);
            assertEquals(tempFile.getName(), metadata.get(AbstractFileUrisLoader.META_FILENAME));
            assertTrue(metadata.get(AbstractFileUrisLoader.META_PATH).endsWith(".txt"));
            assertEquals(tempFile.getParent(), metadata.get(AbstractFileUrisLoader.META_SOURCE));
            assertNotNull(metadata.get(AbstractFileUrisLoader.META_LAST_MODIFIED));
        }
    }

    @Test
    void testGetMetaPathWithoutLoaderPath() throws Exception {
        File tempFile = File.createTempFile("corb-loader", ".txt");
        tempFile.deleteOnExit();

        try (AbstractFileUrisLoader loader = newLoader()) {
            assertEquals(tempFile.getCanonicalPath(), loader.getMetaPath(tempFile));
        }
    }

    @Test
    void testGetMetaPathWithLoaderPathStripsPrefix() throws Exception {
        File root = Files.createTempDirectory("corb-loader-root").toFile();
        root.deleteOnExit();
        File nested = new File(root, "nested/child.txt");
        assertTrue(nested.getParentFile().mkdirs());
        assertTrue(nested.createNewFile());
        nested.deleteOnExit();

        try (AbstractFileUrisLoader loader = newLoader()) {
            Properties props = new Properties();
            props.setProperty(LOADER_PATH, root.getCanonicalPath());
            loader.setProperties(props);

            assertEquals("nested/child.txt", loader.getMetaPath(nested));
        }
    }

    @Test
    void testToLoaderDocFromFileAndInputStream() throws Exception {
        File tempFile = File.createTempFile("corb-loader", ".txt");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), "abc".getBytes(StandardCharsets.UTF_8));

        try (AbstractFileUrisLoader loader = newLoader(); InputStream inputStream = Files.newInputStream(tempFile.toPath())) {
            Document doc = loader.toLoaderDoc(tempFile);
            assertNotNull(doc);
            assertEquals(AbstractFileUrisLoader.LOADER_DOC, doc.getDocumentElement().getTagName());

            Element content = (Element) doc.getDocumentElement().getElementsByTagName(AbstractFileUrisLoader.CONTENT).item(0);
            assertEquals("true", content.getAttribute(AbstractFileUrisLoader.BASE64_ENCODED));
            assertFalse(content.getTextContent().isEmpty());

            Map<String, String> metadata = new HashMap<>();
            metadata.put(AbstractFileUrisLoader.META_FILENAME, tempFile.getName());
            Document doc2 = loader.toLoaderDoc(metadata, inputStream);
            assertEquals(AbstractFileUrisLoader.LOADER_DOC, doc2.getDocumentElement().getTagName());
        }
    }

    @Test
    void testToLoaderDocFromMetadataAndString() throws Exception {
        try (AbstractFileUrisLoader loader = newLoader()) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("filename", "demo.txt");

            Document doc = loader.toLoaderDoc(metadata, "hello", false);
            Element root = doc.getDocumentElement();
            assertEquals(AbstractFileUrisLoader.LOADER_DOC, root.getTagName());
            Element content = (Element) root.getElementsByTagName(AbstractFileUrisLoader.CONTENT).item(0);
            assertEquals("false", content.getAttribute(AbstractFileUrisLoader.BASE64_ENCODED));
            assertEquals("hello", content.getTextContent());

            NodeList metadataNodes = root.getElementsByTagName(AbstractFileUrisLoader.LOADER_METADATA);
            assertNotNull(metadataNodes.item(0));
        }
    }

    @Test
    void testToLoaderDocFromMetadataAndNode() throws Exception {
        try (AbstractFileUrisLoader loader = newLoader()) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("filename", "demo.xml");

            Document payloadDoc = loader.docFactory.newDocumentBuilder().newDocument();
            Element payloadRoot = payloadDoc.createElement("payload");
            payloadRoot.setTextContent("plain-text");
            payloadDoc.appendChild(payloadRoot);

            Document doc = loader.toLoaderDoc(metadata, payloadRoot, false);
            Element root = doc.getDocumentElement();
            assertEquals(AbstractFileUrisLoader.LOADER_DOC, root.getTagName());
            Element content = (Element) root.getElementsByTagName(AbstractFileUrisLoader.CONTENT).item(0);
            assertEquals("false", content.getAttribute(AbstractFileUrisLoader.BASE64_ENCODED));
            assertEquals("plain-text", content.getTextContent());
        }
    }

    @Test
    void testGetTempDirUsesProvidedWritableDirectory() throws Exception {
        Path tempRoot = Files.createTempDirectory("corb-loader-temp-root");
        tempRoot.toFile().deleteOnExit();

        try (AbstractFileUrisLoader loader = newLoader()) {
            Path dir = loader.getTempDir(new File(tempRoot.toString()), tempRoot.toString());
            assertTrue(Files.isDirectory(dir));
            assertTrue(dir.toString().startsWith(tempRoot.toString()));
        }
    }

    @Test
    void testGetTempDirRejectsInvalidDirectory() throws Exception {
        File tempFile = File.createTempFile("corb-loader-invalid-dir", ".txt");
        tempFile.deleteOnExit();

        try (AbstractFileUrisLoader loader = newLoader()) {
            assertThrows(InvalidParameterException.class,
                () -> loader.getTempDir(tempFile, tempFile.getAbsolutePath()));
        }
    }

    @Test
    void testToISODateTimeVariants() {
        try (AbstractFileUrisLoader loader = newLoader()) {
            String isoFromMilliseconds = loader.toISODateTime(0L);
            assertEquals("1970-01-01T00:00Z", isoFromMilliseconds);

            String isoFromInstant = loader.toISODateTime(Instant.parse("2024-02-03T04:05:06Z"));
            assertEquals("2024-02-03T04:05Z", isoFromInstant);
        }
    }

    @Test
    void testShouldUseEnvelopeAndBase64EncodeDefaults() {
        try (AbstractFileUrisLoader loader = newLoader()) {
            assertTrue(loader.shouldUseEnvelope());
            assertTrue(loader.shouldBase64Encode());
        }
    }

    @Test
    void testShouldUseEnvelopeAndBase64EncodeFromProperties() {
        try (AbstractFileUrisLoader loader = newLoader()) {
            Properties props = new Properties();
            props.setProperty(LOADER_USE_ENVELOPE, "false");
            props.setProperty(LOADER_BASE64_ENCODE, "false");
            loader.setProperties(props);

            assertFalse(loader.shouldUseEnvelope());
            assertFalse(loader.shouldBase64Encode());
        }
    }
}
