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
import static com.marklogic.developer.corb.Options.LOADER_USE_ENVELOPE;
import static com.marklogic.developer.corb.Options.METADATA;
import static com.marklogic.developer.corb.Options.METADATA_TO_PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractJsonFileUrisLoaderTest {

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    private JsonLoader newLoader() {
        return new JsonLoader();
    }

    private static final class JsonLoader extends AbstractJsonFileUrisLoader {
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
    void testShouldBase64EncodeDefaultsToFalse() {
        try (JsonLoader loader = newLoader()) {
            assertFalse(loader.shouldBase64Encode());
        }
    }

    @Test
    void testToLoaderPayloadWithoutEnvelopeReturnsJsonAsIs() throws Exception {
        try (JsonLoader loader = newLoader()) {
            Properties properties = new Properties();
            properties.setProperty(LOADER_USE_ENVELOPE, "false");
            loader.setProperties(properties);

            String json = "{\"hello\":\"world\"}";
            assertEquals(json, loader.toLoaderPayload(json, null));
        }
    }

    @Test
    void testToLoaderPayloadWithEnvelopeWrapsJsonDocument() throws Exception {
        try (JsonLoader loader = newLoader()) {
            Properties properties = new Properties();
            properties.setProperty(LOADER_USE_ENVELOPE, "true");
            loader.setProperties(properties);

            String payload = loader.toLoaderPayload("{\"hello\":\"world\"}", null);
            assertTrue(payload.contains("<corb-loader"));
            assertTrue(payload.contains("application/json"));
            assertTrue(payload.contains("base64Encoded=\"false\""));
            assertTrue(payload.contains("hello"));
        }
    }

    @Test
    void testSetMetadataContentToModulesStoresMetadataForPreAndPost() throws Exception {
        try (JsonLoader loader = newLoader()) {
            Properties properties = new Properties();
            loader.setProperties(properties);

            File file = File.createTempFile("json-loader", ".json");
            file.deleteOnExit();
            Files.write(file.toPath(), "{\"source\":\"sample\"}".getBytes(StandardCharsets.UTF_8));

            loader.setMetadataContentToModules("{\"source\":\"sample\"}", file);

            assertNotNull(properties.getProperty(PRE_BATCH_MODULE + '.' + METADATA));
            assertNotNull(properties.getProperty(POST_BATCH_MODULE + '.' + METADATA));
            assertTrue(properties.getProperty(PRE_BATCH_MODULE + '.' + METADATA).contains("sample"));
        }
    }

    @Test
    void testSetMetadataContentToModulesStoresMetadataForProcessWhenEnabled() throws Exception {
        try (JsonLoader loader = newLoader()) {
            Properties properties = new Properties();
            properties.setProperty(METADATA_TO_PROCESS_MODULE, "true");
            loader.setProperties(properties);

            File file = File.createTempFile("json-loader", ".json");
            file.deleteOnExit();
            Files.write(file.toPath(), "{\"source\":\"sample\"}".getBytes(StandardCharsets.UTF_8));

            loader.setMetadataContentToModules("{\"source\":\"sample\"}", file);

            assertNotNull(properties.getProperty(PROCESS_MODULE + '.' + METADATA));
            assertTrue(properties.getProperty(PROCESS_MODULE + '.' + METADATA).contains("sample"));
        }
    }

    @Test
    void testShouldBase64EncodeCanBeOverriddenByProperty() {
        try (JsonLoader loader = newLoader()) {
            Properties properties = new Properties();
            properties.setProperty(LOADER_BASE64_ENCODE, "true");
            loader.setProperties(properties);

            assertTrue(loader.shouldBase64Encode());
        }
    }
}
