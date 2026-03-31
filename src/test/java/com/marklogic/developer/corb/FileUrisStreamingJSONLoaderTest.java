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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.marklogic.developer.corb.Options.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileUrisStreamingJSONLoaderTest {

    private static final String SAMPLE_JSON = "src/test/resources/jsonUrisLoader/sample.json";
    private static final String ARRAY_ROOT_JSON = "src/test/resources/jsonUrisLoader/array-root.json";

    @Test
    void openExtractsConfiguredJsonValues() throws Exception {
        try (FileUrisStreamingJSONLoader loader = newLoader("/items/*", "/metadata", false)) {
            loader.open();
            assertEquals(3, loader.getTotalCount());
            String metadata = loader.properties.getProperty(PRE_BATCH_MODULE + '.' + METADATA);
            assertTrue(metadata.contains("\"batch\": 42"));
            List<String> values = readAll(loader);
            assertEquals(3, values.size());
            assertTrue(values.get(0).contains("\"uri\": \"/items/1.json\""));
        }
    }

    @Test
    void openExtractsConfiguredJsonValuesUseEnvelopeAndBase64() throws Exception {
        try (FileUrisStreamingJSONLoader loader = newLoader("/items/*", "/metadata", true)) {
            loader.properties.setProperty(LOADER_BASE64_ENCODE, "true");
            loader.open();
            assertEquals(3, loader.getTotalCount());
            String metadata = loader.properties.getProperty(PRE_BATCH_MODULE + '.' + METADATA);
            assertFalse(metadata.contains("\"batch\": 42"));
            List<String> values = readAll(loader);
            assertEquals(3, values.size());
            assertFalse(values.get(0).contains("\"uri\": \"/items/1.json\""));
        }
    }

    @Test
    void openSupportsDescendantMatching() throws Exception {
        try (FileUrisStreamingJSONLoader loader = newLoader("//uri", null, false)) {
            loader.open();
            assertEquals(4, loader.getTotalCount());
            List<String> values = readAll(loader);
            assertEquals(4, values.size());
            assertEquals("\"/items/1.json\"", values.get(0));
            assertEquals("\"/groups/1.json\"", values.get(3));
        }
    }

    @Test
    void openWrapsValuesInLoaderEnvelopeWhenEnabled() throws Exception {
        try (FileUrisStreamingJSONLoader loader = newLoader("/items/*", null, true)) {
            loader.open();
            String value = loader.next();
            assertTrue(value.contains(AbstractFileUrisLoader.LOADER_DOC));
            assertTrue(value.contains(AbstractFileUrisLoader.BASE64_ENCODED + "=\"false\""));
            assertTrue(value.contains("application/json"));
        }
    }

    @Test
    void openSelectsEachRootArrayObjectWithSlashStar() throws Exception {
        try (FileUrisStreamingJSONLoader loader = newLoader(ARRAY_ROOT_JSON, "/*", null, false)) {
            loader.open();
            assertEquals(2, loader.getTotalCount());
            List<String> values = readAll(loader);
            assertEquals(2, values.size());
            assertTrue(values.get(0).contains("\"uri\": \"/array/1.json\""));
            assertTrue(values.get(1).contains("\"uri\": \"/array/2.json\""));
        }
    }

    @Test
    void openSelectsChildrenOfEachRootArrayObjectWithSlashStarSlashStar() throws Exception {
        try (FileUrisStreamingJSONLoader loader = newLoader(ARRAY_ROOT_JSON, "/*/*", null, false)) {
            loader.open();
            assertEquals(4, loader.getTotalCount());
            List<String> values = readAll(loader);
            assertEquals(4, values.size());
            assertEquals("\"/array/1.json\"", values.get(0));
            assertEquals("\"alpha\"", values.get(1));
            assertEquals("\"/array/2.json\"", values.get(2));
            assertEquals("\"beta\"", values.get(3));
        }
    }

    private FileUrisStreamingJSONLoader newLoader(String nodeExpression, String metadataExpression, boolean useEnvelope) {
        return newLoader(SAMPLE_JSON, nodeExpression, metadataExpression, useEnvelope);
    }

    private FileUrisStreamingJSONLoader newLoader(String jsonFile, String nodeExpression, String metadataExpression, boolean useEnvelope) {
        FileUrisStreamingJSONLoader loader = new FileUrisStreamingJSONLoader();
        Properties properties = new Properties();
        properties.setProperty(JSON_FILE, jsonFile);
        properties.setProperty(LOADER_USE_ENVELOPE, Boolean.toString(useEnvelope));
        if (nodeExpression != null) {
            properties.setProperty(JSON_NODE, nodeExpression);
        }
        if (metadataExpression != null) {
            properties.setProperty(JSON_METADATA, metadataExpression);
        }
        loader.setProperties(properties);
        return loader;
    }

    private List<String> readAll(FileUrisStreamingJSONLoader loader) throws CorbException {
        List<String> values = new ArrayList<>();
        while (loader.hasNext()) {
            String value = loader.next();
            assertFalse(value == null || value.isEmpty());
            values.add(value);
        }
        return values;
    }
}
