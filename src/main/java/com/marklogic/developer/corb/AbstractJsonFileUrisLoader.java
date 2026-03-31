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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
import static com.marklogic.developer.corb.util.XmlUtils.documentToString;

/**
 * Shared behavior for JSON file-based URI loaders.
 */
abstract class AbstractJsonFileUrisLoader extends AbstractFileUrisLoader {

    protected static final Logger LOG = Logger.getLogger(AbstractJsonFileUrisLoader.class.getName());
    protected static final String EXCEPTION_MSG_PROBLEM_READING_JSON_FILE = "Problem while reading the JSON file";
    protected static final String EXCEPTION_MSG_PROBLEM_READING_JSON_METADATA = "Problem while reading the JSON metadata from file";

    protected File jsonFile;
    protected String customMetadata;

    protected String toLoaderPayload(String jsonContent, File file) throws CorbException {
        if (!shouldUseEnvelope()) {
            return jsonContent;
        }

        try {
            Map<String, String> metadata = file != null ? getMetadata(file) : new HashMap<>();
            metadata.put(META_CONTENT_TYPE, "application/json");
            String selector = getProperty(JSON_NODE);
            if (selector != null) {
                metadata.put(JSON_NODE, selector);
            }
            if (shouldBase64Encode()) {
                return documentToString(
                    toLoaderDoc(metadata, new ByteArrayInputStream(jsonContent.getBytes(StandardCharsets.UTF_8)))
                );
            }
            return documentToString(toLoaderDoc(metadata, jsonContent, false));
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_JSON_FILE, ex);
        }
    }

    protected void setMetadataContentToModules(String metadataContent, File file) throws CorbException {
        if (metadataContent == null) {
            return;
        }
        try {
            String content = metadataContent;
            if (shouldUseEnvelope()) {
                Map<String, String> metadataMap = file != null ? getMetadata(file) : new HashMap<>();
                metadataMap.put(META_CONTENT_TYPE, "application/json");
                if (shouldBase64Encode()) {
                    content = documentToString(toLoaderDoc(metadataMap, new ByteArrayInputStream(metadataContent.getBytes(StandardCharsets.UTF_8))));
                } else {
                    content = documentToString(toLoaderDoc(metadataMap, metadataContent, false));
                }
            }
            properties.put(PRE_BATCH_MODULE + '.' + METADATA, content);
            properties.put(POST_BATCH_MODULE + '.' + METADATA, content);
            if (stringToBoolean(getProperty(METADATA_TO_PROCESS_MODULE), false)) {
                properties.put(PROCESS_MODULE + '.' + METADATA, content);
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, EXCEPTION_MSG_PROBLEM_READING_JSON_METADATA, ex);
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_JSON_METADATA, ex);
        }
    }

    @Override
    protected boolean shouldBase64Encode() {
        String shouldEncode = getProperty(LOADER_BASE64_ENCODE);
        return stringToBoolean(shouldEncode, false);
    }
}
