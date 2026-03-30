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

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.marklogic.developer.corb.Options.JSON_FILE;
import static com.marklogic.developer.corb.Options.JSON_METADATA;
import static com.marklogic.developer.corb.Options.JSON_NODE;
import static com.marklogic.developer.corb.Options.JSON_TEMP_DIR;
import static com.marklogic.developer.corb.Options.TEMP_DIR;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

/**
 * Memory-efficient JSON URI loader that extracts matching JSON values to temp files.
 */
public class FileUrisStreamingJSONLoader extends AbstractJsonFileUrisLoader {

    private Path tempDir;
    private Iterator<Path> files;
    private final List<Path> extractedFiles = new ArrayList<>();
    private final FileAttribute<?>[] fileAttributes = new FileAttribute<?>[0];

    @Override
    public void open() throws CorbException {
        String jsonFilename = getLoaderPath(JSON_FILE);
        String selectorExpression = getProperty(JSON_NODE);
        String metadataExpression = getProperty(JSON_METADATA);
        jsonFile = FileUtils.getFile(jsonFilename);
        try {
            if (shouldSetBatchRef()) {
                batchRef = jsonFile.getCanonicalPath();
            }
            tempDir = getTempDir();
            JsonSelector selector = JsonSelectorFactory.newSelector(selectorExpression);
            JsonSelector metadataSelector = isNotEmpty(metadataExpression) ? JsonSelectorFactory.newSelector(metadataExpression) : null;
            JsonExtractor extractor = new JsonExtractor(selector, metadataSelector, new JsonExtractor.ExtractionHandler() {
                @Override
                public void onNode(String currentPath, String rawJson) throws IOException {
                    Path file = Files.createTempFile(tempDir, "json-value-", ".json", fileAttributes);
                    Files.write(file, rawJson.getBytes(StandardCharsets.UTF_8));
                    extractedFiles.add(file);
                }

                @Override
                public void onMetadata(String currentPath, String rawJson) {
                    customMetadata = rawJson;
                }
            });
            try (Reader reader = Files.newBufferedReader(jsonFile.toPath())) {
                setTotalCount(extractor.extract(reader));
            }
            files = extractedFiles.iterator();
            setMetadataContentToModules(customMetadata, jsonFile);
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_JSON_FILE, ex);
        }
    }

    @Override
    public boolean hasNext() {
        return files != null && files.hasNext();
    }

    @Override
    public String next() throws CorbException {
        Path path = files.next();
        try {
            String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            String payload;
            if (shouldUseEnvelope()) {
                Map<String, String> metadata = getMetadata(jsonFile);
                metadata.put(META_SOURCE, path.toFile().getCanonicalPath());
                metadata.put(META_CONTENT_TYPE, "application/json");
                if (shouldBase64Encode()) {
                    try (java.io.InputStream inputStream = Files.newInputStream(path)) {
                        payload = com.marklogic.developer.corb.util.XmlUtils.documentToString(toLoaderDoc(metadata, inputStream));
                    }
                } else {
                    payload = com.marklogic.developer.corb.util.XmlUtils.documentToString(toLoaderDoc(metadata, json, false));
                }
            } else {
                payload = json;
            }
            Files.deleteIfExists(path);
            return payload;
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_JSON_FILE, ex);
        }
    }

    protected Path getTempDir() throws IOException {
        String tempDirOption = getProperty(JSON_TEMP_DIR);
        if (StringUtils.isBlank(tempDirOption)) {
            tempDirOption = getProperty(TEMP_DIR);
        }
        return getTempDir(jsonFile, tempDirOption, fileAttributes);
    }

    @Override
    public void close() {
        super.close();
        FileUtils.deleteQuietly(tempDir);
        files = null;
        extractedFiles.clear();
        tempDir = null;
        customMetadata = null;
        jsonFile = null;
    }
}
