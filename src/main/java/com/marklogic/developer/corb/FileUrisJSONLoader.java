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

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static com.marklogic.developer.corb.Options.JSON_FILE;
import static com.marklogic.developer.corb.Options.JSON_METADATA;
import static com.marklogic.developer.corb.Options.JSON_NODE;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

/**
 * JSON URI loader that keeps extracted values in memory for iteration.
  * @since 2.6.0
 */
public class FileUrisJSONLoader extends AbstractJsonFileUrisLoader {

    protected final List<String> extractedValues = new ArrayList<>();
    protected int index;

    /**
     * Opens the JSON file, extracts values based on the provided JSON node selector, and stores them in memory.
     *
     * @throws CorbException if there is an issue reading the JSON file or extracting values
     */
    @Override
    public void open() throws CorbException {
        String fileName = getLoaderPath(JSON_FILE);
        jsonFile = FileUtils.getFile(fileName);
        try {
            if (shouldSetBatchRef()) {
                batchRef = jsonFile.getCanonicalPath();
            }

            JsonSelector selector = JsonSelectorFactory.newSelector(getProperty(JSON_NODE));
            String metadataExpression = getProperty(JSON_METADATA);
            JsonSelector metadataSelector = isNotEmpty(metadataExpression) ? JsonSelectorFactory.newSelector(metadataExpression) : null;
            JsonExtractor extractor = new JsonExtractor(selector, metadataSelector, new JsonExtractor.ExtractionHandler() {
                @Override
                public void onNode(String currentPath, String rawJson) {
                    extractedValues.add(rawJson);
                }

                @Override
                public void onMetadata(String currentPath, String rawJson) {
                    customMetadata = rawJson;
                }
            });
            try (Reader reader = Files.newBufferedReader(jsonFile.toPath())) {
                setTotalCount(extractor.extract(reader));
            }
            setMetadataContentToModules(customMetadata, jsonFile);
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_JSON_FILE, ex);
        }
    }

    /** Checks if there are more extracted values to iterate over.
     *
     * @return true if there are more values, false otherwise
     */
    @Override
    public boolean hasNext() {
        return index < extractedValues.size();
    }
    /** Returns the next extracted value as a loader payload.
     *
     * @return the next loader payload, or null if there are no more values
     * @throws CorbException if there is an issue converting the extracted value to a loader payload
     */
    @Override
    public String next() throws CorbException {
        if (!hasNext()) {
            return null;
        }
        return toLoaderPayload(extractedValues.get(index++), jsonFile);
    }

    /**
     * Closes the loader and clears any stored extracted values and metadata.
     */
    @Override
    public void close() {
        super.close();
        extractedValues.clear();
        index = 0;
        customMetadata = null;
        jsonFile = null;
    }
}
