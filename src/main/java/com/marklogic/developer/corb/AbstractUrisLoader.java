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

import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.URIS_REPLACE_PATTERN;
import static com.marklogic.developer.corb.Options.URIS_TOTAL_COUNT;

import com.marklogic.developer.corb.util.StringUtils;

import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trim;

import java.util.Properties;

/**
 * Abstract base class for URI loader implementations.
 * Provides common functionality for loading URIs to be processed by CoRB tasks,
 * including configuration management, URI replacement patterns, total count tracking,
 * and batch reference handling.
 *
 * <p>Subclasses must implement specific URI loading strategies such as:</p>
 * <ul>
 *   <li>Loading from files</li>
 *   <li>Querying from MarkLogic</li>
 *   <li>Loading from external sources</li>
 * </ul>
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public abstract class AbstractUrisLoader implements UrisLoader {

    /**
     * Transform options containing configuration settings for the CoRB job.
     */
    protected TransformOptions options;

    /**
     * Pool of content sources for connecting to MarkLogic database.
     * Shared across multiple stages and should not be closed by the loader.
     */
    protected ContentSourcePool csp;

    /**
     * Collection name to use when loading URIs.
     * May be used by subclasses to filter or organize URI loading.
     */
    protected String collection;

    /**
     * Configuration properties containing job settings and parameters.
     * Used for retrieving configuration values with fallback to system properties.
     */
    protected Properties properties;

    /**
     * Total number of URIs to be processed by the job.
     * Updated via {@link #setTotalCount(long)} and made available to batch modules.
     */
    private long total = 0;

    /**
     * Array of URI replacement patterns parsed from {@link Options#URIS_REPLACE_PATTERN}.
     * Contains alternating find/replace strings (even indices are find patterns, odd indices are replacements).
     */
    protected String[] replacements = new String[0];

    /**
     * Batch reference identifier for tracking the current batch execution.
     * Used to correlate URIs with a specific batch run.
     */
    protected String batchRef;

    /**
     * Constructor that initializes the TransformOptions.
     */
    public AbstractUrisLoader() {
        options = new TransformOptions();
    }

    /**
     * Sets the transform options for the CoRB job.
     *
     * @param options the TransformOptions instance
     */
    @Override
    public void setOptions(TransformOptions options) {
        this.options = options;
    }

    /**
     * Gets the transform options.
     *
     * @return the TransformOptions instance
     */
    public TransformOptions getOptions() {
        return options;
    }

    /**
     * Sets the content source pool for database connections.
     *
     * @param csp the ContentSourcePool instance
     */
    @Override
    public void setContentSourcePool(ContentSourcePool csp) {
        this.csp = csp;
    }

    /**
     * Sets the collection name for URI loading.
     *
     * @param collection the collection name
     */
    @Override
    public void setCollection(String collection) {
        this.collection = collection;
    }

    /**
     * Sets the configuration properties.
     *
     * @param properties the Properties object
     */
    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Gets the batch reference identifier.
     *
     * @return the batch reference string, or null if not set
     */
    @Override
    public String getBatchRef() {
        return batchRef;
    }

    /**
     * Sets the batch reference identifier.
     *
     * @param batchRef the batch reference string
     */
    public void setBatchRef(String batchRef) {
        this.batchRef = batchRef;
    }

    /**
     * Gets the total count of URIs to be processed.
     *
     * @return the total count of URIs
     */
    @Override
    public long getTotalCount() {
        return this.total;
    }

    /**
     * Sets the total count of URIs to be processed.
     * Also updates the properties to make the count available to PRE_BATCH_MODULE and POST_BATCH_MODULE.
     *
     * @param totalCount the total number of URIs
     */
    public void setTotalCount(long totalCount) {
        this.total = totalCount;

        if (properties != null && this.total > 0) {
            properties.put(PRE_BATCH_MODULE + '.' + URIS_TOTAL_COUNT, String.valueOf(this.total));
            properties.put(POST_BATCH_MODULE + '.' + URIS_TOTAL_COUNT, String.valueOf(this.total));
        }
    }

    /**
     * Retrieves a property value by key.
     * First checks system properties, then falls back to instance properties.
     * The returned value is trimmed of leading and trailing whitespace.
     *
     * @param key the property key name
     * @return the trimmed property value, or null if not found
     */
    public String getProperty(String key) {
        String val = System.getProperty(key);
        if (val == null && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }

    /**
     * Closes the URI loader and releases resources.
     * Note: Does not close the ContentSourcePool as it will be used by downstream stages.
     */
    @Override
    public void close() {
        //don't close the ContentSourcePool. It will be used by downstream stages.
        cleanup();
    }

    /**
     * Releases resources held by the URI loader.
     * Sets all fields to null to aid garbage collection and resets the total count to 0.
     */
    protected void cleanup() {
        options = null;
        csp = null;
        collection = null;
        properties = null;
        replacements = null;
        batchRef = null;
        total = 0;
    }

    /**
     * Parses URI replacement patterns from {@link Options#URIS_REPLACE_PATTERN}.
     * Patterns should be comma-separated pairs of find/replace values.
     * The replacements array will contain alternating find and replace strings.
     *
     * @throws IllegalArgumentException if the pattern contains an odd number of elements
     */
    protected void parseUriReplacePatterns() {
        String urisReplacePattern = getProperty(URIS_REPLACE_PATTERN);
        if (isNotEmpty(urisReplacePattern)) {
            replacements = urisReplacePattern.split(",", -1);
            if (replacements.length % 2 != 0) {
                throw new IllegalArgumentException("Invalid replacement pattern " + urisReplacePattern);
            }
        }
    }

    /**
     * Determines whether the batch reference should be set.
     * Reads the {@link Options#LOADER_SET_URIS_BATCH_REF} property.
     *
     * @return true if batch reference should be set, false otherwise (defaults to false)
     */
    protected boolean shouldSetBatchRef() {
        String setBatchRef = getProperty(Options.LOADER_SET_URIS_BATCH_REF);
        return StringUtils.stringToBoolean(setBatchRef, false);
    }

    /**
     * Gets the loader path by checking multiple property names in order.
     * Tries each provided property name until a non-blank value is found.
     * Falls back to {@link Options#LOADER_PATH} if all provided names are blank.
     *
     * @param propertyName variable number of property names to check in order
     * @return the loader path, or null if no path is configured
     */
    protected String getLoaderPath(String... propertyName) {
        String loaderPath = null;
        for (String name : propertyName) {
            if (isBlank(loaderPath)) {
                loaderPath = getProperty(name);
            }
        }
        if (isBlank(loaderPath)) {
            loaderPath = getProperty(Options.LOADER_PATH);
        }
        return loaderPath;
    }
}
