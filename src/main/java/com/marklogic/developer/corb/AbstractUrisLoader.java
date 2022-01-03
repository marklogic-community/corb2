/*
  * * Copyright (c) 2004-2022 MarkLogic Corporation
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
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public abstract class AbstractUrisLoader implements UrisLoader {

    protected TransformOptions options;
    protected ContentSourcePool csp;
    protected String collection;
    protected Properties properties;
    private long total = 0;
    protected String[] replacements = new String[0];
    protected String batchRef;

    public AbstractUrisLoader() {
        options = new TransformOptions();
    }

    @Override
    public void setOptions(TransformOptions options) {
        this.options = options;
    }

    public TransformOptions getOptions() {
        return options;
    }

    @Override
    public void setContentSourcePool(ContentSourcePool csp) {
        this.csp = csp;
    }

    @Override
    public void setCollection(String collection) {
        this.collection = collection;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getBatchRef() {
        return batchRef;
    }

    public void setBatchRef(String batchRef) {
        this.batchRef = batchRef;
    }

    @Override
    public long getTotalCount() {
        return this.total;
    }

    public void setTotalCount(long totalCount) {
        this.total = totalCount;

        if (properties != null && this.total > 0) {
            properties.put(PRE_BATCH_MODULE + '.' + URIS_TOTAL_COUNT, String.valueOf(this.total));
            properties.put(POST_BATCH_MODULE + '.' + URIS_TOTAL_COUNT, String.valueOf(this.total));
        }
    }

    public String getProperty(String key) {
        String val = System.getProperty(key);
        if (val == null && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }

    @Override
    public void close() {
        //don't close the ContentSourcePool. It will be used by downstream stages.
        cleanup();
    }

    protected void cleanup() {
        options = null;
        csp = null;
        collection = null;
        properties = null;
        replacements = null;
        batchRef = null;
        total = 0;
    }

    protected void parseUriReplacePatterns() {
        String urisReplacePattern = getProperty(URIS_REPLACE_PATTERN);
        if (isNotEmpty(urisReplacePattern)) {
            replacements = urisReplacePattern.split(",", -1);
            if (replacements.length % 2 != 0) {
                throw new IllegalArgumentException("Invalid replacement pattern " + urisReplacePattern);
            }
        }
    }

    protected boolean shouldSetBatchRef() {
        String setBatchRef = getProperty(Options.LOADER_SET_URIS_BATCH_REF);
        return StringUtils.stringToBoolean(setBatchRef, false);
    }

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
