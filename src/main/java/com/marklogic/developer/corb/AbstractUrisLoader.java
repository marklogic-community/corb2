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

import static com.marklogic.developer.corb.Options.URIS_REPLACE_PATTERN;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.ContentSource;
import java.util.Properties;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public abstract class AbstractUrisLoader implements UrisLoader {

    protected Options options;
    protected ContentSource cs;
    protected String collection;
    private int total = 0;
    protected String[] replacements = new String[0];
    protected String batchRef;

    @Override
    public void setOptions(Options options) {
        this.options = options;
    }

    protected Options getOptions() {
        return options;
    }
    
    @Override
    public void setContentSource(ContentSource cs) {
        this.cs = cs;
    }

    @Override
    public void setCollection(String collection) {
        this.collection = collection;
    }

    @Override
    public String getBatchRef() {
        return batchRef;
    }
    
    public void setBatchRef(String batchRef) {
        this.batchRef = batchRef;
    }
    
    @Override
    public int getTotalCount() {
        return this.total;
    }
    
    public void setTotalCount(int totalCount) {
        this.total = totalCount;
    }
    
    protected String getProperty(String key) {
        String value = null;
        if(options != null){
            value = options.getProperty(key, false);
        }
        return value;
    }

    protected void cleanup() {
        options = null;
        cs = null;
        collection = null;
        replacements = null;
        batchRef = null;
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
}
