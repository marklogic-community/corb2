/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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

import static com.marklogic.developer.corb.Options.MAX_OPTS_FROM_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.URIS_MODULE;
import static com.marklogic.developer.corb.Options.XQUERY_MODULE;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.getInlineModuleCode;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isInlineModule;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class QueryUrisLoader extends AbstractUrisLoader {

    private static final int DEFAULT_MAX_OPTS_FROM_MODULE = 10;
    private static final Pattern MODULE_CUSTOM_INPUT = Pattern.compile("("
            + PRE_BATCH_MODULE + "|" + PROCESS_MODULE + "|" + XQUERY_MODULE + "|" + POST_BATCH_MODULE
            + ")\\.[A-Za-z0-9]+=.*");

    Session session;
    ResultSequence res;

    private static final Logger LOG = Logger.getLogger(QueryUrisLoader.class.getName());

    public QueryUrisLoader() {
    }

    @Override
    public void open() throws CorbException {
        List<String> propertyNames = new ArrayList<String>();
        if (properties != null) {
            propertyNames.addAll(properties.stringPropertyNames());
        }
        propertyNames.addAll(System.getProperties().stringPropertyNames());
        parseUriReplacePatterns();

        try {
            RequestOptions opts = new RequestOptions();
            opts.setCacheResult(false);
            // this should be a noop, but xqsync does it
            opts.setResultBufferSize(0);
            LOG.log(Level.INFO, "buffer size = {0}, caching = {1}",
                    new Object[]{opts.getResultBufferSize(), opts.getCacheResult()});

            session = cs.newSession();
            Request req = null;
            String urisModule = options.getUrisModule();
            if (isInlineOrAdhoc(urisModule)) {
                String adhocQuery;
                if (isInlineModule(urisModule)) {
                    adhocQuery = getInlineModuleCode(urisModule);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException("Unable to read inline module");
                    }
                    LOG.log(Level.INFO, "invoking inline uris module");
                } else {
                    String queryPath = urisModule.substring(0, urisModule.indexOf('|'));
                    adhocQuery = AbstractManager.getAdhocQuery(queryPath);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException("Unable to read adhoc query " + queryPath + " from classpath or filesystem");
                    }
                    LOG.log(Level.INFO, "invoking adhoc uris module {0}", queryPath);
                }
                req = session.newAdhocQuery(adhocQuery);
                if (isJavaScriptModule(urisModule)) {
                    opts.setQueryLanguage("javascript");
                }
            } else {
                String root = options.getModuleRoot();
                String modulePath = buildModulePath(root, urisModule);
                LOG.log(Level.INFO, "invoking uris module {0}", modulePath);
                req = session.newModuleInvoke(modulePath);
            }
            // NOTE: collection will be treated as a CWSV
            req.setNewStringVariable("URIS", collection);
            // TODO support DIRECTORY as type
            req.setNewStringVariable("TYPE", TransformOptions.COLLECTION_TYPE);
            req.setNewStringVariable("PATTERN", "[,\\s]+");

            // custom inputs
            for (String propName : propertyNames) {
                if (propName.startsWith(URIS_MODULE + ".")) {
                    String varName = propName.substring((URIS_MODULE + ".").length());
                    String value = getProperty(propName);
                    if (value != null) {
                        req.setNewStringVariable(varName, value);
                    }
                }
            }

            req.setOptions(opts);

            res = session.submitRequest(req);
            ResultItem next = res.next();

            int maxOpts = this.getMaxOptionsFromModule();
            for (int i = 0; i < maxOpts && next != null && batchRef == null && !(next.getItem().asString().matches("\\d+")); i++) {
                String value = next.getItem().asString();
                if (MODULE_CUSTOM_INPUT.matcher(value).matches()) {
                    int idx = value.indexOf('=');
                    properties.put(value.substring(0, idx).replace(XQUERY_MODULE + ".", PROCESS_MODULE + "."), value.substring(idx + 1));
                } else {
                    batchRef = value;
                }
                next = res.next();
            }

            try {
                total = Integer.parseInt(next.getItem().asString());
            } catch (NumberFormatException exc) {
                throw new CorbException("Uris module " + options.getUrisModule() + " does not return total URI count");
            }
        } catch (RequestException exc) {
            throw new CorbException("While invoking Uris Module", exc);
        }
    }

    @Override
    public boolean hasNext() throws CorbException {
        return res != null && res.hasNext();
    }

    @Override
    public String next() throws CorbException {
        String next = res.next().asString();
        for (int i = 0; i < replacements.length - 1; i += 2) {
            next = next.replaceAll(replacements[i], replacements[i + 1]);
        }
        return next;
    }

    @Override
    public void close() {
        if (session != null) {
            LOG.info("closing uris session");
            try {
                if (res != null) {
                    res.close();
                    res = null;
                }
            } finally {
                session.close();
                session = null;
            }
        }
        cleanup();
    }

    protected int getMaxOptionsFromModule() {
        int max = DEFAULT_MAX_OPTS_FROM_MODULE;
        String maxStr = getProperty(MAX_OPTS_FROM_MODULE);
        if (isNotEmpty(maxStr)) {
            try {
                max = Integer.parseInt(maxStr);
            } catch (NumberFormatException ex) {
                LOG.log(Level.WARNING, "Unable to parse MaxOptionsFromModule value: {0}, using default value: {1}", new Object[]{maxStr, DEFAULT_MAX_OPTS_FROM_MODULE});
            }
        }
        return max;
    }
}
