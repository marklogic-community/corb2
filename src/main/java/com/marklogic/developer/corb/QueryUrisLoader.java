/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
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
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class QueryUrisLoader extends AbstractUrisLoader {

    private static final int DEFAULT_MAX_OPTS_FROM_MODULE = 10;
    private static final Pattern MODULE_CUSTOM_INPUT = Pattern.compile('('
            + PRE_BATCH_MODULE + '|' + PROCESS_MODULE + '|' + XQUERY_MODULE + '|' + POST_BATCH_MODULE
            + ")\\.[A-Za-z0-9_-]+=.*");
    private Queue<String> queue;

    protected Session session;
    protected ResultSequence resultSequence;

    private static final Logger LOG = Logger.getLogger(QueryUrisLoader.class.getName());

    @Override
    public void open() throws CorbException {

        parseUriReplacePatterns();

        try {
            RequestOptions opts = new RequestOptions();
            opts.setCacheResult(false);
            // this should be a noop, but xqsync does it
            opts.setResultBufferSize(0);
            LOG.log(INFO, () -> MessageFormat.format("buffer size = {0}, caching = {1}",
                    opts.getResultBufferSize(), opts.getCacheResult()));

            session = cs.newSession();
            Request request;
            String urisModule = options.getUrisModule();
            if (isInlineOrAdhoc(urisModule)) {
                String adhocQuery;
                if (isInlineModule(urisModule)) {
                    adhocQuery = getInlineModuleCode(urisModule);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException("Unable to read inline module");
                    }
                    LOG.log(INFO, "invoking inline uris module");
                } else {
                    String queryPath = urisModule.substring(0, urisModule.indexOf('|'));
                    adhocQuery = AbstractManager.getAdhocQuery(queryPath);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException("Unable to read adhoc query " + queryPath + " from classpath or filesystem");
                    }
                    LOG.log(INFO, () -> MessageFormat.format("invoking adhoc uris module {0}", queryPath));
                }
                request = session.newAdhocQuery(adhocQuery);
                if (isJavaScriptModule(urisModule)) {
                    opts.setQueryLanguage("javascript");
                }
            } else {
                String root = options.getModuleRoot();
                String modulePath = buildModulePath(root, urisModule);
                LOG.log(INFO, () -> MessageFormat.format("invoking uris module {0}", modulePath));
                request = session.newModuleInvoke(modulePath);
            }
            // NOTE: collection will be treated as a CWSV
            request.setNewStringVariable("URIS", collection);
            // TODO support DIRECTORY as type
            request.setNewStringVariable("TYPE", TransformOptions.COLLECTION_TYPE);
            request.setNewStringVariable("PATTERN", "[,\\s]+");

            setCustomInputs(request);

            request.setOptions(opts);

            resultSequence = session.submitRequest(request);

            preProcess(resultSequence);

            queue = createAndPopulateQueue(resultSequence);

        } catch (RequestException exc) {
            throw new CorbException("While invoking Uris Module", exc);
        } finally {
            closeRequestAndSession();
        }
    }

    protected void preProcess(ResultSequence resultSequence) throws CorbException {
        ResultItem nextResultItem = collectCustomInputs(resultSequence);
        try {
            setTotalCount(Integer.parseInt(nextResultItem.getItem().asString()));
        } catch (NumberFormatException exc) {
            throw new CorbException("Uris module " + options.getUrisModule() + " does not return total URI count");
        }
    }

    /**
     * Collect any custom input options or batchRef value in the ResultSequence
     * that precede the count of URIs.
     *
     * @param resultSequence
     * @return the next ResultItem to retrieve from the ResultSequence
     */
    protected ResultItem collectCustomInputs(ResultSequence resultSequence) {
        ResultItem nextResultItem = resultSequence.next();
        int maxOpts = this.getMaxOptionsFromModule();
        for (int i = 0; i < maxOpts
                && nextResultItem != null
                && getBatchRef() == null
                && !(nextResultItem.getItem().asString().matches("\\d+")); i++) {
            String value = nextResultItem.getItem().asString();
            if (MODULE_CUSTOM_INPUT.matcher(value).matches()) {
                int idx = value.indexOf('=');
                properties.put(value.substring(0, idx).replace(XQUERY_MODULE + '.', PROCESS_MODULE + '.'), value.substring(idx + 1));
            } else {
                setBatchRef(value);
            }
            nextResultItem = resultSequence.next();
        }
        return nextResultItem;
    }

    /**
     * Collect all {@value Options#URIS_MODULE} properties from the properties and
     * System.properties (in that order, so System.properties will take
     * precedence over properties) and set as NewStringVariable for each
     * property the Request object.
     *
     * @param request
     */
    protected void setCustomInputs(Request request) {
        List<String> propertyNames = new ArrayList<>();
        // gather all of the p
        if (properties != null) {
            propertyNames.addAll(properties.stringPropertyNames());
        }
        propertyNames.addAll(System.getProperties().stringPropertyNames());
        // custom inputs
        for (String propName : propertyNames) {
            if (propName.startsWith(URIS_MODULE + '.')) {
                String varName = propName.substring((URIS_MODULE + '.').length());
                String value = getProperty(propName);
                if (value != null) {
                    request.setNewStringVariable(varName, value);
                }
            }
        }
    }

    /**
     * Instantiate a new queue and populate with items from the ResultSequence.
     *
     * @param resultSequence
     * @return
     */
    protected Queue<String> createAndPopulateQueue(ResultSequence resultSequence) {
        Queue<String> uriQueue = createQueue();
        return populateQueue(uriQueue, resultSequence);
    }

    protected Queue<String> populateQueue(Queue<String> queue, ResultSequence resultSequence) {
        int i = 0;
        String uri;
        while (resultSequence != null && resultSequence.hasNext()) {
            uri = resultSequence.next().asString();
            if (isBlank(uri)) {
                continue;
            }

            if (queue.isEmpty()) {
                LOG.log(INFO, "received first uri: " + uri);
            }
            //apply replacements (if any) - can be helpful in reducing in-memory footprint for ArrayQueue
            for (int j = 0; j < replacements.length - 1; j += 2) {
                uri = uri.replaceAll(replacements[j], replacements[j + 1]);
            }

            if (!queue.offer(uri)) {
                LOG.log(SEVERE, MessageFormat.format("Unabled to add uri {0} to queue. Received uris {1} which is more than expected {2}", uri, i + 1, getTotalCount()));
            } else if (i >= getTotalCount()) {
                LOG.log(WARNING, MessageFormat.format("Received uri {0} at index {1} which is more than expected {2}", uri, i + 1, getTotalCount()));
            }

            logQueueStatus(i, uri, getTotalCount());
            i++;
        }
        return queue;
    }

    /**
     * Factory method that will produce a new Queue.
     *
     * @return
     */
    protected Queue<String> createQueue() {
        Queue<String> uriQueue;
        if (options != null && options.shouldUseDiskQueue()) {
            uriQueue = new DiskQueue<>(options.getDiskQueueMaxInMemorySize(), options.getDiskQueueTempDir());
        } else {
            uriQueue = new ArrayQueue<>(getTotalCount());
        }
        return uriQueue;
    }

    @Override
    public boolean hasNext() throws CorbException {
        return queue != null && !queue.isEmpty();
    }

    @Override
    public String next() throws CorbException {
        if (queue == null) {
            throw new NoSuchElementException();
        }
        return queue.remove();
    }

    @Override
    public void close() {
        closeRequestAndSession();
        if (queue != null) {
            queue.clear();
            queue = null;
        }
        cleanup();
    }

    private void closeRequestAndSession() {
        if (session != null) {
            LOG.info("closing uris session");
            try {
                if (resultSequence != null) {
                    resultSequence.close();
                    resultSequence = null;
                }
            } finally {
                session.close();
                session = null;
            }
        }
    }

    protected int getMaxOptionsFromModule() {
        int max = DEFAULT_MAX_OPTS_FROM_MODULE;
        String maxStr = getProperty(MAX_OPTS_FROM_MODULE);
        if (isNotEmpty(maxStr)) {
            try {
                max = Integer.parseInt(maxStr);
            } catch (NumberFormatException ex) {
                LOG.log(WARNING, () -> MessageFormat.format("Unable to parse MaxOptionsFromModule value: {0}, using default value: {1}",
                        maxStr, DEFAULT_MAX_OPTS_FROM_MODULE));
            }
        }
        return max;
    }

    protected void logQueueStatus(int currentIndex, String uri, int total) {
        if (0 == currentIndex % 50000) {
            long freeMemory = Runtime.getRuntime().freeMemory();
            double megabytes = 1024d * 1024d;
            if (freeMemory < (16 * megabytes)) {
                LOG.log(WARNING, () -> MessageFormat.format("free memory: {0} MiB", freeMemory / megabytes));
            }
        }
        if (0 == currentIndex % 25000) {
            LOG.log(INFO, () -> MessageFormat.format("queued {0}/{1}: {2}", currentIndex, total, uri));
        }
        if (currentIndex > total) {
            LOG.log(WARNING, () -> MessageFormat.format("expected {0}, got {1}", total, currentIndex));
            LOG.warning("check your uri module!");
        }
    }
}
