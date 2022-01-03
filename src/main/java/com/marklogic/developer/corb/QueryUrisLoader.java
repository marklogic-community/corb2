/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XdmItem;

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

            session = csp.get().newSession();
            Request request;
            String urisModule = options.getUrisModule();
            if (isInlineOrAdhoc(urisModule)) {
                String adhocQuery;
                if (isInlineModule(urisModule)) {
                    adhocQuery = getInlineModuleCode(urisModule);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException("Unable to read inline module");
                    }
                    LOG.log(INFO, "Invoking inline {0}", URIS_MODULE);
                } else {
                    String queryPath = urisModule.substring(0, urisModule.indexOf('|'));
                    adhocQuery = AbstractManager.getAdhocQuery(queryPath);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException("Unable to read adhoc query " + queryPath + " from classpath or filesystem");
                    }
                    LOG.log(INFO, () -> MessageFormat.format("Invoking adhoc {0} {1}", URIS_MODULE, queryPath));
                }
                request = session.newAdhocQuery(adhocQuery);
                if (isJavaScriptModule(urisModule)) {
                    opts.setQueryLanguage("javascript");
                }
            } else {
                String root = options.getModuleRoot();
                String modulePath = buildModulePath(root, urisModule);
                LOG.log(INFO, () -> MessageFormat.format("Invoking {0} {1}", URIS_MODULE, modulePath));
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
            throw new CorbException("While invoking " + URIS_MODULE, exc);
        } finally {
            closeRequestAndSession();
        }
    }

    /**
     * Collect any custom inputs and then read the count of URIs to process
     * @param resultSequence
     * @throws CorbException
     */
    protected void preProcess(ResultSequence resultSequence) throws CorbException {
        ResultItem nextResultItem = collectCustomInputs(resultSequence);
        readTotalCount(nextResultItem);
    }

    /**
     * Read the count of URIs from the Sequence
     * @param resultItem
     * @throws CorbException
     */
    protected void readTotalCount(ResultItem resultItem) throws CorbException {
        try {
            XdmItem item = resultItem.getItem();
            if (ItemType.ARRAY_NODE.equals(item.getItemType())) {
                LOG.severe("First item is an Array. Use Sequence.from() to turn the Array into a Sequence.");
            }
            setTotalCount(Long.parseLong(item.asString()));
        } catch (NullPointerException | NumberFormatException exc) {
            throw new CorbException(URIS_MODULE + " " + options.getUrisModule() + " does not return total URI count");
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
        long lastMessageMillis = System.currentTimeMillis();
        long totalCount = getTotalCount();
        long uriIndex = 0;
        boolean redactUris = options.shouldRedactUris();
        String uri;
        String uriToLog;
        while (resultSequence != null && resultSequence.hasNext()) {
            uri = resultSequence.next().asString();
            if (isBlank(uri)) {
                continue;
            }
            uriToLog = redactUris ? "" : ": " + uri;
            if (queue.isEmpty()) {
                LOG.log(INFO, MessageFormat.format("Received first URI{0}", uriToLog));
            }
            //apply replacements (if any) - can be helpful in reducing in-memory footprint for ArrayQueue
            for (int j = 0; j < replacements.length - 1; j += 2) {
                uri = uri.replaceAll(replacements[j], replacements[j + 1]);
            }

            if (!queue.offer(uri)) { //put the uri into the queue
                LOG.log(SEVERE, MessageFormat.format("Unable to add URI {0} to queue. Received uris {1} which is more than expected {2}", uriToLog, uriIndex + 1, totalCount));
            } else if (uriIndex >= totalCount) {
                LOG.log(WARNING, MessageFormat.format("Received URI{0} at index {1} which is more than expected {2}", uriToLog, uriIndex + 1, totalCount));
            }

            uriIndex++;  //increment before logging the status with received count

            if (0 == uriIndex % 25000) {
                logQueueStatus(uriIndex, uriToLog, totalCount, lastMessageMillis);
                lastMessageMillis = System.currentTimeMillis(); //save this for next iteration
            }

            if (uriIndex > totalCount) {
                LOG.log(WARNING, MessageFormat.format("Expected {0}, got {1}", totalCount, uriIndex));
                LOG.log(WARNING, MessageFormat.format("Check your {0}!", URIS_MODULE));
            }
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
            long total = getTotalCount();
            if (total > Integer.MAX_VALUE) {
                LOG.log(WARNING, () -> MessageFormat.format("Total number of URIs {0, number} is greater than Array capacity. Enable {1}", total, Options.DISK_QUEUE));
            }
            uriQueue = new ArrayQueue<>(Math.toIntExact(total));
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
        super.close();
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

    protected void logQueueStatus(long currentIndex, String uri, long totalCount, long lastMessageMillis) {
        String uriToLog = options.shouldRedactUris() ? "" : uri;
        LOG.log(INFO, () -> MessageFormat.format("queued {0}/{1} {2}", currentIndex, totalCount, uriToLog));

        boolean slowReceive = System.currentTimeMillis() - lastMessageMillis > (1000 * 4);
        if (slowReceive) {
            LOG.log(WARNING, () -> "Slow receive! Consider increasing max heap size and using -XX:+UseG1GC, or using DISK-QUEUE option to limit memory demands.");
        }

        double megabytes = 1024d * 1024d;
        long freeMemory = Runtime.getRuntime().freeMemory();
        if (slowReceive || freeMemory < (16 * megabytes)) {
            LOG.log(WARNING, () -> MessageFormat.format("free memory: {0} MiB", freeMemory / megabytes));
        }
    }
}
