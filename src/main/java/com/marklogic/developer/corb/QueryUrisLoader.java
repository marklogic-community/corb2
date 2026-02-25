/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
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

/**
 * Loads URIs by executing a query module in MarkLogic.
 * <p>
 * QueryUrisLoader executes the configured {@link Options#URIS_MODULE} to retrieve
 * the list of URIs to process. The module can be:
 * </p>
 * <ul>
 * <li>An XQuery module (.xqy file)</li>
 * <li>A JavaScript module (.sjs file)</li>
 * <li>An inline module (INLINE-XQUERY|code or INLINE-JAVASCRIPT|code)</li>
 * <li>An adhoc query (path/to/file.xqy|ADHOC or path/to/file.sjs|ADHOC)</li>
 * </ul>
 * <p>
 * The URIS-MODULE is expected to return a sequence of items in the following order:
 * </p>
 * <ol>
 * <li>Optional: Custom input parameters (key=value pairs) - max 10 by default</li>
 * <li>Optional: Batch reference string (URIS_BATCH_REF)</li>
 * <li>Required: Total count of URIs (as a number)</li>
 * <li>Required: The URIs to process (as strings)</li>
 * </ol>
 * <p>
 * Custom inputs can be returned by the URIS-MODULE to pass configuration to
 * other modules. These must be in the format:
 * {@code MODULE-NAME.VARIABLE-NAME=value}
 * </p>
 * <p>
 * Example XQuery module:
 * </p>
 * <pre>{@code
 * xquery version "1.0-ml";
 * declare variable $URIS as xs:string external;
 *
 * (: Optional: Return custom inputs :)
 * "PROCESS-MODULE.myVar=someValue",
 *
 * (: Optional: Return batch reference :)
 * "batch-" || fn:current-dateTime(),
 *
 * (: Required: Return total count :)
 * xdmp:estimate(cts:search(fn:collection($URIS), cts:and-query(()))),
 *
 * (: Required: Return URIs :)
 * cts:uris((), (), cts:collection-query($URIS))
 * }</pre>
 * <p>
 * The loader uses XCC to submit the query and stream results. Results are
 * stored in an in-memory queue (ArrayQueue) or disk-backed queue (DiskQueue)
 * depending on configuration.
 * </p>
 * <p>
 * Configuration options:
 * </p>
 * <ul>
 * <li>{@link Options#URIS_MODULE} - Path to the query module (required)</li>
 * <li>{@link Options#MODULE_ROOT} - Root directory for modules</li>
 * <li>{@link Options#URIS_REPLACE_PATTERN} - Patterns to apply to URIs for memory optimization</li>
 * <li>{@link Options#DISK_QUEUE} - Whether to use disk-backed queue</li>
 * <li>{@link Options#MAX_OPTS_FROM_MODULE} - Maximum custom inputs to read (default: 10)</li>
 * </ul>
 *
 * @author MarkLogic Corporation
 * @see AbstractUrisLoader
 * @see ArrayQueue
 * @see DiskQueue
 * @see Options#URIS_MODULE
 */
public class QueryUrisLoader extends AbstractUrisLoader {

    /** Default maximum number of custom input options that can be returned by the URIS-MODULE before the total count */
    private static final int DEFAULT_MAX_OPTS_FROM_MODULE = 10;
    /** Pattern to match custom input properties returned by URIS-MODULE in the format MODULE-NAME.VARIABLE-NAME=value */
    private static final Pattern MODULE_CUSTOM_INPUT = Pattern.compile('('
            + PRE_BATCH_MODULE + '|' + PROCESS_MODULE + '|' + XQUERY_MODULE + '|' + POST_BATCH_MODULE
            + ")\\.[A-Za-z0-9_-]+=.*");
    /** Queue holding URIs to be processed; may be an in-memory ArrayQueue or disk-backed DiskQueue depending on configuration */
    private Queue<String> queue;

    /** XCC session used to execute the URIS-MODULE query against MarkLogic */
    protected Session session;
    /** Result sequence containing URIs and metadata returned by the URIS-MODULE query; streamed to avoid loading all URIs into memory at once */
    protected ResultSequence resultSequence;

    /** Logger for this class */
    private static final Logger LOG = Logger.getLogger(QueryUrisLoader.class.getName());

    /**
     * Opens the loader by executing the URIS-MODULE and initializing the URI queue.
     * <p>
     * The initialization process:
     * </p>
     * <ol>
     * <li>Parses URI replacement patterns (if configured)</li>
     * <li>Creates an XCC session</li>
     * <li>Prepares the URIS-MODULE request (module invoke or adhoc query)</li>
     * <li>Sets request variables (URIS, TYPE, PATTERN) and custom inputs</li>
     * <li>Executes the query</li>
     * <li>Preprocesses results (collects custom inputs, reads total count)</li>
     * <li>Creates and populates the URI queue</li>
     * </ol>
     * <p>
     * The method configures XCC request options to disable result caching and
     * buffering for efficient streaming of large result sets.
     * </p>
     *
     * @throws CorbException if the URIS-MODULE cannot be executed or returns invalid results
     */
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
     * Preprocesses the result sequence before populating the queue.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Collects any custom input parameters from the beginning of the result sequence</li>
     * <li>Reads the total URI count from the next result item</li>
     * </ol>
     *
     * @param resultSequence the result sequence from URIS-MODULE execution
     * @throws CorbException if the total count cannot be read or is invalid
     */
    protected void preProcess(ResultSequence resultSequence) throws CorbException {
        ResultItem nextResultItem = collectCustomInputs(resultSequence);
        readTotalCount(nextResultItem);
    }

    /**
     * Reads the total URI count from the result item.
     * <p>
     * The count must be a numeric value. If the URIS-MODULE returns an array as the
     * first item (after custom inputs), a severe error is logged instructing the
     * user to use Sequence.from() to convert the array to a sequence.
     * </p>
     *
     * @param resultItem the result item containing the total count
     * @throws CorbException if the count is missing, null, or not a valid number
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
     * Collects custom input options and batch reference from the result sequence.
     * <p>
     * This method reads up to {@link #getMaxOptionsFromModule()} items from the
     * beginning of the result sequence, looking for:
     * </p>
     * <ul>
     * <li>Custom input properties (matching {@link #MODULE_CUSTOM_INPUT} pattern)</li>
     * <li>Batch reference string (URIS_BATCH_REF)</li>
     * </ul>
     * <p>
     * Custom inputs are added to the properties for use by other modules.
     * The batch reference is stored separately via {@link #setBatchRef(String)}.
     * </p>
     * <p>
     * Reading stops when:
     * </p>
     * <ul>
     * <li>A numeric value is encountered (the total count)</li>
     * <li>A batch reference is found</li>
     * <li>The maximum number of options have been read</li>
     * </ul>
     *
     * @param resultSequence the result sequence from URIS-MODULE
     * @return the next result item to process (should be the total count)
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
     * Sets custom input variables on the URIS-MODULE request.
     * <p>
     * Collects all properties (from properties file and System properties) that
     * start with {@code URIS-MODULE.} and sets them as external string variables
     * on the request.
     * </p>
     * <p>
     * For example, a property {@code URIS-MODULE.myVar=value} will be set as
     * an external variable {@code $myVar} in the query with the value {@code "value"}.
     * </p>
     * <p>
     * System properties take precedence over properties file values.
     * </p>
     *
     * @param request the XCC request to set variables on
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
     * Creates a new queue and populates it with URIs from the result sequence.
     * <p>
     * This is a convenience method that combines {@link #createQueue()} and
     * {@link #populateQueue(Queue, ResultSequence)}.
     * </p>
     *
     * @param resultSequence the result sequence containing URIs
     * @return a populated queue of URIs
     */
    protected Queue<String> createAndPopulateQueue(ResultSequence resultSequence) {
        Queue<String> uriQueue = createQueue();
        return populateQueue(uriQueue, resultSequence);
    }

    /**
     * Populates the queue with URIs from the result sequence.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Iterates through all remaining items in the result sequence</li>
     * <li>Skips blank URIs</li>
     * <li>Applies URI replacement patterns (if configured)</li>
     * <li>Adds each URI to the queue</li>
     * <li>Logs progress every 25,000 URIs</li>
     * <li>Warns if more URIs are received than expected</li>
     * </ol>
     * <p>
     * Progress messages include memory usage warnings if:
     * </p>
     * <ul>
     * <li>Receiving URIs is slow (more than 4 seconds between batches)</li>
     * <li>Free memory drops below 16 MB</li>
     * </ul>
     *
     * @param queue the queue to populate
     * @param resultSequence the result sequence containing URIs
     * @return the populated queue
     */
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
     * Factory method that creates an appropriate queue based on configuration.
     * <p>
     * Creates either:
     * </p>
     * <ul>
     * <li>{@link DiskQueue} - If {@link Options#DISK_QUEUE} is enabled</li>
     * <li>{@link ArrayQueue} - Otherwise (in-memory queue)</li>
     * </ul>
     * <p>
     * If the total URI count exceeds {@link Integer#MAX_VALUE} and disk queue is not
     * enabled, a warning is logged recommending enabling DISK-QUEUE.
     * </p>
     *
     * @return a new queue instance
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

    /**
     * Checks if more URIs are available in the queue.
     *
     * @return true if the queue has more URIs, false otherwise
     * @throws CorbException if an error occurs
     */
    @Override
    public boolean hasNext() throws CorbException {
        return queue != null && !queue.isEmpty();
    }

    /**
     * Returns the next URI from the queue.
     *
     * @return the next URI to process
     * @throws CorbException if an error occurs
     * @throws NoSuchElementException if the queue is null or empty
     */
    @Override
    public String next() throws CorbException {
        if (queue == null) {
            throw new NoSuchElementException();
        }
        return queue.remove();
    }

    /**
     * Closes the loader and releases resources.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Closes the parent class resources</li>
     * <li>Closes the result sequence and XCC session</li>
     * <li>Clears and nulls the queue</li>
     * <li>Performs cleanup operations</li>
     * </ol>
     */
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

    /**
     * Closes the result sequence and XCC session.
     * <p>
     * This method is called multiple times to ensure resources are released
     * even if errors occur during open().
     * </p>
     */
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

    /**
     * Gets the maximum number of custom options to read from the URIS-MODULE.
     * <p>
     * The value is configured via {@link Options#MAX_OPTS_FROM_MODULE}.
     * If not configured or invalid, returns {@link #DEFAULT_MAX_OPTS_FROM_MODULE} (10).
     * </p>
     *
     * @return the maximum number of custom options to read
     */
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

    /**
     * Logs the current queue status including progress and memory information.
     * <p>
     * Logs warnings if:
     * </p>
     * <ul>
     * <li>Receiving URIs is slow (suggests increasing heap or using DISK-QUEUE)</li>
     * <li>Free memory is low (below 16 MB)</li>
     * </ul>
     *
     * @param currentIndex the current URI index
     * @param uri the current URI (for logging, may be redacted)
     * @param totalCount the total expected URI count
     * @param lastMessageMillis timestamp of the last status message
     */
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
