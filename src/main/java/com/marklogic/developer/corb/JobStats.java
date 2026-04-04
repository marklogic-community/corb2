/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import com.marklogic.developer.corb.util.XmlUtils;
import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import org.w3c.dom.*;

import com.marklogic.developer.corb.util.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Collects, tracks, and reports comprehensive statistics for CoRB jobs.
 * <p>
 * This class provides:
 * </p>
 * <ul>
 * <li>Real-time job execution metrics (task counts, timing, throughput)</li>
 * <li>Performance statistics (TPS, average transaction time, ETC)</li>
 * <li>Job status tracking (paused, running, completed)</li>
 * <li>XML and JSON serialization of metrics</li>
 * <li>Integration with MarkLogic server for metrics logging and persistence</li>
 * </ul>
 * <p>
 * Statistics can be output in either concise or detailed format, with optional
 * inclusion of long-running and failed URIs. Metrics can be logged to the console,
 * MarkLogic server logs, or persisted to a database via a custom metrics module.
 * </p>
 *
 * @see BaseMonitor
 * @see Manager
 * @see JobServer
  * @since 2.4.0
 */
public class JobStats extends BaseMonitor {

    /**
     * Placeholder value used when a metric or parameter is not available or applicable.
     */
    private static final String NOT_APPLICABLE = "NA";

    /**
     * Minimum interval (in milliseconds) between TPS and ETC calculations.
     * Set to 10 seconds to avoid excessive computation for frequent status requests.
     */
    private static final long TPS_ETC_MIN_REFRESH_INTERVAL = 10000L;

    /**
     * Parameter name for collections when invoking the metrics module.
     */
    private static final String METRICS_COLLECTIONS_PARAM = "collections";

    /**
     * Parameter name for the metrics document string when invoking the metrics module.
     */
    private static final String METRICS_DOCUMENT_STR_PARAM = "metricsDocumentStr";

    /**
     * Parameter name for the database name when invoking the metrics module.
     */
    private static final String METRICS_DB_NAME_PARAM = "dbName";

    /**
     * Parameter name for the URI root when invoking the metrics module.
     */
    private static final String METRICS_URI_ROOT_PARAM = "uriRoot";

    /**
     * XQuery version declaration for MarkLogic queries.
     */
    protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\";\n";

    /**
     * Format string for xdmp:log() function calls.
     * Parameters: (1) message, (2) log level.
     */
    private static final String XDMP_LOG_FORMAT = "xdmp:log('%1$s','%2$s')";

    /**
     * Date/time formatter for ISO 8601 format timestamps.
     */
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    /**
     * XML element/field name for job start time.
     */
    private static final String START_TIME = "startTime";

    /**
     * XML element/field name for URI.
     */
    private static final String URI = "uri";

    /**
     * XML element/field name for job ID.
     */
    private static final String JOB_ID = "id";

    /**
     * XML element/field name for job name.
     */
    private static final String JOB_NAME = "name";

    /**
     * XML root element name for a single job's statistics.
     */
    public static final String JOB_ELEMENT = "job";

    /**
     * XML root element name for multiple jobs' statistics.
     */
    public static final String JOBS_ELEMENT = "jobs";
    /**
     * XML namespace for CoRB metrics documents.
     */
    public static final String CORB_NAMESPACE = "http://developer.marklogic.com/code/corb";
    /**
     * XML element/field name for long-running URIs (slow transactions).
     */
    private static final String LONG_RUNNING_URIS = "slowTransactions";

    /**
     * XML element/field name for failed URIs.
     */
    private static final String FAILED_URIS = "failedTransactions";

    /**
     * XML element/field name for URIs load time.
     */
    private static final String URIS_LOAD_TIME = "urisLoadTimeInMillis";

    /**
     * XML element/field name for initialization task time.
     */
    private static final String INIT_TASK_TIME = "initTaskTimeInMillis";

    /**
     * XML element/field name for pre-batch task run time.
     */
    private static final String PRE_BATCH_RUN_TIME = "preBatchRunTimeInMillis";

    /**
     * XML element/field name for post-batch task run time.
     */
    private static final String POST_BATCH_RUN_TIME = "postBatchRunTimeInMillis";

    /**
     * XML element/field name for total job run time.
     */
    private static final String TOTAL_JOB_RUN_TIME = "totalRunTimeInMillis";

    /**
     * XML element/field name for average transaction time.
     */
    private static final String AVERAGE_TRANSACTION_TIME = "averageTransactionTimeInMillis";

    /**
     * XML element/field name for total number of tasks.
     */
    private static final String TOTAL_NUMBER_OF_TASKS = "totalNumberOfTasks";

    /**
     * XML element/field name for number of failed tasks.
     */
    private static final String NUMBER_OF_FAILED_TASKS = "numberOfFailedTasks";

    /**
     * XML element/field name for number of succeeded tasks.
     */
    private static final String NUMBER_OF_SUCCEEDED_TASKS = "numberOfSucceededTasks";

    /**
     * XML element/field name for the metrics document URI.
     */
    private static final String METRICS_DOC_URI = "metricsDocUri";

    /**
     * XML element/field name for paused status.
     */
    private static final String PAUSED = "paused";

    /**
     * XML element/field name for average transactions per second.
     */
    private static final String AVERAGE_TPS = "averageTransactionsPerSecond";

    /**
     * XML element/field name for current transactions per second.
     */
    private static final String CURRENT_TPS = "currentTransactionsPerSecond";

    /**
     * XML element/field name for estimated time of completion.
     */
    private static final String ESTIMATED_TIME_OF_COMPLETION = "estimatedTimeOfCompletion";

    /**
     * XML element/field name for metrics timestamp.
     */
    private static final String METRICS_TIMESTAMP = "timestamp";

    /**
     * XML element/field name for host IP address.
     */
    private static final String HOST = "host";

    /**
     * XML element/field name for job end time.
     */
    private static final String END_TIME = "endTime";

    /**
     * XML element/field name for user-provided options.
     */
    private static final String USER_PROVIDED_OPTIONS = "userProvidedOptions";

    /**
     * XML element/field name for job run location (directory).
     */
    private static final String JOB_LOCATION = "runLocation";

    /**
     * XML element/field name for current thread count.
     */
    private static final String CURRENT_THREAD_COUNT = "currentThreadCount";

    /**
     * XML element/field name for job server port.
     */
    private static final String JOB_SERVER_PORT = "port";

    /**
     * Map of user-provided options/properties from the Manager.
     * These are the configuration options explicitly set by the user.
     */
    private Map<String, String> userProvidedOptions = new HashMap<>();

    /**
     * Job start time as a formatted ISO 8601 date string.
     */
    private String startTime = null;

    /**
     * Job end time as a formatted ISO 8601 date string.
     */
    private String endTime = null;

    /**
     * IP address of the host running the job.
     */
    private String host = null;

    /**
     * Total count of failed task executions.
     */
    private Long numberOfFailedTasks = 0L;

    /**
     * Total count of successful task executions.
     */
    private Long numberOfSucceededTasks = 0L;

    /**
     * Average time per task execution in milliseconds.
     */
    private Double averageTransactionTime = 0.0d;

    /**
     * Time spent loading URIs in milliseconds.
     * Value of -1 indicates not yet measured or not applicable.
     */
    private Long urisLoadTime = -1L;

    /**
     * Pre-batch task execution time in milliseconds.
     * Value of -1 indicates not yet measured or not applicable.
     */
    private Long preBatchRunTime = -1L;

    /**
     * Post-batch task execution time in milliseconds.
     * Value of -1 indicates not yet measured or not applicable.
     */
    private Long postBatchRunTime = -1L;

    /**
     * Initialization task execution time in milliseconds.
     * Value of -1 indicates not yet measured or not applicable.
     */
    private Long initTaskRunTime = -1L;

    /**
     * Total job run time in milliseconds from start to end.
     * Value of -1 indicates job is still running or not started.
     */
    private Long totalRunTimeInMillis = -1L;

    /**
     * File system path where the job is being executed.
     * Typically the user's working directory.
     */
    private String jobRunLocation = null;

    /**
     * Unique identifier for this job.
     * Generated by the Manager during job initialization.
     */
    private String jobId = null;

    /**
     * Human-readable name for this job.
     * Configured via {@link TransformOptions#getJobName()}.
     */
    private String jobName = null;

    /**
     * Map of long-running URIs and their execution times.
     * Key: URI, Value: execution time in milliseconds.
     */
    private Map<String, Long> longRunningUris = new HashMap<>();

    /**
     * List of URIs that failed during execution.
     * Limited to a configured maximum size.
     */
    private List<String> failedUris = null;

    /**
     * URI where the metrics document was persisted in the database.
     * Set after successful execution of the metrics module.
     */
    private String uri = null;

    /**
     * Flag indicating whether the job is currently paused.
     */
    private boolean paused;

    /**
     * Current number of active threads processing tasks.
     * Set to 0 when the job completes.
     */
    private Long currentThreadCount = 0L;

    /**
     * Port number of the job server (if enabled).
     * Value of -1 indicates job server is not running.
     */
    private Long jobServerPort = -1L;

    /**
     * Pool of MarkLogic content sources for executing queries and modules.
     */
    private ContentSourcePool contentSourcePool;

    /**
     * Transform options containing job configuration.
     */
    private TransformOptions options;

    /**
     * Factory for creating XML document builders.
     */
    protected final DocumentBuilderFactory documentBuilderFactory = XmlUtils.newSecureDocumentBuilderFactoryInstance();

    /**
     * Factory for creating XSLT transformers.
     */
    private final TransformerFactory transformerFactory = XmlUtils.newSecureTransformerFactoryInstance();

    /**
     * Compiled XSLT templates for transforming job statistics XML to JSON.
     * Lazily initialized on first use.
     */
    private Templates jobStatsToJsonTemplates;

    /**
     * Logger for this class.
     */
    private static final Logger LOG = Logger.getLogger(JobStats.class.getName());

    /**
     * Lock object for synchronizing access to statistics during refresh operations.
     */
    private final Object lock = new Object();

    /**
     * Constructs a JobStats instance for the specified Manager.
     *
     * @param manager the Manager whose statistics will be tracked
     */
    public JobStats(Manager manager) {
        super(manager);
        options = manager.getOptions();
        contentSourcePool = manager.getContentSourcePool();
        host = getHost();
        jobRunLocation = System.getProperty("user.dir");
        userProvidedOptions = manager.getUserProvidedOptions();
    }

    /**
     * Retrieves the IP address of the local host.
     *
     * @return the host IP address, or "Unknown" if it cannot be determined
     */
    protected String getHost() {
        String hostName = "Unknown";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            hostName = addr.getHostAddress();
        } catch (UnknownHostException ex) {
            try {
                hostName = InetAddress.getLoopbackAddress().getHostAddress();
            } catch (Exception e) {
                LOG.log(INFO, "Host address can not be resolved", e);
            }
        }
        return hostName;
    }

    /**
     * Refreshes all statistics by retrieving current values from the Manager and Monitor.
     * <p>
     * This method is thread-safe and synchronizes on an internal lock.
     * </p>
     */
    private void refresh() {
        synchronized (lock) {
            if (manager != null) {
                jobId = manager.getJobId();
                paused = manager.isPaused();
                startTime = epochMillisAsFormattedDateString(manager.getStartMillis());
                refreshOptions(options);
                Monitor monitor = manager.getMonitor();
                refreshMonitorStats(monitor);
            }
        }
    }

    /**
     * Updates job configuration-related statistics from the TransformOptions.
     *
     * @param options the TransformOptions to retrieve values from
     */
    protected void refreshOptions(TransformOptions options){
        if (options != null) {
            jobName = options.getJobName();
            jobServerPort = options.getJobServerPort().longValue();
            currentThreadCount = (long) options.getThreadCount();
        }
    }

    /**
     * Updates execution statistics from the Monitor.
     * <p>
     * Calculates timing metrics, TPS, and estimated completion time based on
     * current job state (running vs. completed).
     * </p>
     *
     * @param monitor the Monitor to retrieve statistics from
     */
    protected void refreshMonitorStats(Monitor monitor){
        if (monitor != null) {

            taskCount = monitor.getTaskCount();
            if (taskCount > 0) { //job has selected URIs to process

                refreshThreadPoolExecutorStats(monitor.getThreadPoolExecutor());

                long currentTimeMillis = System.currentTimeMillis();
                long totalTime = manager.getEndMillis() - manager.getStartMillis();
                if (totalTime > 0) { //job has completed
                    currentThreadCount = 0L;
                    totalRunTimeInMillis = totalTime;
                    long totalTransformTime = currentTimeMillis - manager.getTransformStartMillis();
                    averageTransactionTime = getAverageTransactionTime(totalTransformTime, numberOfFailedTasks, numberOfSucceededTasks);
                    endTime = epochMillisAsFormattedDateString(manager.getEndMillis());
                    estimatedTimeOfCompletion = null;
                } else { //still running, update the stats
                    totalRunTimeInMillis = currentTimeMillis - manager.getStartMillis();
                    long timeSinceLastReq = currentTimeMillis - prevMillis;
                    //refresh it every 10 seconds or more.. ignore more frequent requests
                    if (timeSinceLastReq > TPS_ETC_MIN_REFRESH_INTERVAL) {
                        long completed = numberOfSucceededTasks + numberOfFailedTasks;
                        populateTps(completed);
                    }
                }
            }
        }
    }

    /**
     * Calculates the average transaction time.
     *
     * @param totalTransformTime total time spent processing all tasks
     * @param numberOfFailedTasks count of failed tasks
     * @param numberOfSucceededTasks count of successful tasks
     * @return average time per task in milliseconds, or 0 if no tasks completed
     */
    protected double getAverageTransactionTime(long totalTransformTime, long numberOfFailedTasks, long numberOfSucceededTasks) {
        long completedTasks = numberOfFailedTasks + numberOfSucceededTasks;
        if (completedTasks > 0) {
            return totalTransformTime / (double) completedTasks;
        } else {
            return 0d;
        }
    }

    /**
     * Updates task execution statistics from the thread pool executor.
     *
     * @param threadPool the executor to retrieve statistics from
     */
    protected void refreshThreadPoolExecutorStats(PausableThreadPoolExecutor threadPool) {
        if (threadPool != null) {
            longRunningUris = threadPool.getTopUris();
            failedUris = threadPool.getFailedUris();
            numberOfFailedTasks = threadPool.getNumFailedUris();
            numberOfSucceededTasks = threadPool.getNumSucceededUris();
        }
    }

    /**
     * Converts epoch milliseconds to a formatted date string.
     *
     * @param epochMillis the timestamp in milliseconds since epoch
     * @return formatted date string in ISO 8601 format
     */
    protected static String epochMillisAsFormattedDateString(long epochMillis) {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        return date.format(DATE_FORMATTER);
    }

    /**
     * Logs job metrics to various destinations.
     * <p>
     * Metrics can be logged to:
     * </p>
     * <ul>
     * <li>Console (if logToConsole is true)</li>
     * <li>MarkLogic server logs</li>
     * <li>MarkLogic database (via metrics module)</li>
     * </ul>
     *
     * @param message optional message to include with metrics
     * @param concise whether to use concise output format
     * @param logToConsole whether to log to console
     */
    public void logMetrics(String message, boolean concise, boolean logToConsole) {
        String logLevel = options.getLogMetricsToServerLog();
        if (options.isMetricsLoggingEnabled(logLevel)) {
            String processModule = options.getMetricsModule();
            Document doc = toXML(concise);
            String metricsLogMessage = toJSON(doc);
            if (logToConsole) {
                LOG.info(metricsLogMessage);
            }

            String metricsDocument;
            if (isJavaScriptModule(processModule)) {
                metricsDocument = metricsLogMessage;
            } else {
                metricsDocument = XmlUtils.documentToString(doc);
            }

            executeModule(metricsDocument);
            logToServer(message, metricsLogMessage);
        }
    }

    /**
     * Logs metrics to the MarkLogic server log.
     *
     * @param message optional message to include
     * @param metrics the metrics to log
     */
    protected void logToServer(String message, String metrics) {
        if (contentSourcePool != null) {
            try {
                ContentSource contentSource = contentSourcePool.get();
                if (contentSource != null) {
                    logToServer(contentSource, message, metrics);
                } else {
                    LOG.log(WARNING, "Unable to log to server, no content source available");
                }
            } catch (CorbException | RequestException ex) {
                LOG.log(SEVERE, "logToServer request failed", ex);
            }
        }
    }

    /**
     * Logs metrics to the MarkLogic server log using the specified content source.
     *
     * @param contentSource the MarkLogic content source
     * @param message optional message to include
     * @param metrics the metrics to log
     * @throws RequestException if the logging request fails
     */
    protected void logToServer(ContentSource contentSource, String message, String metrics) throws RequestException {
        String logLevel = options.getLogMetricsToServerLog();
        if (options.isMetricsLoggingEnabled(logLevel)) {
            try (Session session = contentSource.newSession()) {
                String xquery = XQUERY_VERSION_ML
                    + (message != null
                    ? String.format(XDMP_LOG_FORMAT, message, logLevel.toLowerCase()) + ','
                    : "")
                    + String.format(XDMP_LOG_FORMAT, metrics, logLevel.toLowerCase());
                AdhocQuery query = session.newAdhocQuery(xquery);
                session.submitRequest(query);
            }
        }
    }

    /**
     * Executes the metrics module to persist metrics to the database.
     *
     * @param metrics the metrics document to pass to the module
     */
    protected void executeModule(String metrics) {
        String metricsDatabase = options.getMetricsDatabase();
        if (metricsDatabase != null && contentSourcePool != null) {
            try {
                ContentSource contentSource = contentSourcePool.get();
                if (contentSource != null) {
                    executeModule(contentSource, metricsDatabase, metrics);
                } else {
                    LOG.log(WARNING, "Unable to execute metrics module, no content source available");
                }
            } catch (CorbException | RequestException ex) {
                LOG.log(SEVERE, "logJobStatsToServerDocument request failed", ex);
            }
        }
    }

    /**
     * Executes the metrics module using the specified content source and database.
     * <p>
     * The module receives parameters for database name, URI root, collections,
     * and the metrics document. The module should return the URI where the
     * metrics were persisted.
     * </p>
     *
     * @param contentSource the MarkLogic content source
     * @param metricsDatabase the target database name
     * @param metrics the metrics document to persist
     * @throws RequestException if the module execution fails
     */
    protected void executeModule(ContentSource contentSource, String metricsDatabase, String metrics) throws RequestException {
        String uriRoot = options.getMetricsRoot();
        String collections = options.getMetricsCollections();
        String processModule = options.getMetricsModule();

        ResultSequence seq = null;

        try (Session session = contentSource.newSession()) {
            Request request = manager.getRequestForModule(processModule, session);
            request.setNewStringVariable(METRICS_DB_NAME_PARAM, metricsDatabase);
            request.setNewStringVariable(METRICS_URI_ROOT_PARAM, uriRoot != null ? uriRoot : NOT_APPLICABLE);
            request.setNewStringVariable(METRICS_COLLECTIONS_PARAM, collections != null ? collections : NOT_APPLICABLE);

            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setCacheResult(false);
            if (isJavaScriptModule(processModule)) {
                requestOptions.setQueryLanguage("javascript");
                request.setNewStringVariable(METRICS_DOCUMENT_STR_PARAM, metrics == null ? toJSON() : metrics);
            } else {
                request.setNewStringVariable(METRICS_DOCUMENT_STR_PARAM, metrics == null ? toXmlString() : metrics);
            }
            request.setOptions(requestOptions);

            seq = session.submitRequest(request);
            ResultItem uri = seq != null && seq.hasNext() ? seq.next() : null;
            if (uri != null) {
                this.uri = uri.asString();
            }
        } finally {
            if (null != seq && !seq.isClosed()) {
                seq.close();
            }
        }
    }

    /**
     * Returns a concise JSON representation of job statistics.
     *
     * @return JSON string of metrics
     */
    @Override
    public String toString() {
        return toString(true);
    }

    /**
     * Returns a JSON representation of job statistics.
     *
     * @param concise whether to use concise output format
     * @return JSON string of metrics
     */
    public String toString(boolean concise) {
        return toJSON(concise);
    }

    /**
     * Returns a detailed XML representation of job statistics.
     *
     * @return XML string of metrics
     */
    public String toXmlString() {
        return toXmlString(false);
    }

    /**
     * Returns an XML representation of job statistics.
     *
     * @param concise whether to use concise output format
     * @return XML string of metrics
     */
    public String toXmlString(boolean concise) {
        Document doc = toXML(concise);
        return XmlUtils.documentToString(doc);
    }

    /**
     * Creates an XML document containing statistics for multiple jobs.
     *
     * @param documentBuilderFactory factory for creating XML documents
     * @param jobStatsList list of JobStats instances to include
     * @param concise whether to use concise output format
     * @return XML Document containing all job statistics, or null on error
     */
    public static Document toXML(DocumentBuilderFactory documentBuilderFactory, List<JobStats> jobStatsList, boolean concise) {

        try {
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document doc = documentBuilder.newDocument();
            Element corbJobsElement = doc.createElementNS(CORB_NAMESPACE, JOBS_ELEMENT);
            jobStatsList.stream()
                .filter(Objects::nonNull)
                .forEach(jobStats -> corbJobsElement.appendChild(jobStats.createJobElement(doc, concise)));
            doc.appendChild(corbJobsElement);
            return doc;
        } catch (ParserConfigurationException ex) {
            LOG.log(SEVERE, "Unable to create a new XML Document", ex);
        }
        return null;
    }

    /**
     * Creates an XML document containing job statistics.
     *
     * @param concise whether to use concise output format
     * @return XML Document containing job statistics, or null on error
     */
    public Document toXML(boolean concise) {
        refresh();
        Document doc = null;
        try {
            DocumentBuilder docBuilder = documentBuilderFactory.newDocumentBuilder();
            doc = docBuilder.newDocument();
            Element jobElement = createJobElement(doc, concise);
            doc.appendChild(jobElement);
        } catch (ParserConfigurationException ex) {
            LOG.log(SEVERE, "Unable to create a new XML Document", ex);
        }
        return doc;
    }

    /**
     * Creates an XML element containing all job statistics.
     *
     * @param doc the parent XML document
     * @param concise whether to use concise output format (excludes user options, URIs)
     * @return the job Element with all statistics as child elements
     */
    protected Element createJobElement(Document doc, boolean concise) {
        Element element = doc.createElementNS(CORB_NAMESPACE, JOB_ELEMENT);

        createAndAppendElement(element, METRICS_TIMESTAMP, LocalDateTime.now().format(DATE_FORMATTER));
        createAndAppendElement(element, METRICS_DOC_URI, uri);
        createAndAppendElement(element, JOB_LOCATION, jobRunLocation);
        createAndAppendElement(element, JOB_NAME, jobName);
        createAndAppendElement(element, JOB_ID, jobId);
        if (!concise) {
            createAndAppendElement(element, USER_PROVIDED_OPTIONS, userProvidedOptions);
        }
        createAndAppendElement(element, HOST, host);
        createAndAppendElement(element, JOB_SERVER_PORT, jobServerPort);

        createAndAppendElement(element, START_TIME, startTime);
        createAndAppendElement(element, INIT_TASK_TIME, initTaskRunTime);
        createAndAppendElement(element, PRE_BATCH_RUN_TIME, preBatchRunTime);
        createAndAppendElement(element, URIS_LOAD_TIME, urisLoadTime);
        createAndAppendElement(element, POST_BATCH_RUN_TIME, postBatchRunTime);
        createAndAppendElement(element, END_TIME, endTime);
        createAndAppendElement(element, TOTAL_JOB_RUN_TIME, totalRunTimeInMillis);

        createAndAppendElement(element, PAUSED, Boolean.toString(paused));
        createAndAppendElement(element, TOTAL_NUMBER_OF_TASKS, taskCount);
        createAndAppendElement(element, CURRENT_THREAD_COUNT, currentThreadCount);
        createAndAppendElement(element, CURRENT_TPS, currentTps > 0 ? formatTransactionsPerSecond(currentTps, false) : "");
        createAndAppendElement(element, AVERAGE_TPS, avgTps > 0 ? formatTransactionsPerSecond(avgTps, false) : "");
        createAndAppendElement(element, AVERAGE_TRANSACTION_TIME, averageTransactionTime);
        createAndAppendElement(element, ESTIMATED_TIME_OF_COMPLETION, estimatedTimeOfCompletion);

        createAndAppendElement(element, NUMBER_OF_SUCCEEDED_TASKS, numberOfSucceededTasks);
        createAndAppendElement(element, NUMBER_OF_FAILED_TASKS, numberOfFailedTasks);

        if (manager.exitCode != null) {
            createAndAppendElement(element, "exitCode", manager.getExitCode());
        }
        if (!concise && !options.shouldRedactUris()) {
            addLongRunningUris(element);
            addFailedUris(element);
        }
        return element;
    }

    /**
     * Creates and appends an XML element with a text value if the value is not empty.
     *
     * @param parent the parent node
     * @param localName the element name
     * @param value the text value
     */
    protected void createAndAppendElement(Node parent, String localName, String value) {
        if (StringUtils.isNotEmpty(value)) {
            Element element = createElement(parent, localName, value);
            parent.appendChild(element);
        }
    }

    /**
     * Creates and appends an XML element with an Integer value if non-negative.
     *
     * @param parent the parent node
     * @param localName the element name
     * @param value the Integer value
     */
    protected void createAndAppendElement(Node parent, String localName, Integer value) {
        if (value != null && value >= 0L) {
            createAndAppendElement(parent, localName, value.toString());
        }
    }

    /**
     * Creates and appends an XML element with a Long value if non-negative.
     *
     * @param parent the parent node
     * @param localName the element name
     * @param value the Long value
     */
    protected void createAndAppendElement(Node parent, String localName, Long value) {
        if (value != null && value >= 0L) {
            createAndAppendElement(parent, localName, value.toString());
        }
    }

    /**
     * Creates and appends an XML element with a Double value if non-negative.
     *
     * @param parent the parent node
     * @param localName the element name
     * @param value the Double value
     */
    protected void createAndAppendElement(Node parent, String localName, Double value) {
        if (value != null && value >= 0L) {
            createAndAppendElement(parent, localName, value.toString());
        }
    }

    /**
     * Creates and appends an XML element containing a map of key-value pairs.
     *
     * @param parent the parent node
     * @param localName the element name
     * @param value the map of string key-value pairs
     */
    protected void createAndAppendElement(Node parent, String localName, Map<String, String>value){
        if (value != null && !value.isEmpty()) {
            Document doc = parent.getOwnerDocument();
            Element element = doc.createElementNS(CORB_NAMESPACE, localName);
            for (Map.Entry<String, String> entry : value.entrySet()) {
                createAndAppendElement(element, entry.getKey(), entry.getValue());
            }
            parent.appendChild(element);
        }
    }

    /**
     * Creates an XML element with a text value.
     *
     * @param parent the parent node (used to get the document)
     * @param localName the element name
     * @param value the text value
     * @return the created Element
     */
    protected Element createElement(Node parent, String localName, String value) {
        Document doc = parent.getOwnerDocument();
        Element element = doc.createElementNS(CORB_NAMESPACE, localName);
        Text text = doc.createTextNode(value);
        element.appendChild(text);
        return element;
    }

    /**
     * Returns a detailed JSON representation of job statistics.
     *
     * @return JSON string of metrics
     */
    public String toJSON() {
        return toJSON(false);
    }

    /**
     * Returns a JSON representation of job statistics.
     *
     * @param concise whether to use concise output format
     * @return JSON string of metrics
     */
    public String toJSON(boolean concise) {
        Document doc = toXML(concise);
        return toJSON(doc);
    }

    /**
     * Transforms an XML document to JSON using XSLT templates.
     *
     * @param jobStatsToJsonTemplates the XSLT templates for transformation
     * @param doc the XML document to transform
     * @return JSON string representation
     * @throws TransformerException if the transformation fails
     */
    public static String toJSON(Templates jobStatsToJsonTemplates, Document doc) throws TransformerException {
        StringWriter writer = new StringWriter();
        Transformer autobot = jobStatsToJsonTemplates.newTransformer();
        autobot.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }

    /**
     * Transforms an XML document to JSON.
     *
     * @param doc the XML document to transform
     * @return JSON string representation, or empty string on error
     */
    public String toJSON(Document doc) {
        StringBuilder json = new StringBuilder();
        try {
            if (jobStatsToJsonTemplates == null) {
                jobStatsToJsonTemplates = newJobStatsToJsonTemplates(transformerFactory);
            }
            json.append(toJSON(jobStatsToJsonTemplates, doc));
        } catch (TransformerException e) {
            LOG.log(SEVERE, "Unable to transform to JSON", e);
        }
        return json.toString();
    }

    /**
     * Creates XSLT templates for transforming job statistics XML to JSON.
     *
     * @param transformerFactory the transformer factory
     * @return compiled XSLT Templates
     * @throws TransformerConfigurationException if the templates cannot be created
     */
    public static Templates newJobStatsToJsonTemplates(TransformerFactory transformerFactory) throws TransformerConfigurationException {
        return newTemplates(transformerFactory, "jobStatsToJson.xsl");
    }

    /**
     * Creates XSLT templates from a stylesheet file in the classpath.
     *
     * @param transformerFactory the transformer factory
     * @param stylesheetFilename the name of the stylesheet file
     * @return compiled XSLT Templates
     * @throws TransformerConfigurationException if the stylesheet cannot be loaded or compiled
     */
    protected static Templates newTemplates(TransformerFactory transformerFactory, String stylesheetFilename) throws TransformerConfigurationException {
        URL url = Manager.class.getResource( "/" + stylesheetFilename);
        if (url == null) {
            throw new TransformerConfigurationException("Could not find the template file " + stylesheetFilename + " in the classpath");
        }
        try {
            StreamSource source = new StreamSource(url.openStream());
            source.setSystemId(url.toURI().toString()); //required in order for XSLT to resolve relative paths and itself with document('')
            return transformerFactory.newTemplates(source);
        } catch (URISyntaxException | IOException ex) {
            throw new TransformerConfigurationException("Could not find the template file " + stylesheetFilename + " in the classpath", ex);
        }
    }

    /**
     * Adds an XML element containing long-running URIs with their execution times and ranks.
     *
     * @param parent the parent node to append to
     */
    protected void addLongRunningUris(Node parent) {

        if (longRunningUris != null && !longRunningUris.isEmpty()) {
            Document doc = parent.getOwnerDocument();
            Element rankingElement = doc.createElementNS(CORB_NAMESPACE, LONG_RUNNING_URIS);

            NavigableSet<Long> ranks = new TreeSet<>();
            ranks.addAll(longRunningUris.values());
            Map<Integer, List<Element>> rankToXML = new HashMap<>();
            int numUris = longRunningUris.keySet().size();
            for (Map.Entry<String, Long> entry : longRunningUris.entrySet()) {
                Long time = entry.getValue();
                Integer rank = numUris - ranks.headSet(time).size();
                List<Element> urisWithSameRank = rankToXML.get(rank);

                if (urisWithSameRank != null) {
                    urisWithSameRank.add(createElement(rankingElement, URI, entry.getKey()));
                } else {
                    List<Element> rankData = new ArrayList<>();
                    rankData.add(createElement(rankingElement, URI, entry.getKey()));
                    rankData.add(createElement(rankingElement, "rank", rank.toString()));
                    rankData.add(createElement(rankingElement, "timeInMillis", time.toString()));
                    urisWithSameRank = rankData;
                }
                rankToXML.put(rank, urisWithSameRank);
            }

            for (Map.Entry<Integer, List<Element>> entry : rankToXML.entrySet()){
                Element uriElement = doc.createElementNS(CORB_NAMESPACE, "Uri");
                for (Element element : entry.getValue()) {
                    uriElement.appendChild(element);
                }
                rankingElement.appendChild(uriElement);
            }
            parent.appendChild(rankingElement);
        }
    }

    /**
     * Adds an XML element containing the list of failed URIs.
     *
     * @param parent the parent node to append to
     */
    protected void addFailedUris(Node parent) {
        if (failedUris != null && !failedUris.isEmpty()) {
            Document doc = parent.getOwnerDocument();
            Element failedUrisElement = doc.createElementNS(CORB_NAMESPACE, FAILED_URIS);
            for (String nodeVal : failedUris) {
                createAndAppendElement(failedUrisElement, URI, nodeVal);
            }
            parent.appendChild(failedUrisElement);
        }
    }

    /**
     * Sets the initialization task run time.
     *
     * @param initTaskRunTime the run time in milliseconds
     */
    public void setInitTaskRunTime(Long initTaskRunTime) {
        this.initTaskRunTime = initTaskRunTime;
    }

    /**
     * Sets the pre-batch task run time.
     *
     * @param preBatchRunTime the run time in milliseconds
     */
    public void setPreBatchRunTime(Long preBatchRunTime) {
        this.preBatchRunTime = preBatchRunTime;
    }

    /**
     * Sets the URIs load time.
     *
     * @param urisLoadTime the load time in milliseconds
     */
    public void setUrisLoadTime(Long urisLoadTime) {
        this.urisLoadTime = urisLoadTime;
    }

    /**
     * Sets the post-batch task run time.
     *
     * @param postBatchRunTime the run time in milliseconds
     */
    public void setPostBatchRunTime(Long postBatchRunTime) {
        this.postBatchRunTime = postBatchRunTime;
    }
}
