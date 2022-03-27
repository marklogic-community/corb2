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
import com.marklogic.xcc.exceptions.RequestException;
import org.w3c.dom.*;

import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class JobStats extends BaseMonitor {

    private static final String NOT_APPLICABLE = "NA";
    private static final long TPS_ETC_MIN_REFRESH_INTERVAL = 10000L;
    private static final String METRICS_COLLECTIONS_PARAM = "collections";
    private static final String METRICS_DOCUMENT_STR_PARAM = "metricsDocumentStr";
    private static final String METRICS_DB_NAME_PARAM = "dbName";
    private static final String METRICS_URI_ROOT_PARAM = "uriRoot";
    protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\";\n";
    private static final String XDMP_LOG_FORMAT = "xdmp:log('%1$s','%2$s')";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final String START_TIME = "startTime";
    private static final String URI = "uri";
    private static final String JOB_ID = "id";
    private static final String JOB_NAME = "name";
    public static final String JOB_ELEMENT = "job";
    public static final String JOBS_ELEMENT = "jobs";
    public static final String CORB_NAMESPACE = "http://developer.marklogic.com/code/corb";
    private static final String LONG_RUNNING_URIS = "slowTransactions";
    private static final String FAILED_URIS = "failedTransactions";
    private static final String URIS_LOAD_TIME = "urisLoadTimeInMillis";
    private static final String INIT_TASK_TIME = "initTaskTimeInMillis";
    private static final String PRE_BATCH_RUN_TIME = "preBatchRunTimeInMillis";
    private static final String POST_BATCH_RUN_TIME = "postBatchRunTimeInMillis";
    private static final String TOTAL_JOB_RUN_TIME = "totalRunTimeInMillis";
    private static final String AVERAGE_TRANSACTION_TIME = "averageTransactionTimeInMillis";
    private static final String TOTAL_NUMBER_OF_TASKS = "totalNumberOfTasks";
    private static final String NUMBER_OF_FAILED_TASKS = "numberOfFailedTasks";
    private static final String NUMBER_OF_SUCCEEDED_TASKS = "numberOfSucceededTasks";
    private static final String METRICS_DOC_URI = "metricsDocUri";
    private static final String PAUSED = "paused";
    private static final String AVERAGE_TPS = "averageTransactionsPerSecond";
    private static final String CURRENT_TPS = "currentTransactionsPerSecond";
    private static final String ESTIMATED_TIME_OF_COMPLETION = "estimatedTimeOfCompletion";
    private static final String METRICS_TIMESTAMP = "timestamp";

    private static final String HOST = "host";
    private static final String END_TIME = "endTime";
    private static final String USER_PROVIDED_OPTIONS = "userProvidedOptions";
    private static final String JOB_LOCATION = "runLocation";
    private static final String CURRENT_THREAD_COUNT = "currentThreadCount";
    private static final String JOB_SERVER_PORT = "port";

    private Map<String, String> userProvidedOptions = new HashMap<>();
    private String startTime = null;
    private String endTime = null;
    private String host = null;

    private Long numberOfFailedTasks = 0L;
    private Long numberOfSucceededTasks = 0L;
    private Double averageTransactionTime = 0.0d;
    private Long urisLoadTime = -1L;
    private Long preBatchRunTime = -1L;
    private Long postBatchRunTime = -1L;
    private Long initTaskRunTime = -1L;
    private Long totalRunTimeInMillis = -1L;
    private String jobRunLocation = null;
    private String jobId = null;
    private String jobName = null;
    private Map<String, Long> longRunningUris = new HashMap<>();
    private List<String> failedUris = null;
    private String uri = null;
    private boolean paused;
    private Long currentThreadCount = 0L;
    private Long jobServerPort = -1L;

    private ContentSourcePool contentSourcePool;
    private TransformOptions options;

    protected final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private final TransformerFactory transformerFactory  = TransformerFactory.newInstance();
    private Templates jobStatsToJsonTemplates;

    private static final Logger LOG = Logger.getLogger(JobStats.class.getName());
    private final Object lock = new Object();

    public JobStats(Manager manager) {
        super(manager);
        options = manager.getOptions();
        contentSourcePool = manager.getContentSourcePool();
        host = getHost();
        jobRunLocation = System.getProperty("user.dir");
        userProvidedOptions = manager.getUserProvidedOptions();
    }

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

    protected void refreshOptions(TransformOptions options){
        if (options != null) {
            jobName = options.getJobName();
            jobServerPort = options.getJobServerPort().longValue();
            currentThreadCount = (long) options.getThreadCount();
        }
    }

    protected void refreshMonitorStats(Monitor monitor){
        if (monitor != null) {

            taskCount = monitor.getTaskCount();
            if (taskCount > 0) { //job has selected URIs to process

                refreshThreadPoolExecutorStats(monitor.getThreadPoolExecutor());

                Long currentTimeMillis = System.currentTimeMillis();
                Long totalTime = manager.getEndMillis() - manager.getStartMillis();
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

    protected double getAverageTransactionTime(long totalTransformTime, long numberOfFailedTasks, long numberOfSucceededTasks) {
        long completedTasks = numberOfFailedTasks + numberOfSucceededTasks;
        if (completedTasks > 0) {
            return totalTransformTime / Double.valueOf(completedTasks);
        } else {
            return 0d;
        }
    }

    protected void refreshThreadPoolExecutorStats(PausableThreadPoolExecutor threadPool) {
        if (threadPool != null) {
            longRunningUris = threadPool.getTopUris();
            failedUris = threadPool.getFailedUris();
            numberOfFailedTasks = threadPool.getNumFailedUris();
            numberOfSucceededTasks = threadPool.getNumSucceededUris();
        }
    }

    protected static String epochMillisAsFormattedDateString(long epochMillis) {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        return date.format(DATE_FORMATTER);
    }

    public void logMetrics(String message, boolean concise, boolean logToConsole) {
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
            String uri = seq.hasNext() ? seq.next().asString() : null;
            if (uri != null) {
                this.uri = uri;
            }
        } finally {
            if (null != seq && !seq.isClosed()) {
                seq.close();
                seq = null;
            }
        }
    }

    @Override
    public String toString() {
        return toString(true);
    }

    public String toString(boolean concise) {
        return toJSON(concise);
    }

    public String toXmlString() {
        return toXmlString(false);
    }

    public String toXmlString(boolean concise) {
        Document doc = toXML(concise);
        return XmlUtils.documentToString(doc);
    }

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

    protected void createAndAppendElement(Node parent, String localName, String value) {
        if (StringUtils.isNotEmpty(value)) {
            Element element = createElement(parent, localName, value);
            parent.appendChild(element);
        }
    }

    protected void createAndAppendElement(Node parent, String localName, Integer value) {
        if (value != null && value >= 0L) {
            createAndAppendElement(parent, localName, value.toString());
        }
    }

    protected void createAndAppendElement(Node parent, String localName, Long value) {
        if (value != null && value >= 0L) {
            createAndAppendElement(parent, localName, value.toString());
        }
    }

    protected void createAndAppendElement(Node parent, String localName, Double value) {
        if (value != null && value >= 0L) {
            createAndAppendElement(parent, localName, value.toString());
        }
    }

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

    protected Element createElement(Node parent, String localName, String value) {
        Document doc = parent.getOwnerDocument();
        Element element = doc.createElementNS(CORB_NAMESPACE, localName);
        Text text = doc.createTextNode(value);
        element.appendChild(text);
        return element;
    }

    public String toJSON() {
        return toJSON(false);
    }

    public String toJSON(boolean concise) {
        Document doc = toXML(concise);
        return toJSON(doc);
    }

    public static String toJSON(Templates jobStatsToJsonTemplates, Document doc) throws TransformerException {
        StringWriter writer = new StringWriter();
        Transformer autobot = jobStatsToJsonTemplates.newTransformer();
        autobot.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }

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

    public static Templates newJobStatsToJsonTemplates(TransformerFactory transformerFactory) throws TransformerConfigurationException {
        return newTemplates(transformerFactory, "jobStatsToJson.xsl");
    }

    protected static Templates newTemplates(TransformerFactory transformerFactory, String stylesheetFilename) throws TransformerConfigurationException {
        URL url = Manager.class.getResource( "/" + stylesheetFilename);
        try {
            StreamSource source = new StreamSource(url.openStream());
            source.setSystemId(url.toURI().toString()); //required in order for XSLT to resolve relative paths and itself with document('')
            return transformerFactory.newTemplates(source);
        } catch (URISyntaxException | IOException ex) {
            throw new TransformerConfigurationException("Could not find the template file " + stylesheetFilename + " in the classpath", ex);
        }
    }

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
     * @param initTaskRunTime the initTaskRunTime to set
     */
    public void setInitTaskRunTime(Long initTaskRunTime) {
        this.initTaskRunTime = initTaskRunTime;
    }

    /**
     * @param preBatchRunTime the preBatchRunTime to set
     */
    public void setPreBatchRunTime(Long preBatchRunTime) {
        this.preBatchRunTime = preBatchRunTime;
    }

    public void setUrisLoadTime(Long urisLoadTime) {
        this.urisLoadTime = urisLoadTime;
    }

    /**
     * @param postBatchRunTime the postBatchRunTime to set
     */
    public void setPostBatchRunTime(Long postBatchRunTime) {
        this.postBatchRunTime = postBatchRunTime;
    }
}
