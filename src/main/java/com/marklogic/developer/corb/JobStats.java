package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

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
    private static final long TPS_ETC_MIN_REFRESH_INTERVAL = 10000l;
    private static final String METRICS_COLLECTIONS_PARAM = "collections";
    private static final String METRICS_DOCUMENT_STR_PARAM = "metricsDocumentStr";
    private static final String METRICS_DB_NAME_PARAM = "dbName";
    private static final String METRICS_URI_ROOT_PARAM = "uriRoot";
    protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\";\n";
    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final String START_TIME = "startTime";
    private static final String CLOSE_SQUARE = "]";
    private static final String GT = ">";
    private static final String LT = "<";
    private static final String OPEN_CURLY = "{";
    private static final String CLOSE_CURLY = "}";
    private static final String COMA = ",";
    private static final String URI = "uri";
    private static final String JOB_NAME = "name";
    private static final String JOB_ROOT = "job";
    private static final String DEF_NS = "http://marklogic.github.io/corb/";
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

    protected Map<String, String> userProvidedOptions = new HashMap<>();
    protected String startTime = null;
    protected String endTime = null;
    protected String host = null;
    protected Long totalNumberOfTasks = 0l;
    protected Long numberOfFailedTasks = 0l;
    protected Long numberOfSucceededTasks = 0l;
    protected Double averageTransactionTime = 0.0d;
    protected Long urisLoadTime = null;
    private Long preBatchRunTime = 0l;
    private Long postBatchRunTime = 0l;
    private Long initTaskRunTime = 0l;
    private Long totalRunTimeInMillis = 0l;
    protected String jobRunLocation = null;
    protected String jobName = null;
    protected Map<String, Long> longRunningUris = new HashMap<>();
    protected List<String> failedUris = null;
    protected String uri = null;
    protected String paused = null;
    protected Long currentThreadCount = 0l;
    protected Long jobServerPort = 0l;

    protected TransformOptions options = null;
    protected ContentSource contentSource;

    private static final Logger LOG = Logger.getLogger(JobStats.class.getName());

    public JobStats() {
        super(null, null);
    }

    public JobStats(Manager manager) {
        super(null, manager);
        options = manager.getOptions();
        contentSource = manager.getContentSource();
        startMillis = manager.getStartMillis();
        String jobName = options.getJobName();
        if (jobName != null) {
            setJobName(jobName);
        }
        String hostname = "Unknown";

        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
            try {
                hostname = InetAddress.getLoopbackAddress().getHostName();
            } catch (Exception e) {
                LOG.log(INFO, "Hostname can not be resolved", e);
            }
        }
        setHost(hostname);
        setJobRunLocation(System.getProperty("user.dir"));
        setStartTime(epochMillisAsFormattedDateString(manager.getStartMillis()));
        setUserProvidedOptions(manager.getUserProvidedOptions());
    }

    private void refresh() {
        synchronized (this) {

            taskCount = taskCount > 0 ? taskCount : (pool != null) ? pool.getTaskCount() : 0l;
            if (taskCount > 0) {
                if (pool != null) {
                    setTotalNumberOfTasks(manager.monitor.getTaskCount());
                    setTopTimeTakingUris(pool.getTopUris());
                    setFailedUris(pool.getFailedUris());
                    setNumberOfFailedTasks(pool.getNumFailedUris());
                    setNumberOfSucceededTasks(pool.getNumSucceededUris());
                }
                setJobServerPort(options.getJobServerPort().longValue());

                Long currentTimeMillis = System.currentTimeMillis();
                Long totalTime = manager.getEndMillis() - manager.getStartMillis();
                if (totalTime > 0) {
                    setTotalRunTimeInMillis(totalTime);
                    Long totalTransformTime = currentTimeMillis - manager.getTransformStartMillis();
                    setAverageTransactionTime(totalTransformTime / Double.valueOf(numberOfFailedTasks + numberOfSucceededTasks));
                    setEndTime(epochMillisAsFormattedDateString(manager.getEndMillis()));
                    estimatedTimeOfCompletion = null;
                    setCurrentThreadCount(0l);
                } else {
                    setTotalRunTimeInMillis(currentTimeMillis - manager.getStartMillis());
                    long completed = numberOfSucceededTasks + numberOfFailedTasks;
                    long intervalBetweenRequestsInMillis = TPS_ETC_MIN_REFRESH_INTERVAL;
                    long timeSinceLastReq = currentTimeMillis - prevMillis;
                    //refresh it every 10 seconds or more.. ignore more frequent requests
                    if (timeSinceLastReq > intervalBetweenRequestsInMillis) {
                        populateTps(completed);
                    }
                    setCurrentThreadCount(Long.valueOf(options.getThreadCount()));
                }
            }
        }
    }

    protected String epochMillisAsFormattedDateString(long epochMillis) {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        return date.format(DATE_FORMATTER);
    }

    public void logJobStatsToServer(String message, boolean concise) {
        String processModule = options.getLogMetricsToServerDBTransformModule();
        String metricsToDocument;
        String metricsToServerLog = null;

        if (isJavaScriptModule(processModule)) {
            metricsToDocument = toJSONString(concise);
        } else {
            metricsToDocument = toXMLString(concise);
            metricsToServerLog = getJsonFromXML(metricsToDocument);
        }
        logJobStatsToServerDocument(metricsToDocument);
        logJobStatsToServerLog(message, metricsToServerLog, concise);
//		LOG.info(toString(concise));//DO WE NEED TO LOG?
    }

    private void logJobStatsToServerLog(String message, String metrics, boolean concise) {
        if (contentSource != null) {
            try (Session session = contentSource.newSession()) {
                String logMetricsToServerLog = options.getLogMetricsToServerLog();
                if (options.isMetricsToServerLogEnabled(logMetricsToServerLog)) {
                    metrics = StringUtils.isEmpty(metrics) ? toString(concise) : metrics;
                    String xquery = XQUERY_VERSION_ML
                            + ((message != null)
                                    ? "xdmp:log(\"" + message + "\",'" + logMetricsToServerLog.toLowerCase() + "'),"
                                    : "")
                            + "xdmp:log('" + metrics + "\','" + logMetricsToServerLog.toLowerCase() + "')";

                    AdhocQuery query = session.newAdhocQuery(xquery);
                    session.submitRequest(query);
                }
            } catch (Exception e) {
                LOG.log(SEVERE, "logJobStatsToServer request failed", e);
            }
        }
    }

    private void logJobStatsToServerDocument(String metrics) {
        String logMetricsToServerDBName = options.getLogMetricsToServerDBName();
        if (logMetricsToServerDBName != null) {
            String uriRoot = options.getLogMetricsToServerDBURIRoot();

            ResultSequence seq = null;
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setCacheResult(false);
            String collections = options.getLogMetricsToServerDBCollections();
            String processModule = options.getLogMetricsToServerDBTransformModule();

            Thread.yield();// try to avoid thread starvation

            if (contentSource != null) {
                try (Session session = contentSource.newSession()) {
                    Request request = manager.getRequestForModule(processModule, session);

                    request.setNewStringVariable(METRICS_DB_NAME_PARAM, logMetricsToServerDBName);
                    if (uriRoot != null) {
                        request.setNewStringVariable(METRICS_URI_ROOT_PARAM, uriRoot);
                    } else {
                        request.setNewStringVariable(METRICS_URI_ROOT_PARAM, NOT_APPLICABLE);
                    }
                    if (collections != null) {
                        request.setNewStringVariable(METRICS_COLLECTIONS_PARAM, collections);
                    } else {
                        request.setNewStringVariable(METRICS_COLLECTIONS_PARAM, NOT_APPLICABLE);
                    }
                    if (isJavaScriptModule(processModule)) {
                        requestOptions.setQueryLanguage("javascript");
                        request.setNewStringVariable(METRICS_DOCUMENT_STR_PARAM,
                                metrics == null ? toJSONString() : metrics);
                    } else {
                        request.setNewStringVariable(METRICS_DOCUMENT_STR_PARAM,
                                metrics == null ? toXMLString() : metrics);

                    }
                    request.setOptions(requestOptions);

                    seq = session.submitRequest(request);
                    String uri = seq.hasNext() ? seq.next().asString() : null;
                    if (uri != null) {
                        setUri(uri);
                    }
                    session.close();
                    Thread.yield();// try to avoid thread starvation
                    seq.close();
                    Thread.yield();// try to avoid thread starvation

                } catch (Exception exc) {
                    LOG.log(SEVERE, "logJobStatsToServerDocument request failed", exc);
                } finally {

                    if (null != seq && !seq.isClosed()) {
                        seq.close();
                        seq = null;
                    }
                    Thread.yield();// try to avoid thread starvation
                }
            }
        }
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobRunLocation() {
        return jobRunLocation;
    }

    public void setJobRunLocation(String jobRunLocation) {
        this.jobRunLocation = jobRunLocation;
    }

    public Map<String, String> getUserProvidedOptions() {
        return userProvidedOptions;
    }

    public void setUserProvidedOptions(Map<String, String> userProvidedOptions) {
        this.userProvidedOptions = userProvidedOptions;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Long getTotalNumberOfTasks() {
        return totalNumberOfTasks;
    }

    public void setTotalNumberOfTasks(Long totalNumberOfTasks) {
        this.totalNumberOfTasks = totalNumberOfTasks;
    }

    public Long getNumberOfFailedTasks() {
        return numberOfFailedTasks;
    }

    public void setNumberOfFailedTasks(Long numberOfFailedTasks) {
        this.numberOfFailedTasks = numberOfFailedTasks;
    }
    public void setNumberOfFailedTasks(Integer numberOfFailedTasks) {
        this.numberOfFailedTasks = numberOfFailedTasks.longValue();
    }

    public Double getAverageTransactionTime() {
        return averageTransactionTime;
    }

    public void setAverageTransactionTime(Double averageTransactionTime) {
        this.averageTransactionTime = averageTransactionTime;
    }

    public Long getUrisLoadTime() {
        return urisLoadTime;
    }

    public void setUrisLoadTime(Long urisLoadTime) {
        this.urisLoadTime = urisLoadTime;
    }

    public Map<String, Long> getTopTimeTakingUris() {
        return longRunningUris;
    }

    public void setTopTimeTakingUris(Map<String, Long> topTimeTakingUris) {
        this.longRunningUris = topTimeTakingUris;
    }

    @Override
    public String toString() {
        return toString(true);
    }

    public String toString(boolean concise) {
        return toJSONString(concise);
    }

    public String toXMLString() {
        return toXMLString(false);
    }

    public String toXMLString(boolean concise) {
        StringBuilder strBuff = new StringBuilder();
        this.refresh();
        strBuff.append(xmlNode(JOB_LOCATION, this.jobRunLocation))
                .append(xmlNode(JOB_NAME, this.jobName))
                .append(xmlNode(HOST, host))
                .append(concise ? "" : xmlNode(USER_PROVIDED_OPTIONS, userProvidedOptions))
                .append(xmlNode(START_TIME, startTime))
                .append(xmlNode(END_TIME, endTime))
                .append(concise ? "" : xmlNode(INIT_TASK_TIME, initTaskRunTime))
                .append(concise ? "" : xmlNode(PRE_BATCH_RUN_TIME, preBatchRunTime))
                .append(concise ? "" : xmlNode(URIS_LOAD_TIME, urisLoadTime))
                .append(concise ? "" : xmlNode(POST_BATCH_RUN_TIME, postBatchRunTime))
                .append(xmlNode(AVERAGE_TRANSACTION_TIME, averageTransactionTime))
                .append(xmlNode(TOTAL_NUMBER_OF_TASKS, totalNumberOfTasks))
                .append(xmlNode(TOTAL_JOB_RUN_TIME, totalRunTimeInMillis))
                .append(xmlNode(NUMBER_OF_FAILED_TASKS, numberOfFailedTasks))
                .append(xmlNode(NUMBER_OF_SUCCEEDED_TASKS, numberOfSucceededTasks))
                .append(xmlNode(METRICS_DOC_URI, uri))
                .append(xmlNode(PAUSED, paused))
                .append(xmlNode(JOB_SERVER_PORT, jobServerPort))
                .append(xmlNode(AVERAGE_TPS, avgTps > 0 ? formatTransactionsPerSecond(avgTps) : ""))
                .append(xmlNode(CURRENT_TPS, currentTps > 0 ? formatTransactionsPerSecond(currentTps) : ""))
                .append(xmlNode(ESTIMATED_TIME_OF_COMPLETION, estimatedTimeOfCompletion))
                .append(xmlNode(CURRENT_THREAD_COUNT, currentThreadCount))
                .append(xmlNode(METRICS_TIMESTAMP, LocalDateTime.now().format(DATE_FORMATTER)))
                .append(concise ? "" : xmlNodeArray(FAILED_URIS, URI, failedUris))
                .append(concise ? "" : xmlNodeRanks(LONG_RUNNING_URIS, longRunningUris));
        return xmlNode(JOB_ROOT, strBuff.toString(), DEF_NS);
    }

    public String toJSONString() {
        return toJSONString(false);
    }

    public String toJSONString(boolean concise) {
        return getJsonFromXML(toXMLString(concise));
    }

    private static String getJsonFromXML(String xml) {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;
        try {
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new InputSource(new StringReader(xml)));

            StringBuilder strBuff = new StringBuilder();
            Node root = doc.getDocumentElement();
            strBuff.append("{\"").append(root.getNodeName()).append("\":{");
            strBuff.append(getJson(root));
            strBuff.append("}}");
            return strBuff.toString();
        } catch (Exception e) {
            // unable to generate json
            // Log and continue
            LOG.log(INFO, "Unable to generate JSON document", e);
        }
        return "";
    }

    private static String getJson(Node node) {
        StringBuffer strBuff = new StringBuffer();

        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (i != 0) {
                strBuff.append(COMA);
            }
            Node child = nodeList.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (child.getNodeName().equals(LONG_RUNNING_URIS)) {
                    strBuff.append("\"").append(child.getNodeName()).append("\":[");
                    NodeList uris = child.getChildNodes();
                    for (int j = 0; j < uris.getLength(); j++) {
                        if (uris.item(j).hasChildNodes()) {
                            if (j != 0) {
                                strBuff.append(COMA);
                            }
                            strBuff.append(OPEN_CURLY);
                            NodeList innerUris = uris.item(j).getChildNodes();
                            for (int k = 0; k < innerUris.getLength(); k++) {
                                Node uri = innerUris.item(k);
                                if (k != 0) {
                                    strBuff.append(COMA);
                                }
                                strBuff.append("\"").append(uri.getNodeName()).append("\":\"")
                                        .append(uri.getTextContent()).append("\"");
                            }
                            strBuff.append(CLOSE_CURLY);
                        }
                    }
                    strBuff.append(CLOSE_SQUARE);
                } else if (child.getNodeName().equals(FAILED_URIS)) {
                    strBuff.append("\"").append(child.getNodeName()).append("\":[");
                    NodeList uris = child.getChildNodes();
                    for (int j = 0; j < uris.getLength(); j++) {
                        if (uris.item(j).hasChildNodes()) {
                            if (j != 0) {
                                strBuff.append(COMA);
                            }
                            NodeList innerUris = uris.item(j).getChildNodes();
                            for (int k = 0; k < innerUris.getLength(); k++) {
                                Node uri = innerUris.item(k);
                                if (k != 0) {
                                    strBuff.append(COMA);
                                }
                                strBuff.append("\"").append(uri.getTextContent()).append("\"");
                            }

                        }
                    }
                    strBuff.append(CLOSE_SQUARE);
                } else if (child.hasChildNodes() && child.getFirstChild().getNodeType() == Node.TEXT_NODE) {
                    strBuff.append("\"").append(child.getNodeName()).append("\":\"").append(child.getTextContent())
                            .append("\"");
                } else {
                    strBuff.append("\"").append(child.getNodeName()).append("\":" + OPEN_CURLY);
                    strBuff.append(getJson(child));
                    strBuff.append(CLOSE_CURLY);
                }
            }

        }
        return strBuff.toString();
    }

    private static String xmlNode(String nodeName, String nodeVal) {
        return xmlNode(nodeName, nodeVal, null);
    }

    private static String xmlNode(String nodeName, Long nodeVal) {
        if (nodeVal != null && nodeVal > 0l) {
            return xmlNode(nodeName, nodeVal.toString(), null);
        } else {
            return "";
        }
    }

    private static String xmlNode(String nodeName, Double nodeVal) {
        if (nodeVal != null && nodeVal > 0.0) {
            return xmlNode(nodeName, nodeVal.toString(), null);
        } else {
            return "";
        }
    }

    private static String xmlNodeArray(String nodeName, String childNodeName, List<String> nodeVals) {
        if (nodeVals != null && !nodeVals.isEmpty()) {

            StringBuffer strBuff = new StringBuffer();
            for (String nodeVal : nodeVals) {
                strBuff.append(xmlNode(childNodeName, nodeVal));
            }
            return xmlNode(nodeName, strBuff.toString(), null);
        } else {
            return "";
        }

    }

    private static String xmlNode(String nodeName, String nodeVal, String defaultNS) {
        if (StringUtils.isNotEmpty(nodeVal)) {
            StringBuffer strBuff = new StringBuffer();
            strBuff.append(LT).append(nodeName);
            if (defaultNS != null) {
                strBuff.append(" xmlns='").append(defaultNS).append("' ");

            }

            strBuff.append(GT).append(nodeVal).append("</").append(nodeName).append(GT);
            return strBuff.toString();
        } else {
            return "";
        }

    }

    private static String xmlNode(String nodeName, Map<String, String> nodeVal) {
        if (nodeVal != null && !nodeVal.isEmpty()) {
            StringBuffer strBuff = new StringBuffer();
            for (Map.Entry<String, String> entry : nodeVal.entrySet()){
                strBuff.append(xmlNode(entry.getKey(), entry.getValue()));
            }
            return xmlNode(nodeName, strBuff.toString());
        } else {
            return "";
        }
    }

    private static String xmlNodeRanks(String nodeName, Map<String, Long> nodeVal) {
        if (nodeVal != null && !nodeVal.isEmpty()) {
            StringBuffer strBuff = new StringBuffer();
            NavigableSet<Long> ranks = new TreeSet<>();
            ranks.addAll(nodeVal.values());
            Map<Integer, String> rankToXML = new HashMap<>();
            int numUris = nodeVal.keySet().size();
            for (Map.Entry<String, Long> entry : nodeVal.entrySet()) {
                StringBuffer strBuff2 = new StringBuffer();
                Long time = entry.getValue();
                Integer rank = numUris - ranks.headSet(time).size();
                String urisWithSameRank = rankToXML.get(rank);
                if (urisWithSameRank != null) {
                    strBuff2.append(urisWithSameRank).append(xmlNode(URI, entry.getKey()));
                } else {
                    strBuff2.append(xmlNode(URI, entry.getKey())).append(xmlNode("rank", "" + rank))
                            .append(xmlNode("timeInMillis", (time) + ""));
                }

                rankToXML.put(rank, strBuff2.toString());
            }
            for (Map.Entry<Integer, String> entry : rankToXML.entrySet()){
                strBuff.append(xmlNode("Uri", entry.getValue()));
            }
            return xmlNode(nodeName, strBuff.toString());
        } else {
            return "";
        }

    }

    /**
     * @param failedUris the failedUris to set
     */
    public void setFailedUris(List<String> failedUris) {
        this.failedUris = failedUris;
    }

    /**
     * @return the failedUris
     */
    public List<String> getFailedUris() {
        return failedUris;
    }

    /**
     * @return the preBatchRunTime
     */
    public Long getPreBatchRunTime() {
        return preBatchRunTime;
    }

    /**
     * @param preBatchRunTime the preBatchRunTime to set
     */
    public void setPreBatchRunTime(Long preBatchRunTime) {
        this.preBatchRunTime = preBatchRunTime;
    }

    /**
     * @return the postBatchRunTime
     */
    public Long getPostBatchRunTime() {
        return postBatchRunTime;
    }

    /**
     * @param postBatchRunTime the postBatchRunTime to set
     */
    public void setPostBatchRunTime(Long postBatchRunTime) {
        this.postBatchRunTime = postBatchRunTime;
    }

    /**
     * @return the initTaskRunTime
     */
    public Long getInitTaskRunTime() {
        return initTaskRunTime;
    }

    /**
     * @param initTaskRunTime the initTaskRunTime to set
     */
    public void setInitTaskRunTime(Long initTaskRunTime) {
        this.initTaskRunTime = initTaskRunTime;
    }

    /**
     * @return the numberOfSucceededTasks
     */
    public Long getNumberOfSucceededTasks() {
        return numberOfSucceededTasks;
    }

    /**
     * @param numberOfSucceededTasks the numberOfSucceededTasks to set
     */
    public void setNumberOfSucceededTasks(Long numberOfSucceededTasks) {
        this.numberOfSucceededTasks = numberOfSucceededTasks;
    }
    public void setNumberOfSucceededTasks(Integer numberOfSucceededTasks) {
        this.numberOfSucceededTasks = numberOfSucceededTasks.longValue();
    }
    /**
     * @return the totalRunTimeInMillis
     */
    public Long getTotalRunTimeInMillis() {
        return totalRunTimeInMillis;
    }

    /**
     * @param totalRunTimeInMillis the totalRunTimeInMillis to set
     */
    public void setTotalRunTimeInMillis(Long totalRunTimeInMillis) {
        this.totalRunTimeInMillis = totalRunTimeInMillis;
    }

    /**
     * @return the uri
     */
    protected String getUri() {
        return uri;
    }

    /**
     * @param uri the uri to set
     */
    protected void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return the paused
     */
    protected String getPaused() {
        return paused;
    }

    /**
     * @param paused the paused to set
     */
    protected void setPaused(String paused) {
        this.paused = paused;
    }

    /**
     * @return the currentThreadCount
     */
    public Long getCurrentThreadCount() {
        return currentThreadCount;
    }

    /**
     * @param currentThreadCount the currentThreadCount to set
     */
    public void setCurrentThreadCount(Long currentThreadCount) {
        this.currentThreadCount = currentThreadCount;
    }

    /**
     * @return the avgTps
     */
    public double getAvgTps() {
        return avgTps;
    }

    /**
     * @param avgTps the avgTps to set
     */
    public void setAvgTps(double avgTps) {
        this.avgTps = avgTps;
    }

    /**
     * @return the currentTps
     */
    public double getCurrentTps() {
        return currentTps;
    }

    /**
     * @param currentTps the currentTps to set
     */
    public void setCurrentTps(double currentTps) {
        this.currentTps = currentTps;
    }

    /**
     * @return the estimatedTimeOfCompletion
     */
    public String getEstimatedTimeOfCompletion() {
        return estimatedTimeOfCompletion;
    }

    /**
     * @param estimatedTimeOfCompletion the estimatedTimeOfCompletion to set
     */
    public void setEstimatedTimeOfCompletion(String estimatedTimeOfCompletion) {
        this.estimatedTimeOfCompletion = estimatedTimeOfCompletion;
    }

    /**
     * @param pool the pool to set
     */
    public void setPool(PausableThreadPoolExecutor pool) {
        this.pool = pool;
    }

    /**
     * @return the jobServerPort
     */
    public Long getJobServerPort() {
        return jobServerPort;
    }

    /**
     * @param jobServerPort the jobServerPort to set
     */
    public void setJobServerPort(Long jobServerPort) {
        this.jobServerPort = jobServerPort;
    }

}
