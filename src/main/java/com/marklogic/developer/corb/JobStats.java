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
    private static final String XDMP_LOG_FORMAT = "xdmp:log('%1$s','%2$s')";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final String START_TIME = "startTime";
    private static final String OPEN_SQUARE = "[";
    private static final String CLOSE_SQUARE = "]";
    private static final String GT = ">";
    private static final String LT = "<";
    private static final String QUOTE = "\"";
    private static final String COLON = ":";
    private static final String OPEN_CURLY = "{";
    private static final String CLOSE_CURLY = "}";
    private static final String COMMA = ",";
    private static final String URI = "uri";
    private static final String JOB_ID = "id";
    private static final String JOB_NAME = "name";
    public static final String JOB_ROOT = "job";
    public static final String CORB_NS = "http://marklogic.github.io/corb/";
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

    private Long numberOfFailedTasks = 0l;
    private Long numberOfSucceededTasks = 0l;
    private Double averageTransactionTime = 0.0d;
    private Long urisLoadTime = -1l;
    private Long preBatchRunTime = -1l;
    private Long postBatchRunTime = -1l;
    private Long initTaskRunTime = -1l;
    private Long totalRunTimeInMillis = -1l;
    private String jobRunLocation = null;
    private String jobId = null;
    private String jobName = null;
    private Map<String, Long> longRunningUris = new HashMap<>();
    private List<String> failedUris = null;
    private String uri = null;
    private boolean paused;
    private Long currentThreadCount = 0l;
    private Long jobServerPort = -1l;

    private ContentSourcePool csp;
    private TransformOptions options;

    private static final Logger LOG = Logger.getLogger(JobStats.class.getName());

    public JobStats(Manager manager) {
        super(manager);
        options = manager.options;
        csp = manager.getContentSourcePool();
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
        synchronized (this) {

            if (manager != null && manager.monitor != null) {
                jobId = manager.jobId;
                paused = manager.isPaused();

                if (options != null) {
                    jobName = options.getJobName();
                    jobServerPort = options.getJobServerPort().longValue();
                }

                startTime = epochMillisAsFormattedDateString(manager.getStartMillis());

                taskCount = manager.monitor.getTaskCount();
                if (taskCount > 0) {

                    PausableThreadPoolExecutor threadPool = manager.monitor.pool;
                    longRunningUris = threadPool.getTopUris();
                    failedUris = threadPool.getFailedUris();
                    numberOfFailedTasks = Integer.toUnsignedLong(threadPool.getNumFailedUris());
                    numberOfSucceededTasks = Integer.toUnsignedLong(threadPool.getNumSucceededUris());

                    Long currentTimeMillis = System.currentTimeMillis();
                    Long totalTime = manager.getEndMillis() - manager.getStartMillis();
                    if (totalTime > 0) {
                        currentThreadCount = 0l;
                        totalRunTimeInMillis = totalTime;
                        long totalTransformTime = currentTimeMillis - manager.getTransformStartMillis();
                        averageTransactionTime = totalTransformTime / Double.valueOf(numberOfFailedTasks) + Double.valueOf(numberOfSucceededTasks);
                        endTime = epochMillisAsFormattedDateString(manager.getEndMillis());
                        estimatedTimeOfCompletion = null;
                    } else {

                        currentThreadCount = Long.valueOf(options.getThreadCount());
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
    }

    protected static String epochMillisAsFormattedDateString(long epochMillis) {
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        return date.format(DATE_FORMATTER);
    }

    public void logToServer(String message, boolean concise) {
        String processModule = options.getMetricsModule();
        String metricsDocument;
        String metricsLogMessage = null;

        if (isJavaScriptModule(processModule)) {
            metricsDocument = toJSONString(concise);
            metricsLogMessage = metricsDocument;
        } else {
            metricsDocument = toXMLString(concise);
            metricsLogMessage = getJsonFromXML(metricsDocument);
        }
        executeModule(metricsDocument);
        logToServer(message, metricsLogMessage);
    }

    private void logToServer(String message, String metrics) {
        if (csp != null) {
        		ContentSource contentSource = csp.get();
            try (Session session = contentSource.newSession()) {
                String logLevel = options.getLogMetricsToServerLog();
                if (options.isMetricsLoggingEnabled(logLevel)) {
                    String xquery = XQUERY_VERSION_ML
                            + (message != null
                                    ? String.format(XDMP_LOG_FORMAT, message, logLevel.toLowerCase()) + ","
                                    : "")
                            + String.format(XDMP_LOG_FORMAT, metrics, logLevel.toLowerCase());

                    AdhocQuery query = session.newAdhocQuery(xquery);
                    session.submitRequest(query);
                }
            } catch (Exception e) {
                LOG.log(SEVERE, "logJobStatsToServer request failed", e);
            }
        }
    }

    private void executeModule(String metrics) {
        String metricsDatabase = options.getMetricsDatabase();
        if (metricsDatabase != null) {
            String uriRoot = options.getMetricsRoot();

            ResultSequence seq = null;
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setCacheResult(false);
            String collections = options.getMetricsCollections();
            String processModule = options.getMetricsModule();

            Thread.yield();// try to avoid thread starvation

            if (csp != null) {
            		ContentSource contentSource = csp.get();
                try (Session session = contentSource.newSession()) {
                    Request request = manager.getRequestForModule(processModule, session);

                    request.setNewStringVariable(METRICS_DB_NAME_PARAM, metricsDatabase);
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
                        this.uri = uri;
                    }
                    
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
        refresh();
        strBuff.append(xmlNode(JOB_LOCATION, jobRunLocation))
                .append(xmlNode(JOB_ID, jobId))
                .append(xmlNode(JOB_NAME, jobName))
                .append(xmlNode(HOST, host))
                .append(concise ? "" : xmlNode(USER_PROVIDED_OPTIONS, userProvidedOptions))
                .append(xmlNode(START_TIME, startTime))
                .append(xmlNode(END_TIME, endTime))
                .append(concise ? "" : xmlNode(INIT_TASK_TIME, initTaskRunTime))
                .append(concise ? "" : xmlNode(PRE_BATCH_RUN_TIME, preBatchRunTime))
                .append(concise ? "" : xmlNode(URIS_LOAD_TIME, urisLoadTime))
                .append(concise ? "" : xmlNode(POST_BATCH_RUN_TIME, postBatchRunTime))
                .append(xmlNode(AVERAGE_TRANSACTION_TIME, averageTransactionTime))
                .append(concise ? "" : xmlNode(TOTAL_NUMBER_OF_TASKS, taskCount))
                .append(xmlNode(TOTAL_JOB_RUN_TIME, totalRunTimeInMillis))
                .append(xmlNode(NUMBER_OF_FAILED_TASKS, numberOfFailedTasks))
                .append(xmlNode(NUMBER_OF_SUCCEEDED_TASKS, numberOfSucceededTasks))
                .append(xmlNode(METRICS_DOC_URI, uri))
                .append(xmlNode(PAUSED, Boolean.toString(paused)))
                .append(xmlNode(JOB_SERVER_PORT, jobServerPort))
                .append(xmlNode(AVERAGE_TPS, avgTps > 0 ? formatTransactionsPerSecond(avgTps) : ""))
                .append(xmlNode(CURRENT_TPS, currentTps > 0 ? formatTransactionsPerSecond(currentTps) : ""))
                .append(xmlNode(ESTIMATED_TIME_OF_COMPLETION, estimatedTimeOfCompletion))
                .append(xmlNode(CURRENT_THREAD_COUNT, currentThreadCount))
                .append(xmlNode(METRICS_TIMESTAMP, LocalDateTime.now().format(DATE_FORMATTER)))
                .append(concise ? "" : xmlNodeArray(FAILED_URIS, URI, failedUris))
                .append(concise ? "" : xmlNodeRanks(LONG_RUNNING_URIS, longRunningUris));
        return xmlNode(JOB_ROOT, strBuff.toString(), CORB_NS);
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
            strBuff.append(OPEN_CURLY + "\"").append(root.getNodeName()).append("\"" + COLON + OPEN_CURLY);
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
                strBuff.append(COMMA);
            }
            Node child = nodeList.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (child.getNodeName().equals(LONG_RUNNING_URIS)) {
                    strBuff.append(toJsonProperty(child.getNodeName()))
                            .append(COLON)
                            .append(OPEN_SQUARE);
                    NodeList uris = child.getChildNodes();
                    for (int j = 0; j < uris.getLength(); j++) {
                        if (uris.item(j).hasChildNodes()) {
                            if (j != 0) {
                                strBuff.append(COMMA);
                            }
                            strBuff.append(OPEN_CURLY);
                            NodeList innerUris = uris.item(j).getChildNodes();
                            for (int k = 0; k < innerUris.getLength(); k++) {
                                Node uri = innerUris.item(k);
                                if (k != 0) {
                                    strBuff.append(COMMA);
                                }
                                strBuff.append(toJsonProperty(uri.getNodeName()))
                                        .append(COLON)
                                        .append(toJsonValue(uri.getTextContent()));
                            }
                            strBuff.append(CLOSE_CURLY);
                        }
                    }
                    strBuff.append(CLOSE_SQUARE);
                } else if (child.getNodeName().equals(FAILED_URIS)) {
                    strBuff.append(toJsonProperty(child.getNodeName()))
                            .append(COLON)
                            .append(OPEN_SQUARE);
                    NodeList uris = child.getChildNodes();
                    for (int j = 0; j < uris.getLength(); j++) {
                        if (uris.item(j).hasChildNodes()) {
                            if (j != 0) {
                                strBuff.append(COMMA);
                            }
                            NodeList innerUris = uris.item(j).getChildNodes();
                            for (int k = 0; k < innerUris.getLength(); k++) {
                                Node uri = innerUris.item(k);
                                if (k != 0) {
                                    strBuff.append(COMMA);
                                }
                                strBuff.append(toJsonValue(uri.getTextContent()));
                            }
                        }
                    }
                    strBuff.append(CLOSE_SQUARE);
                } else if (child.hasChildNodes() && child.getFirstChild().getNodeType() == Node.TEXT_NODE) {
                    strBuff.append(toJsonProperty(child.getNodeName()))
                            .append(COLON)
                            .append(toJsonValue(child.getTextContent()));
                } else {
                    strBuff.append(toJsonProperty(child.getNodeName()))
                            .append(COLON)
                            .append(OPEN_CURLY)
                            .append(getJson(child))
                            .append(CLOSE_CURLY);
                }
            }
        }
        return strBuff.toString();
    }

    public static StringBuffer toJsonProperty(String propertyName) {
        return new StringBuffer(QUOTE).append(propertyName).append(QUOTE);
    }

    public static StringBuffer toJsonValue(String value) {
        StringBuffer buffer = new StringBuffer();
        if (isNumeric(value)) {
            buffer.append(value);
        } else if (Boolean.TRUE.toString().equalsIgnoreCase(value) || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
            buffer.append(Boolean.toString(StringUtils.stringToBoolean(value)));
        } else {
            buffer.append(QUOTE)
                .append(value.replace(QUOTE, "\\\""))
                .append(QUOTE);
        }
        return buffer;
    }

    public static boolean isNumeric(String str) {
        return str.matches("-?\\d+(\\.\\d+)?");
    }

    private static String xmlNode(String nodeName, String nodeVal) {
        return xmlNode(nodeName, nodeVal, null);
    }

    private static String xmlNode(String nodeName, Long nodeVal) {
        return xmlNode(nodeName, nodeVal != null && nodeVal >= 0l ? nodeVal.toString() : null);
    }

    protected static String xmlNode(String nodeName, Double nodeVal) {
        return xmlNode(nodeName, nodeVal != null && nodeVal >= 0.0 ? nodeVal.toString() : null);
    }

    protected static String xmlNode(String nodeName, Integer nodeVal) {
        return xmlNode(nodeName, nodeVal != null && nodeVal >= 0 ? nodeVal.toString() : null);
    }

    protected static String xmlNodeArray(String nodeName, String childNodeName, List<String> nodeVals) {
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

    protected static String xmlNode(String nodeName, String nodeVal, String defaultNS) {
        if (StringUtils.isNotEmpty(nodeVal)) {
            StringBuffer strBuff = new StringBuffer();
            strBuff.append(LT).append(nodeName);
            if (defaultNS != null) {
                strBuff.append(" xmlns='").append(defaultNS).append('\'');
            }

            strBuff.append(GT).append(nodeVal).append("</").append(nodeName).append(GT);
            return strBuff.toString();
        } else {
            return "";
        }
    }

    protected static String xmlNode(String nodeName, Map<String, String> nodeVal) {
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

    protected static String xmlNodeRanks(String nodeName, Map<String, Long> nodeVal) {
        if (nodeVal != null && !nodeVal.isEmpty()) {
            StringBuffer strBuff = new StringBuffer();
            NavigableSet<Long> ranks = new TreeSet<>();
            ranks.addAll(nodeVal.values());
            Map<Integer, String> rankToXML = new HashMap<>();
            int numUris = nodeVal.keySet().size();
            for (Map.Entry<String, Long> entry : nodeVal.entrySet()) {
                StringBuffer rankXml = new StringBuffer();
                Long time = entry.getValue();
                Integer rank = numUris - ranks.headSet(time).size();
                String urisWithSameRank = rankToXML.get(rank);
                if (urisWithSameRank != null) {
                    rankXml.append(urisWithSameRank).append(xmlNode(URI, entry.getKey()));
                } else {
                    rankXml.append(xmlNode(URI, entry.getKey()))
                            .append(xmlNode("rank", rank))
                            .append(xmlNode("timeInMillis", time));
                }
                rankToXML.put(rank, rankXml.toString());
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
