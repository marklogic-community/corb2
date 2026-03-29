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

import com.marklogic.developer.corb.util.XmlUtils;
import com.sun.net.httpserver.*;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.marklogic.developer.corb.Options.JOB_SERVER_PORT;
import static com.marklogic.developer.corb.util.StringUtils.parsePortRanges;
import static java.util.logging.Level.INFO;

/**
 * HTTP server for monitoring and managing CoRB jobs via a RESTful API and web dashboard.
 * <p>
 * The JobServer provides:
 * </p>
 * <ul>
 * <li>A web-based dashboard for monitoring job progress and statistics</li>
 * <li>RESTful endpoints for retrieving job metrics in XML or JSON format</li>
 * <li>Support for managing multiple concurrent jobs</li>
 * <li>Static resource serving for the web UI and dependencies</li>
 * </ul>
 * <p>
 * The server automatically binds to the first available port from a set of candidates
 * and provides endpoints at:
 * </p>
 * <ul>
 * <li>{@code /} - Web dashboard</li>
 * <li>{@code /metrics} - Aggregate metrics for all jobs</li>
 * <li>{@code /{jobId}} - Job-specific dashboard</li>
 * <li>{@code /{jobId}/metrics} - Job-specific metrics</li>
 * </ul>
 *
 * @see Manager
 * @see JobServicesHandler
 * @see JobStats
 */
public class JobServer {

    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = Logger.getLogger(JobServer.class.getName());

    /**
     * Default size for the HTTP request queue.
     * <p>
     * This value determines how many incoming connections can be queued before
     * the server starts rejecting new connections. A value of 100 provides a reasonable
     * balance for handling bursts of concurrent requests.
     * </p>
     */
    private static final int DEFAULT_REQUEST_QUEUE_SIZE = 100;

    /**
     * Separator line used in log messages for visual clarity.
     * <p>
     * Used to highlight important log messages such as server startup information.
     * </p>
     */
    private static final String SEPARATOR = "*****************************************************************************************";

    /**
     * Classpath location of the web resources folder.
     * <p>
     * Contains static web assets such as HTML, CSS, and JavaScript files for the
     * job monitoring dashboard. Resources are loaded from this folder using the
     * class loader.
     * </p>
     */
    private static final String WEB_FOLDER = "/web";

    /**
     * Classpath location of the WebJars resources folder.
     * <p>
     * WebJars are packaged client-side libraries (e.g., jQuery, Bootstrap) that are
     * bundled as JAR files. This path points to the standard WebJars resource location
     * within JAR files.
     * </p>
     */
    private static final String WEBJARS_FOLDER = "/META-INF/resources/webjars";

    /**
     * Root HTTP path for the server.
     * <p>
     * Represents the base URL path "/" for the web server. Used as the default context
     * path and as a prefix for constructing job-specific paths.
     * </p>
     */
    public static final String HTTP_RESOURCE_PATH = "/";

    /**
     * HTTP path for the metrics endpoint.
     * <p>
     * Requests to this path return job statistics and metrics in XML or JSON format.
     * Can be used for both aggregate metrics (all jobs) and job-specific metrics when
     * combined with a job ID prefix.
     * </p>
     */
    public static final String METRICS_PATH = "/metrics";

    /**
     * MIME type for XML content.
     * <p>
     * Used in Content-Type headers when returning XML-formatted responses.
     * </p>
     */
    public static final String MIME_XML = "application/xml";

    /**
     * MIME type for JSON content.
     * <p>
     * Used in Content-Type headers when returning JSON-formatted responses.
     * This is the default format for metrics endpoints unless XML is explicitly requested.
     * </p>
     */
    public static final String MIME_JSON = "application/json";

    /**
     * HTTP header name for content type.
     * <p>
     * Used to specify the MIME type of the response body in HTTP headers.
     * </p>
     */
    protected static final String HEADER_CONTENT_TYPE = "Content-Type";

    /**
     * The underlying HTTP server instance.
     * <p>
     * This is the Java built-in {@link HttpServer} that handles incoming HTTP requests,
     * manages connections, and dispatches requests to registered handlers.
     * </p>
     */
    private HttpServer server;

    /**
     * List of Manager instances being monitored by this server.
     * <p>
     * Each Manager represents a CoRB job that can be monitored and managed through
     * the web interface. Managers are added when jobs are registered with the server
     * and are used to provide job statistics and metrics.
     * </p>
     */
    private List<Manager> managers = new ArrayList<>();

    /**
     * Map of HTTP context paths to their corresponding HttpContext instances.
     * <p>
     * Stores registered HTTP contexts to enable lookup and prevent duplicate context
     * creation. The key is the context path (e.g., "/", "/jobId") and the value is
     * the HttpContext object that handles requests to that path.
     * </p>
     */
    private Map<String, HttpContext> contexts = new HashMap<>();

    /**
     * Factory for creating DOM document builders for XML processing.
     * <p>
     * Used to create DocumentBuilder instances for parsing and generating XML documents,
     * particularly for job statistics and metrics responses.
     * </p>
     */
    private final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

    /**
     * Factory for creating XSLT transformers.
     * <p>
     * Used to create Transformer instances for converting XML documents to other formats,
     * particularly for transforming job statistics XML to JSON format.
     * </p>
     */
    private TransformerFactory transformerFactory  = TransformerFactory.newInstance();

    /**
     * Cached XSLT templates for transforming job statistics XML to JSON.
     * <p>
     * These templates are compiled once and reused for multiple transformations to
     * improve performance. The templates are lazily initialized on the first request
     * that requires JSON output.
     * </p>
     */
    private Templates jobStatsToJsonTemplates;

    /**
     * Handler for Job Server options-builder endpoints.
     */
    private final JobBuilderHandler jobBuilderHandler;

    /**
     * Flag indicating whether the server has received any HTTP requests.
     * <p>
     * Used to determine shutdown behavior - if requests have been received, the server
     * will wait a grace period before stopping to allow in-flight requests to complete.
     * This prevents abrupt termination of active connections.
     * </p>
     */
    private boolean hasReceivedRequests = false;

    /**
     * Main method to start the JobServer.
     * <p>
     * The server will attempt to bind to the first available port from the specified
     * range (defaulting to 8003-9999 if not provided). Once started, it will log
     * access URLs for monitoring and managing jobs.
     * </p>
     *
     * @param args command-line arguments (optional port range)
     * @throws IOException if the server fails to start or bind to a port
     */
    public static void main(String[] args) throws IOException {
        String portOption = args != null && args.length > 0 ? args[0] : System.getProperty(JOB_SERVER_PORT);
        String jobServerPortRange = portOption != null ? portOption : "8003-9999";
        Set<Integer> jobServerPorts = new LinkedHashSet<>(parsePortRanges(jobServerPortRange));
        JobServer.create(jobServerPorts, null).start();
    }

    /**
     * Creates a JobServer instance bound to the specified port.
     *
     * @param port the port number to bind to
     * @return a new JobServer instance
     * @throws IOException if the server cannot bind to the specified port
     */
    public static JobServer create(Integer port) throws IOException {
        return JobServer.create(Collections.singleton(port), null);
    }

    /**
     * Creates a JobServer instance that binds to the first available port from the given candidates.
     *
     * @param portCandidates set of port numbers to try binding to
     * @param manager the Manager instance to add to this server (may be null)
     * @return a new JobServer instance
     * @throws IOException if none of the candidate ports are available
     */
    public static JobServer create(Set<Integer> portCandidates, Manager manager) throws IOException {
        return create(portCandidates, DEFAULT_REQUEST_QUEUE_SIZE, manager);
    }

    /**
     * Creates a JobServer instance with a custom request queue size.
     *
     * @param portCandidates set of port numbers to try binding to
     * @param requestQueueSize the maximum number of requests to queue
     * @param manager the Manager instance to add to this server (may be null)
     * @return a new JobServer instance
     * @throws IOException if none of the candidate ports are available
     */
    public static JobServer create(Set<Integer> portCandidates, int requestQueueSize, Manager manager) throws IOException {
        return new JobServer(portCandidates, requestQueueSize, manager);
    }

    /**
     * Constructs a JobServer with the specified configuration.
     *
     * @param portCandidates set of port numbers to try binding to
     * @param requestQueueSize the maximum number of requests to queue
     * @param manager the Manager instance to add to this server (may be null)
     * @throws IOException if the server cannot be created or bound to any candidate port
     */
    protected JobServer(Set<Integer> portCandidates, int requestQueueSize, Manager manager) throws IOException {

        server = HttpServer.create();
        bindFirstAvailablePort(portCandidates, requestQueueSize);
        setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        this.jobBuilderHandler = new JobBuilderHandler(this);
        addManager(manager);

        createContext(HTTP_RESOURCE_PATH, this::handleRequest);
    }

    /**
     * Handles incoming HTTP requests, routing to either metrics/API endpoints or static resources.
     * <p>
     * Requests to {@code /metrics} or with format parameters are treated as API requests
     * and return job statistics in XML or JSON format. All other requests are treated as
     * static resource requests for the web UI.
     * </p>
     *
     * @param httpExchange the HTTP exchange containing request and response information
     */
    public void handleRequest(HttpExchange httpExchange) {
        hasReceivedRequests = true;
        String path = httpExchange.getRequestURI().getPath();
        String querystring = httpExchange.getRequestURI().getQuery();
        Map<String,String> params = JobServicesHandler.querystringToMap(querystring);

        if (JobBuilderHandler.canHandle(path)) {
            try {
                jobBuilderHandler.handle(httpExchange);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Unable to handle options builder request", ex);
            }
        } else if (METRICS_PATH.equals(path) || hasParameter(params, JobServicesHandler.PARAM_FORMAT)) {
            alowXSS(httpExchange);

            String contentType = determineContentType(params);
            httpExchange.getResponseHeaders().add(HEADER_CONTENT_TYPE, contentType);

            StringBuilder response = new StringBuilder();

            List<JobStats> managerJobStats = managers.stream()
                .filter(manager -> manager.getJobId() != null) //don't include jobs that have not started
                .map(manager -> {
                    // In case the Manager was added prior to the jobId being assigned, create an HTTPContext now that it is available
                    String jobPath = HTTP_RESOURCE_PATH + manager.getJobId();
                    if (!contexts.containsKey(jobPath)) {
                        addManagerContext(manager);
                    }
                   return manager.getJobStats();
                }).collect(Collectors.toList());

            boolean concise = hasParameter(params, JobServicesHandler.PARAM_CONCISE);
            Document jobs = JobStats.toXML(documentBuilderFactory, managerJobStats, concise);

            if (MIME_XML.equals(contentType)) {
                response.append(XmlUtils.documentToString(jobs));
            } else {
                response.append(toJson(jobs));
            }

            httpExchange.getResponseHeaders().add(HEADER_CONTENT_TYPE, contentType);

            try (OutputStream out = httpExchange.getResponseBody()) {
                httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length());
                out.write(response.toString().getBytes(StandardCharsets.UTF_8));
                out.flush();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Unable to list jobs", ex);
            }

        } else {
            // filename and extensions aren't necessary in the request, but now we need to find the file
            if (path.isEmpty() || HTTP_RESOURCE_PATH.equals(path)) {
                path = "/dashboard.html";
            }
            handleStaticRequest(path, httpExchange);
        }
    }

    /**
     * Transforms an XML document to JSON format using XSLT.
     *
     * @param doc the XML document to transform
     * @return the JSON representation as a string, or empty string if transformation fails
     */
    protected String toJson(Document doc) {
        StringBuilder json = new StringBuilder();
        try {
            if (jobStatsToJsonTemplates == null) {
                jobStatsToJsonTemplates = JobStats.newJobStatsToJsonTemplates(transformerFactory);
            }
            json.append(JobStats.toJSON(jobStatsToJsonTemplates, doc));
        } catch (TransformerException e) {
            LOG.log(Level.SEVERE, "Unable to transform to JSON", e);
        }
        return json.toString();
    }

    /**
     * Handles requests for static resources (HTML, CSS, JavaScript files).
     * <p>
     * Resources are loaded from either the {@code /web} folder or {@code /META-INF/resources/webjars}
     * (for WebJar dependencies). Returns 404 if the resource is not found.
     * </p>
     *
     * @param path the resource path to retrieve
     * @param httpExchange the HTTP exchange for sending the response
     */
    public static void handleStaticRequest(String path, HttpExchange httpExchange) {

        try (InputStream webInputStream = Manager.class.getResourceAsStream(WEB_FOLDER + path);
             InputStream webJarInputStream = Manager.class.getResourceAsStream(WEBJARS_FOLDER + path);
             OutputStream output = httpExchange.getResponseBody()) {

            if (webInputStream == null && webJarInputStream == null) {
                String response = "Error 404 File not found: " + path;
                httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, response.length());
                output.write(response.getBytes());
            } else {
                httpExchange.getResponseHeaders().set(HEADER_CONTENT_TYPE, getContentType(path));
                httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);

                final byte[] buffer = new byte[0x10000];
                int count = 0;

                InputStream inputStream = webInputStream != null ? webInputStream : webJarInputStream;
                while ((count = inputStream.read(buffer)) >= 0) {
                    output.write(buffer, 0, count);
                }
            }
            output.flush();
        } catch (IOException ex) {
            LOG.log(Level.WARNING, "Unable to open file", ex);
        }
    }

    /**
     * Attempts to bind the server to the first available port from the candidate list.
     *
     * @param portCandidates set of port numbers to try
     * @param requestQueueSize the maximum number of requests to queue
     */
    public void bindFirstAvailablePort(Set<Integer> portCandidates, int requestQueueSize) {
        InetSocketAddress socket;
        for (int portCandidate : portCandidates) {
            socket = new InetSocketAddress(portCandidate);
            try {
                bind(socket, requestQueueSize);
                break; //port is available
            } catch (IOException ex) {
                LOG.log(Level.FINE, () -> MessageFormat.format("Port {0,number} is not available, trying the next candidate", portCandidate));
            }
        }
    }

    /**
     * Determines the appropriate MIME content type based on the file extension.
     *
     * @param path the file path
     * @return the MIME content type (defaults to "text/html; charset=utf-8")
     */
    public static String getContentType(String path) {
        String contentType = "text/html; charset=utf-8";
        if (path != null) {
            if (path.endsWith(".js")) {
                contentType = "application/javascript";
            } else if (path.endsWith(".css")) {
                contentType = "text/css";
            }
        }
        return contentType;
    }

    /**
     * Adds a Manager instance to be monitored by this server.
     * <p>
     * If the Manager has a job ID, an HTTP context is created for job-specific endpoints.
     * If the Manager doesn't have its own JobServer, this server is assigned to it.
     * </p>
     *
     * @param manager the Manager to add (ignored if null)
     */
    public void addManager(Manager manager) {
        if (manager != null) {
            if (manager.getJobId() != null) {
                addManagerContext(manager);
                LOG.log(INFO, () -> MessageFormat.format("Monitor and manage the job at http://localhost:{0,number,#}/{1}", server.getAddress().getPort(), manager.jobId));
                LOG.log(INFO, () -> MessageFormat.format("Retrieve job metrics data at http://localhost:{0,number,#}/{1}{2}", server.getAddress().getPort(), manager.jobId, METRICS_PATH));
            }
            managers.add(manager);
        }
    }

    /**
     * Creates an HTTP context for a Manager's job-specific endpoints.
     *
     * @param manager the Manager for which to create a context
     */
    protected void addManagerContext(Manager manager) {
        //if the job was started without it's own JobServer, set this one as it's server
        if (manager.getJobServer() == null) {
            manager.setJobServer(this);
        }
        HttpHandler handler = new JobServicesHandler(manager);
        createContext(HTTP_RESOURCE_PATH + manager.getJobId(), handler);
    }

    /**
     * Logs the server startup message with access URLs.
     */
    public void logUsage() {
        LOG.log(INFO, SEPARATOR);
        LOG.log(INFO,  "Job Server has started");
        LOG.log(INFO, () -> MessageFormat.format("Monitor the status of jobs at http://localhost:{0,number,#}", server.getAddress().getPort()));
        LOG.log(INFO, () -> MessageFormat.format("Retrieve job metrics data at http://localhost:{0,number,#}{1}", server.getAddress().getPort(), METRICS_PATH));
        LOG.log(INFO, SEPARATOR);
    }

    /**
     * Creates an HTTP context with the specified path and handler.
     *
     * @param path the context path
     * @param handler the handler for requests to this context
     * @return the created HttpContext
     */
    public HttpContext createContext(String path, HttpHandler handler) {
        HttpContext context = server.createContext(path, handler);
        contexts.put(path, context);
        return context;
    }

    /**
     * Creates an HTTP context with the specified path.
     *
     * @param context the context path
     * @return the created HttpContext
     */
    public HttpContext createContext(String context) {
        return server.createContext(context);
    }

    /**
     * Returns the socket address the server is bound to.
     *
     * @return the InetSocketAddress of the server
     */
    public InetSocketAddress getAddress() {
        return server.getAddress();
    }

    /**
     * Starts the HTTP server and logs usage information.
     */
    public void start() {
        server.start();
        logUsage();
    }

    /**
     * Stops the HTTP server after the specified delay.
     * <p>
     * If the server has received requests, it will wait the specified delay
     * before stopping to allow in-flight requests to complete.
     * </p>
     *
     * @param delayMillis delay in milliseconds before stopping
     */
    public void stop(int delayMillis) {
        if (hasReceivedRequests) {
            LOG.log(INFO, () -> MessageFormat.format("Stopping job server in {0,number,#}ms", delayMillis));
            try {
                TimeUnit.MILLISECONDS.sleep(delayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        server.stop(0);
    }

    /**
     * Binds the server to the specified socket address with the given backlog.
     *
     * @param inetSocketAddress the socket address to bind to
     * @param i the maximum number of queued incoming connections
     * @throws IOException if the bind operation fails
     */
    public void bind(InetSocketAddress inetSocketAddress, int i) throws IOException {
        server.bind(inetSocketAddress, i);
    }

    /**
     * Sets the executor for handling HTTP requests.
     *
     * @param executor the Executor to use for request handling
     */
    public void setExecutor(Executor executor) {
        server.setExecutor(executor);
    }

    /**
     * Returns the executor used for handling HTTP requests.
     *
     * @return the Executor instance
     */
    public Executor getExecutor() {
        return server.getExecutor();
    }

    /**
     * Removes the HTTP context with the specified path.
     *
     * @param s the context path to remove
     */
    public void removeContext(String s) {
        server.removeContext(s);
    }

    /**
     * Removes the specified HTTP context.
     *
     * @param httpContext the HttpContext to remove
     */
    public void removeContext(HttpContext httpContext) {
        server.removeContext(httpContext);
    }

    /**
     * Checks if the format parameter is set to XML.
     *
     * @param params map of query parameters
     * @return true if format=xml is specified, false otherwise
     */
    public static boolean hasParamFormatXml(Map<String, String> params) {
        return
            hasParameter(params, JobServicesHandler.PARAM_FORMAT) &&
                "xml".equalsIgnoreCase(getParameter(params, JobServicesHandler.PARAM_FORMAT));
    }

    /**
     * Determines the response content type based on the format parameter.
     *
     * @param params map of query parameters
     * @return MIME_XML if format=xml, otherwise MIME_JSON
     */
    public static String determineContentType(Map<String, String> params) {
        return hasParamFormatXml(params) ? MIME_XML : MIME_JSON;
    }

    /**
     * Retrieves a parameter value from the map using case-insensitive key matching.
     *
     * @param map the parameter map
     * @param key the parameter key to look up
     * @return the parameter value, or null if not found
     */
    protected static String getParameter(Map<String, String> map, String key) {
        String value = null;
        String caseSensitiveKey = key.toLowerCase(Locale.ENGLISH);
        if (map.containsKey(caseSensitiveKey)) {
            value = map.get(caseSensitiveKey);
        } else {
            caseSensitiveKey = key.toUpperCase(Locale.ENGLISH);
            if (map.containsKey(caseSensitiveKey)) {
                value = map.get(caseSensitiveKey);
            }
        }
        return value;
    }

    /**
     * Checks if a parameter exists in the map (case-insensitive).
     *
     * @param params the parameter map
     * @param key the parameter key to check
     * @return true if the parameter exists, false otherwise
     */
    protected static boolean hasParameter(Map<String, String>params, String key){
        return getParameter(params, key) != null;
    }

    /**
     * Adds CORS headers to the response to allow cross-origin requests.
     *
     * @param httpExchange the HTTP exchange to modify
     */
    protected static void alowXSS(HttpExchange httpExchange) {
        Headers headers = httpExchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET,POST");
        headers.add("Access-Control-Max-Age", "3600");
        headers.add("Access-Control-Allow-Headers", HEADER_CONTENT_TYPE);
    }
}
