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
import java.nio.charset.Charset;
import java.text.MessageFormat;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.logging.Level.INFO;

public class JobServer {

    private static final Logger LOG = Logger.getLogger(JobServer.class.getName());
    private static final int DEFAULT_REQUEST_QUEUE_SIZE = 100;
    private static final String SEPARATOR = "*****************************************************************************************";
    private static final String WEB_FOLDER = "/web";
    private static final String WEBJARS_FOLDER = "/META-INF/resources/webjars";

    public static final String HTTP_RESOURCE_PATH = "/";
    public static final String METRICS_PATH = "/metrics";
    public static final String MIME_XML = "application/xml";
    public static final String MIME_JSON = "application/json";
    protected static final String HEADER_CONTENT_TYPE = "Content-Type";

    private HttpServer server;
    private List<Manager> managers = new ArrayList<>();
    private Map<String, HttpContext> contexts = new HashMap<>();
    private final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private TransformerFactory transformerFactory  = TransformerFactory.newInstance();
    private Templates jobStatsToJsonTemplates;
    private boolean hasReceivedRequests = false;

    public static JobServer create(Integer port) throws IOException {
        return JobServer.create(Collections.singleton(port), null);
    }

    public static JobServer create(Set<Integer> portCandidates, Manager manager) throws IOException {
        return create(portCandidates, DEFAULT_REQUEST_QUEUE_SIZE, manager);
    }

    public static JobServer create(Set<Integer> portCandidates, int requestQueueSize, Manager manager) throws IOException {
        return new JobServer(portCandidates, requestQueueSize, manager);
    }

    protected JobServer(Set<Integer> portCandidates, int requestQueueSize, Manager manager) throws IOException {

        server = HttpServer.create();
        bindFirstAvailablePort(portCandidates, requestQueueSize);
        setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        addManager(manager);

        createContext(HTTP_RESOURCE_PATH, this::handleRequest);
    }

    public void handleRequest(HttpExchange httpExchange) {
        hasReceivedRequests = true;
        String path = httpExchange.getRequestURI().getPath();
        String querystring = httpExchange.getRequestURI().getQuery();
        Map<String,String> params = JobServicesHandler.querystringToMap(querystring);

        if (METRICS_PATH.equals(path) || hasParameter(params, JobServicesHandler.PARAM_FORMAT)) {
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
                out.write(response.toString().getBytes(Charset.forName("UTF-8")));
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

    protected void addManagerContext(Manager manager) {
        //if the job was started without it's own JobServer, set this one as it's server
        if (manager.getJobServer() == null) {
            manager.setJobServer(this);
        }
        HttpHandler handler = new JobServicesHandler(manager);
        createContext(HTTP_RESOURCE_PATH + manager.getJobId(), handler);
    }

    public void logUsage() {
        LOG.log(INFO, SEPARATOR);
        LOG.log(INFO,  "Job Server has started");
        LOG.log(INFO, () -> MessageFormat.format("Monitor the status of jobs at http://localhost:{0,number,#}", server.getAddress().getPort()));
        LOG.log(INFO, () -> MessageFormat.format("Retrieve job metrics data at http://localhost:{0,number,#}{1}", server.getAddress().getPort(), METRICS_PATH));
        LOG.log(INFO, SEPARATOR);
    }

    public HttpContext createContext(String path, HttpHandler handler) {
        HttpContext context = server.createContext(path, handler);
        contexts.put(path, context);
        return context;
    }

    public HttpContext createContext(String context) {
        return server.createContext(context);
    }

    public InetSocketAddress getAddress() {
        return server.getAddress();
    }

    public void start() {
        server.start();
        logUsage();
    }

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

    public void bind(InetSocketAddress inetSocketAddress, int i) throws IOException {
        server.bind(inetSocketAddress, i);
    }

    public void setExecutor(Executor executor) {
        server.setExecutor(executor);
    }

    public Executor getExecutor() {
        return server.getExecutor();
    }

    public void removeContext(String s) {
        server.removeContext(s);
    }

    public void removeContext(HttpContext httpContext) {
        server.removeContext(httpContext);
    }

    public static boolean hasParamFormatXml(Map<String, String> params) {
        return
            hasParameter(params, JobServicesHandler.PARAM_FORMAT) &&
                "xml".equalsIgnoreCase(getParameter(params, JobServicesHandler.PARAM_FORMAT));
    }

    public static String determineContentType(Map<String, String> params) {
        return hasParamFormatXml(params) ? MIME_XML : MIME_JSON;
    }

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

    protected static boolean hasParameter(Map<String, String>params, String key){
        return getParameter(params, key) != null;
    }

    protected static void alowXSS(HttpExchange httpExchange) {
        Headers headers = httpExchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET,POST");
        headers.add("Access-Control-Max-Age", "3600");
        headers.add("Access-Control-Allow-Headers", HEADER_CONTENT_TYPE);
    }
}
