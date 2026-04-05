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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP handler for job-specific service endpoints.
 * <p>
 * This handler processes HTTP requests for individual CoRB jobs, providing:
 * </p>
 * <ul>
 * <li>Job metrics and statistics in XML or JSON format</li>
 * <li>Job control operations (pause, resume)</li>
 * <li>Dynamic thread count adjustment</li>
 * <li>Static resource serving for job-specific dashboard pages</li>
 * </ul>
 * <p>
 * Supported HTTP methods: GET, POST, OPTIONS.
 * </p>
 * <p>
 * Query parameters:
 * </p>
 * <ul>
 * <li>{@code FORMAT} - Response format ("xml" or "json", defaults to JSON)</li>
 * <li>{@code CONCISE} - Whether to return concise metrics (presence indicates true)</li>
 * <li>{@code COMMAND} - Job control command ("PAUSE" or "RESUME")</li>
 * <li>{@code THREAD-COUNT} - New thread count for the job (positive integer)</li>
 * </ul>
 *
 * @see JobServer
 * @see Manager
 * @see JobStats
  * @since 2.4.0
 */
public class JobServicesHandler implements HttpHandler {

    /**
     * Logger for this class.
     * Used to log unsupported HTTP methods and parameter parsing errors.
     */
    private static final Logger LOG = Logger.getLogger(JobServicesHandler.class.getName());

    /**
     * Query parameter name for specifying the response format.
     * Valid values: "xml" or "json" (case-insensitive).
     * If not specified, defaults to JSON format.
     */
    public static final String PARAM_FORMAT = "FORMAT";

    /**
     * Query parameter name for requesting concise metrics output.
     * When present (regardless of value), returns a concise version of job statistics
     * that excludes user-provided options and URI lists.
     */
    public static final String PARAM_CONCISE = "CONCISE";

    /**
     * The Manager instance for the job being served.
     * Used to retrieve job statistics, execute control commands,
     * and update job configuration.
     */
    private Manager manager;

    /**
     * Constructs a JobServicesHandler for the specified Manager.
     *
     * @param manager the Manager instance to handle requests for
     */
    JobServicesHandler(Manager manager) {
        this.manager = manager;
    }

    /**
     * Handles incoming HTTP requests for job-specific operations.
     * <p>
     * The method:
     * </p>
     * <ol>
     * <li>Parses query parameters from the request</li>
     * <li>Processes job control commands (pause/resume)</li>
     * <li>Updates thread count if requested</li>
     * <li>Returns metrics data or serves static resources based on the request path</li>
     * </ol>
     * <p>
     * Requests to paths containing "/metrics" or with a FORMAT parameter return
     * job statistics. All other requests serve static resources for the job dashboard.
     * </p>
     *
     * @param httpExchange the HTTP exchange containing request and response information
     * @throws IOException if an I/O error occurs during request processing
     */
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        String querystring = httpExchange.getRequestURI().getQuery();
        Map<String,String> params = querystringToMap(querystring);
        String method = httpExchange.getRequestMethod();
        if ("GET".equals(method) || "POST".equals(method) || "OPTIONS".equals(method)) {
            pauseResumeJob(params);
            updateThreads(params);
            String path = httpExchange.getRequestURI().getPath();
            if (path.contains(JobServer.METRICS_PATH) || JobServer.hasParameter(params, JobServicesHandler.PARAM_FORMAT)) {
                JobServer.alowXSS(httpExchange);
                writeMetricsOut(httpExchange, params, manager);
            } else {
                String jobId = manager.getJobId();
                String relativePath = path.substring(path.indexOf(jobId) + jobId.length());
                if (relativePath.isEmpty() || "/".equals(relativePath)) {
                    relativePath = "/index.html";
                }
                // Normalize and validate path to prevent directory traversal
                Path normalizedPath = Paths.get(relativePath).normalize();
                if (normalizedPath.isAbsolute() || normalizedPath.startsWith("..")) {
                    LOG.log(Level.WARNING, "Rejected potential directory traversal path: {0}", relativePath);
                    httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_FORBIDDEN, -1L);
                    return;
                }
                JobServer.handleStaticRequest(relativePath, httpExchange);
            }

        } else {
            LOG.log(Level.WARNING, "Unsupported method {0}", method);
            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0L);
        }
    }

    /**
     * Writes job metrics to the HTTP response in the requested format.
     * <p>
     * Metrics can be returned in either XML or JSON format, determined by the
     * FORMAT query parameter. The output can be concise or detailed based on
     * the presence of the CONCISE parameter.
     * </p>
     *
     * @param httpExchange the HTTP exchange for sending the response
     * @param params map of query parameters
     * @param manager the Manager whose metrics should be returned
     * @throws IOException if an I/O error occurs while writing the response
     */
    protected static void writeMetricsOut(HttpExchange httpExchange, Map<String, String> params, Manager manager) throws IOException {
        boolean concise = JobServer.hasParameter(params, PARAM_CONCISE);
        String response;
        String contentType;
        JobStats jobStats = manager.getJobStats();
        if (JobServer.hasParamFormatXml(params)) {
            contentType = JobServer.MIME_XML;
            response = jobStats.toXmlString(concise);
        } else {
            contentType = JobServer.MIME_JSON;
            response =  jobStats.toJSON(concise);
        }

        httpExchange.getResponseHeaders().add(JobServer.HEADER_CONTENT_TYPE, contentType);
        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length());
        try (OutputStream out = httpExchange.getResponseBody()) {
            out.write(response.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }

    /**
     * Parses a URL query string into a map of parameter names to values.
     * <p>
     * Parameters without values (e.g., "?param1&amp;param2") are mapped to empty strings.
     * </p>
     *
     * @param query the query string to parse (may be null)
     * @return a map of parameter names to values (never null)
     */
    public static Map<String, String> querystringToMap(String query){
        Map<String, String> result = new HashMap<>();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                if (pair.length > 1) {
                    result.put(pair[0], pair[1]);
                } else {
                    result.put(pair[0], "");
                }
            }
        }
        return result;
    }

    /**
     * Processes job control commands from request parameters.
     * <p>
     * Recognizes the following COMMAND parameter values:
     * </p>
     * <ul>
     * <li>PAUSE - Pauses the job execution</li>
     * <li>RESUME - Resumes a paused job</li>
     * </ul>
     *
     * @param params map of query parameters
     */
    protected void pauseResumeJob(Map<String, String> params) {
        String value = JobServer.getParameter(params, Options.COMMAND);
        if ("PAUSE".equalsIgnoreCase(value)) {
            manager.pause();
        } else if ("RESUME".equalsIgnoreCase(value)) {
            manager.resume();
        }
    }

    /**
     * Updates the job's thread count based on the THREAD-COUNT parameter.
     * <p>
     * The thread count must be a positive integer. Invalid values are logged
     * and ignored.
     * </p>
     *
     * @param params map of query parameters
     */
    protected void updateThreads(Map<String, String> params) {
        String value = JobServer.getParameter(params, Options.THREAD_COUNT);
        if (value != null) {
            try {
                int threadCount = Integer.parseInt(value);
                if (threadCount > 0) {
                    manager.setThreadCount(threadCount);
                }
            } catch (NumberFormatException e) {
                LOG.log(Level.WARNING, MessageFormat.format("{0} value not numeric", Options.THREAD_COUNT), e);
            }
        }
    }

}
