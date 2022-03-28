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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JobServicesHandler implements HttpHandler {

    private static final Logger LOG = Logger.getLogger(JobServicesHandler.class.getName());
    public static final String PARAM_FORMAT = "FORMAT";
    public static final String PARAM_CONCISE = "CONCISE";

    private Manager manager;

    JobServicesHandler(Manager manager) {
        this.manager = manager;
    }

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
                JobServer.handleStaticRequest(relativePath, httpExchange);
            }

        } else {
            LOG.log(Level.WARNING, "Unsupported method {0}", method);
            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0L);
        }
    }

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
            out.write(response.getBytes(Charset.forName("UTF-8")));
            out.flush();
        }
    }

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

    protected void pauseResumeJob(Map<String, String> params) {
        String value = JobServer.getParameter(params, Options.COMMAND);
        if ("PAUSE".equalsIgnoreCase(value)) {
            manager.pause();
        } else if ("RESUME".equalsIgnoreCase(value)) {
            manager.resume();
        }
    }

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
