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

import com.marklogic.developer.corb.util.IOUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles Job Server requests for the browser-based options builder.
 * @since 2.6.0
 */
public class JobBuilderHandler implements HttpHandler {

    public static final String BUILDER_ROOT_PATH = "/builder";
    public static final String BUILDER_METADATA_PATH = BUILDER_ROOT_PATH + "/metadata";
    public static final String BUILDER_PROPERTIES_PATH = BUILDER_ROOT_PATH + "/properties";
    public static final String BUILDER_JOBS_PATH = BUILDER_ROOT_PATH + "/jobs";

    private static final Logger LOG = Logger.getLogger(JobBuilderHandler.class.getName());
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    private final JobBuilderService service;

    JobBuilderHandler(JobServer jobServer) {
        this(new JobBuilderService(new EmbeddedJobLauncher(jobServer)));
    }

    JobBuilderHandler(JobBuilderService service) {
        this.service = service;
    }

    static boolean canHandle(String path) {
        return path != null && path.startsWith(BUILDER_ROOT_PATH);
    }

    /**
     * Handles incoming HTTP requests for the options builder, routing them to the appropriate service methods based on the request path and method.
     *
     * @param httpExchange the HTTP exchange containing the request and response objects
     * @throws IOException if an I/O error occurs while handling the request
     */
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        JobServer.allowXSS(httpExchange);
        String method = httpExchange.getRequestMethod();
        String path = httpExchange.getRequestURI().getPath();

        if ("OPTIONS".equalsIgnoreCase(method)) {
            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
            return;
        }

        try {
            if (BUILDER_METADATA_PATH.equals(path)) {
                requireMethod(method, "GET");
                writeResponse(httpExchange, HttpURLConnection.HTTP_OK, JobServer.MIME_JSON, service.buildMetadataJson(), null);
            } else if (BUILDER_PROPERTIES_PATH.equals(path)) {
                requireMethod(method, "POST");
                Map<String, String> submittedValues = readFormValues(httpExchange);
                writeResponse(
                    httpExchange,
                    HttpURLConnection.HTTP_OK,
                    "text/plain; charset=utf-8",
                    service.buildPropertiesFile(submittedValues),
                    "attachment; filename=\"" + service.resolveDownloadFilename(submittedValues) + '"');
            } else if (BUILDER_JOBS_PATH.equals(path)) {
                requireMethod(method, "POST");
                Map<String, String> submittedValues = readFormValues(httpExchange);
                writeResponse(httpExchange, HttpURLConnection.HTTP_OK, JobServer.MIME_JSON, service.launchJob(submittedValues).toJson(), null);
            } else {
                writeError(httpExchange, HttpURLConnection.HTTP_NOT_FOUND, "Unknown options builder endpoint");
            }
        } catch (IllegalArgumentException ex) {
            writeError(httpExchange, HttpURLConnection.HTTP_BAD_REQUEST, ex.getMessage());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Unable to process options builder request", ex);
            writeError(httpExchange, HttpURLConnection.HTTP_INTERNAL_ERROR, ex.getMessage());
        }
    }

    private void requireMethod(String actualMethod, String expectedMethod) {
        if (!expectedMethod.equalsIgnoreCase(actualMethod)) {
            throw new IllegalArgumentException("Unsupported method " + actualMethod + " for options builder endpoint");
        }
    }

    private Map<String, String> readFormValues(HttpExchange httpExchange) throws IOException {
        String body = new String(IOUtils.toByteArray(httpExchange.getRequestBody()), UTF_8);
        return decodeFormValues(body);
    }

    private Map<String, String> decodeFormValues(String formBody) throws IOException {
        Map<String, String> result = new HashMap<>();
        if (formBody == null || formBody.isEmpty()) {
            return result;
        }

        String[] params = formBody.split("&");
        for (String param : params) {
            String[] pair = param.split("=", 2);
            String key = URLDecoder.decode(pair[0], UTF_8.name());
            String value = pair.length > 1 ? URLDecoder.decode(pair[1], UTF_8.name()) : "";
            result.put(key, value);
        }
        return result;
    }

    private void writeError(HttpExchange httpExchange, int statusCode, String message) throws IOException {
        String safeMessage = message == null ? "" : message;
        writeResponse(httpExchange, statusCode, JobServer.MIME_JSON,
            "{\"error\":\"" + JobBuilderService.escapeJson(safeMessage) + "\"}", null);
    }

    private void writeResponse(HttpExchange httpExchange, int statusCode, String contentType, String body, String contentDisposition)
        throws IOException {
        byte[] bytes = body.getBytes(UTF_8);
        httpExchange.getResponseHeaders().set(JobServer.HEADER_CONTENT_TYPE, contentType);
        if (contentDisposition != null) {
            httpExchange.getResponseHeaders().set("Content-Disposition", contentDisposition);
        }
        httpExchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream outputStream = httpExchange.getResponseBody()) {
            outputStream.write(bytes);
            outputStream.flush();
        }
    }

    static class EmbeddedJobLauncher implements JobBuilderService.JobLauncher {

        private static final Logger LAUNCHER_LOG = Logger.getLogger(EmbeddedJobLauncher.class.getName());

        private final JobServer jobServer;

        EmbeddedJobLauncher(JobServer jobServer) {
            this.jobServer = jobServer;
        }

        @Override
        public JobBuilderService.JobLaunchResult launch(Properties properties) throws Exception {
            final EmbeddedManager manager = new EmbeddedManager();
            manager.init(properties);
            manager.jobId = UUID.randomUUID().toString();
            jobServer.addManager(manager);

            Thread jobThread = new Thread(() -> {
                try {
                    manager.run();
                } catch (Exception ex) {
                    LAUNCHER_LOG.log(Level.SEVERE, "Options builder job failed", ex);
                } finally {
                    manager.close();
                }
            }, "corb-builder-" + manager.getJobId());
            jobThread.start();

            return new JobBuilderService.JobLaunchResult(
                manager.getJobId(),
                manager.options.getJobName(),
                JobServer.HTTP_RESOURCE_PATH + manager.getJobId());
        }
    }

    static class EmbeddedManager extends Manager {
        @Override
        public void close() {
            if (scheduledExecutor != null) {
                scheduledExecutor.shutdown();
            }
            IOUtils.closeQuietly(csp);
        }
    }
}
