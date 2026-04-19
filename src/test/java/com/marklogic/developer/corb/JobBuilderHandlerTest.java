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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

class JobBuilderHandlerTest {

    @Test
    void handlePropertiesRequestDecodesFormValues() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_PROPERTIES_PATH));
        when(exchange.getRequestMethod()).thenReturn("POST");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        when(exchange.getRequestBody()).thenReturn(new ByteArrayInputStream((Options.PROCESS_MODULE + "=%2Fext%2Fprocess.xqy&" + JobBuilderService.PARAM_DOWNLOAD_FILE_NAME + "=nightly%20run").getBytes(StandardCharsets.UTF_8)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        JobBuilderService service = mock(JobBuilderService.class);
        when(service.buildPropertiesFile(anyMap())).thenReturn(Options.PROCESS_MODULE + "=/ext/process.xqy\n");
        when(service.resolveDownloadFilename(anyMap())).thenReturn("nightly-run.properties");

        JobBuilderHandler handler = new JobBuilderHandler(service);
        handler.handle(exchange);

        verify(service).buildPropertiesFile(org.mockito.ArgumentMatchers.argThat(params -> "/ext/process.xqy".equals(params.get(Options.PROCESS_MODULE))));
        assertTrue(headers.getFirst("Content-Disposition").contains("nightly-run.properties"));
        assertTrue(out.toString(StandardCharsets.UTF_8.name()).contains(Options.PROCESS_MODULE + "=/ext/process.xqy"));
    }

    @Test
    void handleJobLaunchReturnsLauncherPayload() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_JOBS_PATH));
        when(exchange.getRequestMethod()).thenReturn("POST");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        when(exchange.getRequestBody()).thenReturn(new ByteArrayInputStream((Options.JOB_NAME + "=builder-demo").getBytes(StandardCharsets.UTF_8)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        JobBuilderService service = mock(JobBuilderService.class);
        when(service.launchJob(anyMap())).thenReturn(new JobBuilderService.JobLaunchResult("job-123", "builder-demo", "/job-123"));

        JobBuilderHandler handler = new JobBuilderHandler(service);
        handler.handle(exchange);

        verify(service).launchJob(org.mockito.ArgumentMatchers.argThat(params -> "builder-demo".equals(params.get(Options.JOB_NAME))));
        assertTrue(out.toString(StandardCharsets.UTF_8.name()).contains("\"jobId\":\"job-123\""));
        assertTrue(out.toString(StandardCharsets.UTF_8.name()).contains("\"jobPath\":\"/job-123\""));
    }

    // -------------------------------------------------------------------------
    // canHandle static method
    // -------------------------------------------------------------------------

    @Test
    void canHandleReturnsTrueForBuilderPath() {
        assertTrue(JobBuilderHandler.canHandle(JobBuilderHandler.BUILDER_ROOT_PATH));
        assertTrue(JobBuilderHandler.canHandle(JobBuilderHandler.BUILDER_METADATA_PATH));
        assertTrue(JobBuilderHandler.canHandle(JobBuilderHandler.BUILDER_PROPERTIES_PATH));
        assertTrue(JobBuilderHandler.canHandle(JobBuilderHandler.BUILDER_JOBS_PATH));
    }

    @Test
    void canHandleReturnsFalseForNonBuilderOrNullPath() {
        assertFalse(JobBuilderHandler.canHandle(null));
        assertFalse(JobBuilderHandler.canHandle("/jobs"));
        assertFalse(JobBuilderHandler.canHandle("/metrics"));
    }

    // -------------------------------------------------------------------------
    // OPTIONS method → 204 No Content
    // -------------------------------------------------------------------------

    @Test
    void handleOptionsRequestReturnsNoContent() throws Exception {
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestMethod()).thenReturn("OPTIONS");
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_METADATA_PATH));
        Headers headers = new Headers();
        when(exchange.getResponseHeaders()).thenReturn(headers);

        JobBuilderHandler handler = new JobBuilderHandler(mock(JobBuilderService.class));
        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, -1);
    }

    // -------------------------------------------------------------------------
    // GET /builder/metadata
    // -------------------------------------------------------------------------

    @Test
    void handleGetToMetadataReturnsJson() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_METADATA_PATH));
        when(exchange.getRequestMethod()).thenReturn("GET");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        JobBuilderService service = mock(JobBuilderService.class);
        when(service.buildMetadataJson()).thenReturn("{\"groups\":[]}");

        JobBuilderHandler handler = new JobBuilderHandler(service);
        handler.handle(exchange);

        verify(service).buildMetadataJson();
        assertTrue(out.toString(StandardCharsets.UTF_8.name()).contains("\"groups\""));
    }

    // -------------------------------------------------------------------------
    // Unknown path → 404
    // -------------------------------------------------------------------------

    @Test
    void handleUnknownPathReturns404() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_ROOT_PATH + "/unknown"));
        when(exchange.getRequestMethod()).thenReturn("GET");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        JobBuilderHandler handler = new JobBuilderHandler(mock(JobBuilderService.class));
        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_NOT_FOUND), anyLong());
    }

    // -------------------------------------------------------------------------
    // Wrong HTTP method → 400 (IllegalArgumentException from requireMethod)
    // -------------------------------------------------------------------------

    @Test
    void handleWrongMethodForMetadataReturns400() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_METADATA_PATH));
        when(exchange.getRequestMethod()).thenReturn("POST");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);
        when(exchange.getRequestBody()).thenReturn(new ByteArrayInputStream(new byte[0]));

        JobBuilderHandler handler = new JobBuilderHandler(mock(JobBuilderService.class));
        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_BAD_REQUEST), anyLong());
        assertTrue(out.toString(StandardCharsets.UTF_8.name()).contains("\"error\""));
    }

    // -------------------------------------------------------------------------
    // Service throws generic Exception → 500
    // -------------------------------------------------------------------------

    @Test
    void handleServiceExceptionReturns500() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_METADATA_PATH));
        when(exchange.getRequestMethod()).thenReturn("GET");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        JobBuilderService service = mock(JobBuilderService.class);
        when(service.buildMetadataJson()).thenThrow(new RuntimeException("something went wrong"));

        JobBuilderHandler handler = new JobBuilderHandler(service);
        handler.handle(exchange);

        verify(exchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), anyLong());
        assertTrue(out.toString(StandardCharsets.UTF_8.name()).contains("\"error\""));
    }

    // -------------------------------------------------------------------------
    // EmbeddedManager.close() branches
    // -------------------------------------------------------------------------

    @Test
    void embeddedManagerCloseWithNullScheduledExecutorDoesNotThrow() {
        JobBuilderHandler.EmbeddedManager manager = new JobBuilderHandler.EmbeddedManager();
        // scheduledExecutor is null after construction — should not throw
        assertDoesNotThrow(manager::close);
    }

    @Test
    void embeddedManagerCloseShutdownsScheduledExecutor() {
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        JobBuilderHandler.EmbeddedManager manager = new JobBuilderHandler.EmbeddedManager();
        manager.scheduledExecutor = executor;
        manager.close();
        verify(executor).shutdown();
    }

    // -------------------------------------------------------------------------
    // EmbeddedJobLauncher: constructor + launch failure path (init throws)
    // -------------------------------------------------------------------------

    @Test
    void embeddedJobLauncherLaunchFailureReturnedAs500() throws Exception {
        // Build a handler backed by a real EmbeddedJobLauncher (via the JobServer constructor).
        // init(properties) will throw because no XCC connection is configured.
        JobServer jobServer = mock(JobServer.class);
        JobBuilderHandler handler = new JobBuilderHandler(jobServer);

        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobBuilderHandler.BUILDER_JOBS_PATH));
        when(exchange.getRequestMethod()).thenReturn("POST");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        when(exchange.getRequestBody()).thenReturn(new ByteArrayInputStream(new byte[0]));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        handler.handle(exchange);

        // init throws CorbException (no XCC) → caught as generic Exception → 500
        verify(exchange).sendResponseHeaders(eq(HttpURLConnection.HTTP_INTERNAL_ERROR), anyLong());
    }
}
