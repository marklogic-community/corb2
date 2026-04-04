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
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
}
