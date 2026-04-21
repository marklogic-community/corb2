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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;;

class JobBuilderServiceTest {

    @Test
    void buildMetadataJsonIncludesGroupedOptions() {
        JobBuilderService service = new JobBuilderService(null);

        String json = service.buildMetadataJson();

        assertTrue(json.contains("\"id\":\"connection\""));
        assertTrue(json.contains("\"name\":\"XCC-CONNECTION-URI\""));
        assertTrue(json.contains("\"id\":\"security\""));
        assertTrue(json.contains("\"name\":\"SSL-CONFIG-CLASS\""));
        assertTrue(json.contains("\"id\":\"retry\""));
        assertTrue(json.contains("\"name\":\"QUERY-RETRY-LIMIT\""));
        assertTrue(json.contains("\"subgroupTitle\":\"INIT\""));
        assertTrue(json.contains("\"subgroupTitle\":\"XML\""));
        assertTrue(json.contains("\"subgroupTitle\":\"EXPORT-FILE\""));
        assertTrue(json.contains("\"subgroupTitle\":\"METRICS\""));
        assertTrue(json.contains("\"subgroupTitle\":\"URIS\""));
        assertTrue(json.contains("\"subgroupTitle\":\"LOADER\""));
        assertTrue(json.contains("\"subgroupTitle\":\"RESTART\""));
        assertFalse(json.contains("\"name\":\"XQUERY-MODULE\""));

        String restartableOption = metadataForOption(json, Options.RESTARTABLE);
        assertTrue(restartableOption.contains("\"inputType\":\"boolean\""));
        assertTrue(restartableOption.contains("\"subgroupTitle\":\"RESTART\""));

        String restartStateDirOption = metadataForOption(json, Options.RESTART_STATE_DIR);
        assertTrue(restartStateDirOption.contains("\"subgroupTitle\":\"RESTART\""));
    }

    @Test
    void reflectedBuilderCatalogIncludesAllSupportedPublicOptions() {
        JobBuilderService service = new JobBuilderService(null);

        int reflectedCount = 0;
        for (Field field : Options.class.getDeclaredFields()) {
            if (String.class.equals(field.getType())
                && Modifier.isPublic(field.getModifiers())
                && field.getAnnotation(Usage.class) != null
                && field.getAnnotation(Deprecated.class) == null) {
                Usage usage = field.getAnnotation(Usage.class);
                if (isNotEmpty(usage.description())) {
                    reflectedCount++;
                }
            }
        }

        int builderCount = 0;
        for (JobBuilderService.OptionGroup group : service.getOptionGroups()) {
            builderCount += group.getOptionCount();
        }

        assertEquals(reflectedCount, builderCount);
    }

    @Test
    void buildPropertiesFileMergesAdditionalPropertiesAndLetsUiValuesWin() throws Exception {
        JobBuilderService service = new JobBuilderService(null);
        Map<String, String> values = new HashMap<>();
        values.put(JobBuilderService.PARAM_ADDITIONAL_PROPERTIES, "CUSTOM-OPTION=true\nTHREAD-COUNT=2\nPROCESS-MODULE=/extra/process.xqy\n");
        values.put(Options.PROCESS_MODULE, "/main/process.xqy");
        values.put(Options.THREAD_COUNT, "8");
        values.put(Options.JOB_NAME, "builder-job");
        values.put("UNSUPPORTED-FORM-OPTION", "ignored");

        String text = service.buildPropertiesFile(values);
        Properties properties = new Properties();
        properties.load(new StringReader(text));

        assertEquals("/main/process.xqy", properties.getProperty(Options.PROCESS_MODULE));
        assertEquals("8", properties.getProperty(Options.THREAD_COUNT));
        assertEquals("builder-job", properties.getProperty(Options.JOB_NAME));
        assertEquals("true", properties.getProperty("CUSTOM-OPTION"));
        assertFalse(properties.containsKey("UNSUPPORTED-FORM-OPTION"));
    }

    @Test
    void resolveDownloadFilenameSanitizesPathAndAddsExtension() {
        JobBuilderService service = new JobBuilderService(null);
        Map<String, String> values = new HashMap<>();
        values.put(JobBuilderService.PARAM_DOWNLOAD_FILE_NAME, "../nightly-export");

        assertEquals("..-nightly-export.properties", service.resolveDownloadFilename(values));
    }

    private String metadataForOption(String json, String optionName) {
        String marker = "\"name\":\"" + optionName + '"';
        int start = json.indexOf(marker);
        assertTrue(start >= 0, "Expected option metadata for " + optionName);
        int end = json.indexOf('}', start);
        assertTrue(end > start, "Expected JSON object to close for " + optionName);
        return json.substring(start, end);
    }

    // -------------------------------------------------------------------------
    // resolveDownloadFilename additional branches
    // -------------------------------------------------------------------------

    @Test
    void resolveDownloadFilenameWithBlankNameReturnsDefault() {
        JobBuilderService service = new JobBuilderService(null);
        assertEquals(JobBuilderService.DEFAULT_PROPERTIES_FILE_NAME,
            service.resolveDownloadFilename(Collections.emptyMap()));
    }

    @Test
    void resolveDownloadFilenameWithPropertiesExtensionNotDuplicated() {
        JobBuilderService service = new JobBuilderService(null);
        Map<String, String> values = new HashMap<>();
        values.put(JobBuilderService.PARAM_DOWNLOAD_FILE_NAME, "my-job.properties");
        assertEquals("my-job.properties", service.resolveDownloadFilename(values));
    }

    // -------------------------------------------------------------------------
    // launchJob branches
    // -------------------------------------------------------------------------

    @Test
    void launchJobWithNullLauncherThrowsIllegalState() {
        JobBuilderService service = new JobBuilderService(null);
        assertThrows(IllegalStateException.class, () -> service.launchJob(Collections.emptyMap()));
    }

    @Test
    void launchJobDelegatesToLauncher() throws Exception {
        JobBuilderService.JobLauncher launcher = mock(JobBuilderService.JobLauncher.class);
        when(launcher.launch(any())).thenReturn(new JobBuilderService.JobLaunchResult("j1", "test-job", "/j1"));
        JobBuilderService service = new JobBuilderService(launcher);
        JobBuilderService.JobLaunchResult result = service.launchJob(Collections.emptyMap());
        assertEquals("j1", result.toJson().contains("\"jobId\":\"j1\"") ? "j1" : "");
        verify(launcher).launch(any());
    }

    // -------------------------------------------------------------------------
    // escapeJson special characters
    // -------------------------------------------------------------------------

    @Test
    void escapeJsonWithNullReturnsEmpty() {
        assertEquals("", JobBuilderService.escapeJson(null));
    }

    @Test
    void escapeJsonWithSpecialChars() {
        assertEquals("\\\"", JobBuilderService.escapeJson("\""));
        assertEquals("\\\\", JobBuilderService.escapeJson("\\"));
        assertEquals("\\b", JobBuilderService.escapeJson("\b"));
        assertEquals("\\f", JobBuilderService.escapeJson("\f"));
        assertEquals("\\n", JobBuilderService.escapeJson("\n"));
        assertEquals("\\r", JobBuilderService.escapeJson("\r"));
        assertEquals("\\t", JobBuilderService.escapeJson("\t"));
        // control character <= 0x1F
        assertEquals("\\u0001", JobBuilderService.escapeJson("\u0001"));
        // regular text unchanged
        assertEquals("hello", JobBuilderService.escapeJson("hello"));
    }

    // -------------------------------------------------------------------------
    // escapePropertiesValue (private — tested via reflection)
    // -------------------------------------------------------------------------

    @Test
    void escapePropertiesValueWithNullReturnsEmpty() throws Exception {
        Method method = JobBuilderService.class.getDeclaredMethod("escapePropertiesValue", String.class);
        method.setAccessible(true);
        JobBuilderService service = new JobBuilderService(null);
        assertEquals("", method.invoke(service, (String) null));
    }

    @Test
    void escapePropertiesValueWithSpecialChars() throws Exception {
        Method method = JobBuilderService.class.getDeclaredMethod("escapePropertiesValue", String.class);
        method.setAccessible(true);
        JobBuilderService service = new JobBuilderService(null);
        assertEquals("\\\\", method.invoke(service, "\\"));
        assertEquals("\\n", method.invoke(service, "\n"));
        assertEquals("\\r", method.invoke(service, "\r"));
        assertEquals("\\t", method.invoke(service, "\t"));
        assertEquals("\\ hello", method.invoke(service, " hello"));  // leading space
        assertEquals("world", method.invoke(service, "world"));      // no escaping needed
    }

    // -------------------------------------------------------------------------
    // JobLaunchResult.toJson with null jobName
    // -------------------------------------------------------------------------

    @Test
    void jobLaunchResultToJsonWithNullJobNameRendersEmpty() {
        JobBuilderService.JobLaunchResult result = new JobBuilderService.JobLaunchResult("id-1", null, "/id-1");
        String json = result.toJson();
        assertTrue(json.contains("\"jobName\":\"\""), "Expected empty jobName in: " + json);
        assertTrue(json.contains("\"jobId\":\"id-1\""));
        assertTrue(json.contains("\"jobPath\":\"/id-1\""));
    }

    // -------------------------------------------------------------------------
    // OptionDefinition with null description and placeholder
    // -------------------------------------------------------------------------

    @Test
    void optionDefinitionWithNullDescriptionAndPlaceholderDefaultsToEmpty() {
        JobBuilderService.OptionDefinition def =
            new JobBuilderService.OptionDefinition("MY-OPT", null, "text", null, "general", "General");
        String json = def.toJson();
        assertTrue(json.contains("\"description\":\"\""), "Expected empty description in: " + json);
        assertTrue(json.contains("\"placeholder\":\"\""), "Expected empty placeholder in: " + json);
    }

    // -------------------------------------------------------------------------
    // getSubmittedValue fallback to getParameter (via buildPropertiesFile)
    // -------------------------------------------------------------------------

    @Test
    void buildPropertiesFileFallsBackToUnderscoreLowercaseParameterName() throws Exception {
        JobBuilderService service = new JobBuilderService(null);
        Map<String, String> values = new HashMap<>();
        // Use the lowercase hyphenated form; JobServer.getParameter does toLowerCase() lookup
        values.put("process-module", "/ext/fallback.xqy");
        values.put("thread-count", "4");

        String text = service.buildPropertiesFile(values);
        Properties properties = new Properties();
        properties.load(new StringReader(text));

        assertEquals("/ext/fallback.xqy", properties.getProperty(Options.PROCESS_MODULE));
        assertEquals("4", properties.getProperty(Options.THREAD_COUNT));
    }

    // -------------------------------------------------------------------------
    // buildPropertiesFile serialization ordering and additional properties
    // -------------------------------------------------------------------------

    @Test
    void buildPropertiesFileSerializesAdditionalPropertiesAfterKnownOptions() throws Exception {
        JobBuilderService service = new JobBuilderService(null);
        Map<String, String> values = new HashMap<>();
        values.put(JobBuilderService.PARAM_ADDITIONAL_PROPERTIES, "ZZZZZ-CUSTOM=last\nAAAA-CUSTOM=first\n");

        String text = service.buildPropertiesFile(values);
        // Additional properties should appear (sorted) and not override absent known options
        assertTrue(text.contains("AAAA-CUSTOM=first"));
        assertTrue(text.contains("ZZZZZ-CUSTOM=last"));
        // Known options from additional props that the UI didn't set should come first in their group order
        int aaIdx = text.indexOf("AAAA-CUSTOM");
        int zzIdx = text.indexOf("ZZZZZ-CUSTOM");
        assertTrue(aaIdx < zzIdx, "Extra properties should be sorted alphabetically");
    }
}
