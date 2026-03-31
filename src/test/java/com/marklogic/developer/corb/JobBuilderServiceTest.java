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
import java.lang.reflect.Modifier;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        String marker = "\"name\":\"" + optionName + "\"";
        int start = json.indexOf(marker);
        assertTrue(start >= 0, "Expected option metadata for " + optionName);
        int end = json.indexOf('}', start);
        assertTrue(end > start, "Expected JSON object to close for " + optionName);
        return json.substring(start, end);
    }
}
