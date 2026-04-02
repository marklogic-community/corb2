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

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;

/**
 * Provides metadata and serialization helpers for the Job Server options builder UI.
 */
public class JobBuilderService {

    static final String PARAM_ADDITIONAL_PROPERTIES = "builder.additionalProperties";
    static final String PARAM_DOWNLOAD_FILE_NAME = "builder.downloadFileName";
    static final String DEFAULT_PROPERTIES_FILE_NAME = "corb-job.properties";

    private static final Logger LOG = Logger.getLogger(JobBuilderService.class.getName());

    private static final String GROUP_CONNECTION = "connection";
    private static final String GROUP_SECURITY = "security";
    private static final String GROUP_INPUT = "input";
    private static final String GROUP_PROCESSING = "processing";
    private static final String GROUP_EXECUTION = "execution";
    private static final String GROUP_EXPORT = "export";
    private static final String GROUP_MONITORING = "monitoring";
    private static final String GROUP_RETRY = "retry";
    private static final String GROUP_ADVANCED = "advanced";

    private static final Set<String> BOOLEAN_OPTIONS = setOf(
        Options.CONTENT_SOURCE_RENEW,
        Options.DISK_QUEUE,
        Options.EXPORT_FILE_AS_ZIP,
        Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE,
        Options.EXPORT_FILE_SORT,
        Options.FAIL_ON_ERROR,
        Options.INSTALL,
        Options.LOADER_BASE64_ENCODE,
        Options.LOADER_SET_URIS_BATCH_REF,
        Options.LOADER_USE_ENVELOPE,
        Options.METADATA_TO_PROCESS_MODULE,
        Options.PRE_POST_BATCH_ALWAYS_EXECUTE,
        Options.RESTARTABLE,
        Options.URIS_REDACTED,
        Options.XCC_HTTPCOMPLIANT,
        Options.XCC_URL_ENCODE_COMPONENTS,
        Options.XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS
    );

    private static final Set<String> NUMBER_OPTIONS = setOf(
        Options.BATCH_SIZE,
        Options.COMMAND_FILE_POLL_INTERVAL,
        Options.CONTENT_SOURCE_RENEW_INTERVAL,
        Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE,
        Options.EXIT_CODE_IGNORED_ERRORS,
        Options.EXIT_CODE_NO_URIS,
        Options.EXPORT_FILE_SPLIT_MAX_LINES,
        Options.MAX_OPTS_FROM_MODULE,
        Options.METRICS_NUM_FAILED_TRANSACTIONS,
        Options.METRICS_NUM_SLOW_TRANSACTIONS,
        Options.METRICS_SYNC_FREQUENCY,
        Options.NUM_TPS_FOR_ETC,
        Options.POST_BATCH_MINIMUM_COUNT,
        Options.PRE_BATCH_MINIMUM_COUNT,
        Options.QUERY_RETRY_INTERVAL,
        Options.QUERY_RETRY_LIMIT,
        Options.THREAD_COUNT,
        Options.XCC_CONNECTION_HOST_RETRY_LIMIT,
        Options.XCC_CONNECTION_RETRY_INTERVAL,
        Options.XCC_CONNECTION_RETRY_LIMIT,
        Options.XCC_PORT,
        Options.XCC_TOKEN_DURATION
    );

    private static final Set<String> PASSWORD_OPTIONS = setOf(
        Options.XCC_API_KEY,
        Options.XCC_OAUTH_TOKEN,
        Options.XCC_PASSWORD,
        Options.SSL_KEY_PASSWORD,
        Options.SSL_KEYSTORE_PASSWORD,
        Options.SSL_TRUSTSTORE_PASSWORD
    );

    private static final Set<String> TEXTAREA_OPTIONS = setOf(
        Options.EXPORT_FILE_BOTTOM_CONTENT,
        Options.EXPORT_FILE_TOP_CONTENT
    );

    private static final Map<String, String> PLACEHOLDERS = new HashMap<>();

    static {
        PLACEHOLDERS.put(Options.BATCH_URI_DELIM, ";");
        PLACEHOLDERS.put(Options.COLLECTION_NAME, "collection-name");
        PLACEHOLDERS.put(Options.COMMAND_FILE, "/tmp/corb-command.properties");
        PLACEHOLDERS.put(Options.CONNECTION_POLICY, "ROUND-ROBIN, RANDOM, or LOAD");
        PLACEHOLDERS.put(Options.DISK_QUEUE_TEMP_DIR, "/tmp/corb-queue");
        PLACEHOLDERS.put(Options.ERROR_FILE_NAME, "errors.txt");
        PLACEHOLDERS.put(Options.EXPORT_FILE_DIR, "/tmp/exports");
        PLACEHOLDERS.put(Options.EXPORT_FILE_NAME, "export.txt");
        PLACEHOLDERS.put(Options.EXPORT_FILE_SPLIT_MAX_SIZE, "10MB");
        PLACEHOLDERS.put(Options.JOB_NAME, "nightly-job");
        PLACEHOLDERS.put(Options.JOB_SERVER_PORT, "8005-8010");
        PLACEHOLDERS.put(Options.LOADER_PATH, "/envelope/instance");
        PLACEHOLDERS.put(Options.LOADER_VARIABLE, "URI");
        PLACEHOLDERS.put(Options.METRICS_DATABASE, "Metrics");
        PLACEHOLDERS.put(Options.METRICS_MODULE, "/ext/metrics.xqy");
        PLACEHOLDERS.put(Options.METRICS_ROOT, "/");
        PLACEHOLDERS.put(Options.MODULE_ROOT, "/");
        PLACEHOLDERS.put(Options.MODULES_DATABASE, "Modules");
        PLACEHOLDERS.put(Options.OPTIONS_FILE, "job.properties");
        PLACEHOLDERS.put(Options.POST_BATCH_MODULE, "/ext/post-batch.xqy");
        PLACEHOLDERS.put(Options.POST_BATCH_TASK, "com.example.PostBatchTask");
        PLACEHOLDERS.put(Options.PRE_BATCH_MODULE, "/ext/pre-batch.xqy");
        PLACEHOLDERS.put(Options.PRE_BATCH_TASK, "com.example.PreBatchTask");
        PLACEHOLDERS.put(Options.PRIVATE_KEY_FILE, "/path/to/private-key.pem");
        PLACEHOLDERS.put(Options.PROCESS_MODULE, "/ext/process.xqy");
        PLACEHOLDERS.put(Options.PROCESS_TASK, "com.example.ProcessTask");
        PLACEHOLDERS.put(Options.SSL_KEYSTORE, "/path/to/keystore.jks");
        PLACEHOLDERS.put(Options.SSL_PROPERTIES_FILE, "/path/to/ssl.properties");
        PLACEHOLDERS.put(Options.SSL_TRUSTSTORE, "/path/to/truststore.jks");
        PLACEHOLDERS.put(Options.TEMP_DIR, "/tmp");
        PLACEHOLDERS.put(Options.URIS_FILE, "/path/to/uris.txt");
        PLACEHOLDERS.put(Options.URIS_MODULE, "/ext/get-uris.xqy");
        PLACEHOLDERS.put(Options.XCC_BASE_PATH, "/manage/v2");
        PLACEHOLDERS.put(Options.XCC_CONNECTION_URI, "xcc://user:password@host:8000/content");
        PLACEHOLDERS.put(Options.XCC_DBNAME, "content");
        PLACEHOLDERS.put(Options.XCC_HOSTNAME, "localhost");
        PLACEHOLDERS.put(Options.XCC_PORT, "8000");
        PLACEHOLDERS.put(Options.XCC_PROTOCOL, "xcc or xccs");
        PLACEHOLDERS.put(Options.XCC_USERNAME, "admin");
        PLACEHOLDERS.put(Options.XML_FILE, "/path/to/uris.xml");
        PLACEHOLDERS.put(Options.XML_NODE, "/*/*");
        PLACEHOLDERS.put(Options.XML_SCHEMA, "/path/to/schema.xsd");
        PLACEHOLDERS.put(Options.XML_TEMP_DIR, "/tmp/corb-xml");
        PLACEHOLDERS.put(Options.ZIP_FILE, "/path/to/uris.zip");
    }

    private final JobLauncher jobLauncher;
    private final List<OptionGroup> optionGroups;
    private final Set<String> supportedOptionNames;
    private final Map<String, String> optionDescriptions;

    public JobBuilderService(JobLauncher jobLauncher) {
        this.jobLauncher = jobLauncher;
        this.optionDescriptions = loadOptionDescriptions();
        this.optionGroups = buildOptionGroups();
        this.supportedOptionNames = buildSupportedOptionNames(optionGroups);
    }

    public String buildMetadataJson() {
        StringBuilder json = new StringBuilder();
        json.append('{');
        json.append("\"groups\":[");
        for (int i = 0; i < optionGroups.size(); i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append(optionGroups.get(i).toJson());
        }
        json.append(']');
        json.append('}');
        return json.toString();
    }

    public String buildPropertiesFile(Map<String, String> submittedValues) {
        return serializeProperties(buildProperties(submittedValues));
    }

    public String resolveDownloadFilename(Map<String, String> submittedValues) {
        String requestedName = trim(getSubmittedValue(submittedValues, PARAM_DOWNLOAD_FILE_NAME));
        if (isBlank(requestedName)) {
            return DEFAULT_PROPERTIES_FILE_NAME;
        }

        String sanitized = requestedName.replace('\\', '-').replace('/', '-').trim();
        if (isBlank(sanitized)) {
            return DEFAULT_PROPERTIES_FILE_NAME;
        }
        if (!sanitized.toLowerCase(Locale.ENGLISH).endsWith(".properties")) {
            sanitized += ".properties";
        }
        return sanitized;
    }

    public JobLaunchResult launchJob(Map<String, String> submittedValues) throws Exception {
        if (jobLauncher == null) {
            throw new IllegalStateException("A job launcher is required to start jobs from the options builder");
        }
        return jobLauncher.launch(buildProperties(submittedValues));
    }

    protected Properties buildProperties(Map<String, String> submittedValues) {
        Properties properties = new Properties();
        mergeAdditionalProperties(properties, getSubmittedValue(submittedValues, PARAM_ADDITIONAL_PROPERTIES));
        for (String optionName : supportedOptionNames) {
            String value = trim(getSubmittedValue(submittedValues, optionName));
            if (isNotBlank(value)) {
                properties.setProperty(optionName, value);
            }
        }
        return properties;
    }

    protected List<OptionGroup> getOptionGroups() {
        return optionGroups;
    }

    protected final void finalize() {}

    private Set<String> buildSupportedOptionNames(List<OptionGroup> groups) {
        Set<String> optionNames = new LinkedHashSet<>();
        for (OptionGroup group : groups) {
            for (OptionDefinition option : group.options) {
                optionNames.add(option.name);
            }
        }
        return optionNames;
    }

    private String getSubmittedValue(Map<String, String> submittedValues, String name) {
        if (submittedValues.containsKey(name)) {
            return submittedValues.get(name);
        }
        return JobServer.getParameter(submittedValues, name);
    }

    private void mergeAdditionalProperties(Properties properties, String rawAdditionalProperties) {
        if (isBlank(rawAdditionalProperties)) {
            return;
        }

        Properties additionalProperties = new Properties();
        try {
            additionalProperties.load(new StringReader(rawAdditionalProperties));
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to parse additional properties", ex);
        }

        for (String propertyName : additionalProperties.stringPropertyNames()) {
            properties.setProperty(propertyName, additionalProperties.getProperty(propertyName));
        }
    }

    private String serializeProperties(Properties properties) {
        List<String> orderedNames = new ArrayList<>();
        for (OptionGroup group : optionGroups) {
            for (OptionDefinition option : group.options) {
                if (properties.containsKey(option.name)) {
                    orderedNames.add(option.name);
                }
            }
        }

        Set<String> remainingNames = new HashSet<>(properties.stringPropertyNames());
        remainingNames.removeAll(new HashSet<>(orderedNames));
        List<String> sortedRemainingNames = new ArrayList<>(remainingNames);
        Collections.sort(sortedRemainingNames);
        orderedNames.addAll(sortedRemainingNames);

        StringBuilder builder = new StringBuilder();
        for (String propertyName : orderedNames) {
            builder.append(propertyName)
                .append('=')
                .append(escapePropertiesValue(properties.getProperty(propertyName)))
                .append('\n');
        }
        return builder.toString();
    }

    private String escapePropertiesValue(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (i == 0 && ch == ' ') {
                        builder.append("\\ ");
                    } else {
                        builder.append(ch);
                    }
            }
        }
        return builder.toString();
    }

    private List<OptionGroup> buildOptionGroups() {
        LinkedHashMap<String, OptionGroupBuilder> builders = new LinkedHashMap<>();
        builders.put(GROUP_CONNECTION, new OptionGroupBuilder(
            GROUP_CONNECTION,
            "Connection",
            "Configure XCC connection details, pooling behavior, and connection-specific settings.",
            Arrays.asList(
                Options.XCC_CONNECTION_URI,
                Options.XCC_PROTOCOL,
                Options.XCC_HOSTNAME,
                Options.XCC_PORT,
                Options.XCC_DBNAME,
                Options.XCC_USERNAME,
                Options.XCC_PASSWORD,
                Options.XCC_API_KEY,
                Options.XCC_OAUTH_TOKEN,
                Options.XCC_BASE_PATH,
                Options.XCC_GRANT_TYPE,
                Options.XCC_TOKEN_DURATION,
                Options.XCC_TOKEN_ENDPOINT,
                Options.XCC_HTTPCOMPLIANT,
                Options.XCC_URL_ENCODE_COMPONENTS,
                Options.XCC_TIME_ZONE,
                Options.CONNECTION_POLICY,
                Options.CONTENT_SOURCE_POOL,
                Options.CONTENT_SOURCE_RENEW,
                Options.CONTENT_SOURCE_RENEW_INTERVAL
            )));
        builders.put(GROUP_SECURITY, new OptionGroupBuilder(
            GROUP_SECURITY,
            "Security & SSL",
            "Manage decryption and SSL/truststore configuration for secure connections.",
            Arrays.asList(
                Options.DECRYPTER,
                Options.JASYPT_PROPERTIES_FILE,
                Options.PRIVATE_KEY_FILE,
                Options.PRIVATE_KEY_ALGORITHM,
                Options.SSL_CONFIG_CLASS,
                Options.SSL_PROPERTIES_FILE,
                Options.SSL_CIPHER_SUITES,
                Options.SSL_ENABLED_PROTOCOLS,
                Options.SSL_KEYSTORE,
                Options.SSL_KEYSTORE_TYPE,
                Options.SSL_KEYSTORE_PASSWORD,
                Options.SSL_KEY_PASSWORD,
                Options.SSL_TRUSTSTORE,
                Options.SSL_TRUSTSTORE_TYPE,
                Options.SSL_TRUSTSTORE_PASSWORD
            )));
        builders.put(GROUP_INPUT, new OptionGroupBuilder(
            GROUP_INPUT,
            "Input & Loaders",
            "Choose where URIs come from and how loader-driven by text file, XML, JSON, Directory, or ZIP inputs are interpreted.",
            Arrays.asList(
                Options.COLLECTION_NAME,
                Options.URIS_MODULE,
                Options.URIS_FILE,
                Options.URIS_LOADER,
                Options.URIS_REDACTED,
                Options.URIS_REPLACE_PATTERN,
                Options.LOADER_PATH,
                Options.LOADER_VARIABLE,
                Options.LOADER_USE_ENVELOPE,
                Options.LOADER_BASE64_ENCODE,
                Options.LOADER_SET_URIS_BATCH_REF,
                Options.JSON_FILE,
                Options.JSON_NODE,
                Options.JSON_METADATA,
                Options.JSON_TEMP_DIR,
                Options.XML_FILE,
                Options.XML_NODE,
                Options.XML_METADATA,
                Options.XML_SCHEMA,
                Options.XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS,
                Options.XML_TEMP_DIR,
                Options.ZIP_FILE
            )));
        builders.put(GROUP_PROCESSING, new OptionGroupBuilder(
            GROUP_PROCESSING,
            "Modules & Tasks",
            "Configure processing modules, tasks, module resolution, and metadata hand-off between job phases.",
            Arrays.asList(
                Options.PROCESS_MODULE,
                Options.PROCESS_TASK,
                Options.INIT_MODULE,
                Options.INIT_TASK,
                Options.PRE_BATCH_MODULE,
                Options.PRE_BATCH_TASK,
                Options.POST_BATCH_MODULE,
                Options.POST_BATCH_TASK,
                Options.MODULE_ROOT,
                Options.MODULES_DATABASE,
                Options.INSTALL,
                Options.MAX_OPTS_FROM_MODULE,
                Options.METADATA,
                Options.METADATA_TO_PROCESS_MODULE
            )));
        builders.put(GROUP_EXECUTION, new OptionGroupBuilder(
            GROUP_EXECUTION,
            "Execution",
            "Tune batching, thread usage, temp files, command-file control, and exit behavior.",
            Arrays.asList(
                Options.THREAD_COUNT,
                Options.BATCH_SIZE,
                Options.BATCH_URI_DELIM,
                Options.FAIL_ON_ERROR,
                Options.DISK_QUEUE,
                Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE,
                Options.DISK_QUEUE_TEMP_DIR,
                Options.TEMP_DIR,
                Options.PRE_BATCH_MINIMUM_COUNT,
                Options.POST_BATCH_MINIMUM_COUNT,
                Options.COMMAND,
                Options.COMMAND_FILE,
                Options.COMMAND_FILE_POLL_INTERVAL,
                Options.PRE_POST_BATCH_ALWAYS_EXECUTE,
                Options.OPTIONS_FILE,
                Options.OPTIONS_FILE_ENCODING,
                Options.EXIT_CODE_NO_URIS,
                Options.EXIT_CODE_IGNORED_ERRORS
            )));
        builders.put(GROUP_EXPORT, new OptionGroupBuilder(
            GROUP_EXPORT,
            "Export & Output",
            "Configure file exports, post-processing, filenames, sorting, zipping, and error output.",
            Arrays.asList(
                Options.EXPORT_FILE_DIR,
                Options.EXPORT_FILE_NAME,
                Options.EXPORT_FILE_PART_EXT,
                Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE,
                Options.EXPORT_FILE_URI_TO_PATH,
                Options.EXPORT_FILE_TOP_CONTENT,
                Options.EXPORT_FILE_BOTTOM_CONTENT,
                Options.EXPORT_FILE_SORT,
                Options.EXPORT_FILE_SORT_COMPARATOR,
                Options.EXPORT_FILE_AS_ZIP,
                Options.EXPORT_FILE_SPLIT_MAX_LINES,
                Options.EXPORT_FILE_SPLIT_MAX_SIZE,
                Options.ERROR_FILE_NAME
            )));
        builders.put(GROUP_MONITORING, new OptionGroupBuilder(
            GROUP_MONITORING,
            "Monitoring & Metrics",
            "Configure the embedded Job Server, job naming, metrics persistence, and throughput reporting.",
            Arrays.asList(
                Options.JOB_NAME,
                Options.JOB_SERVER_PORT,
                Options.NUM_TPS_FOR_ETC,
                Options.METRICS_DATABASE,
                Options.METRICS_COLLECTIONS,
                Options.METRICS_MODULE,
                Options.METRICS_ROOT,
                Options.METRICS_LOG_LEVEL,
                Options.METRICS_NUM_FAILED_TRANSACTIONS,
                Options.METRICS_NUM_SLOW_TRANSACTIONS,
                Options.METRICS_SYNC_FREQUENCY
            )));
        builders.put(GROUP_RETRY, new OptionGroupBuilder(
            GROUP_RETRY,
            "Retry & Resilience",
            "Control retry behavior for query and XCC connection failures.",
            Arrays.asList(
                Options.QUERY_RETRY_ERROR_CODES,
                Options.QUERY_RETRY_ERROR_MESSAGE,
                Options.QUERY_RETRY_INTERVAL,
                Options.QUERY_RETRY_LIMIT,
                Options.RESTARTABLE,
                Options.RESTART_STATE_DIR,
                Options.XCC_CONNECTION_RETRY_LIMIT,
                Options.XCC_CONNECTION_RETRY_INTERVAL,
                Options.XCC_CONNECTION_HOST_RETRY_LIMIT
            )));
        builders.put(GROUP_ADVANCED, new OptionGroupBuilder(
            GROUP_ADVANCED,
            "Advanced",
            "Less common options are still surfaced here, and raw additional properties remain available for anything custom.",
            Collections.emptyList()));

        for (Field field : Options.class.getDeclaredFields()) {
            OptionDefinition option = buildOptionDefinition(field);
            if (option == null) {
                continue;
            }
            String groupId = determineGroupId(field.getName());
            OptionGroupBuilder builder = builders.get(groupId);
            builder.options.add(option);
            builder.registerSubgroup(option);
        }

        List<OptionGroup> groups = new ArrayList<OptionGroup>();
        for (OptionGroupBuilder builder : builders.values()) {
            if (builder.options.isEmpty()) {
                continue;
            }
            Collections.sort(builder.options, builder.newComparator());
            groups.add(builder.toOptionGroup());
        }
        return groups;
    }

    private OptionDefinition buildOptionDefinition(Field field) {
        try {
            if (!String.class.equals(field.getType()) || !Modifier.isPublic(field.getModifiers())) {
                return null;
            }
            if (field.getAnnotation(Deprecated.class) != null || field.getName().endsWith("_LEGACY")) {
                return null;
            }
            Usage usage = field.getAnnotation(Usage.class);
            // if there's no usage annotation or description, we won't include the option in the builder metadata
            if (usage == null || isBlank(usage.description())) {
                return null;
            }
            String optionName = (String) field.get(null);
            String subgroupTitle = determineSubgroupTitle(optionName);
            String subgroupId = subgroupTitle.toLowerCase(Locale.ENGLISH).replaceAll("[^a-z0-9]+", "-");
            return new OptionDefinition(optionName, optionDescriptions.get(optionName), determineInputType(optionName), PLACEHOLDERS.get(optionName), subgroupId, subgroupTitle);
        } catch (IllegalAccessException ex) {
            LOG.log(Level.WARNING, "Unable to build options builder metadata for field " + field.getName(), ex);
            return null;
        }
    }

    private String determineGroupId(String fieldName) {
        if ("COLLECTION_NAME".equals(fieldName) || "MAX_OPTS_FROM_MODULE".equals(fieldName)) {
            return GROUP_ADVANCED;
        }
        if (fieldName.contains("_RETRY") || fieldName.startsWith("RESTART")  ) {
            return GROUP_RETRY;
        }
        if (fieldName.startsWith("XCC_") || fieldName.startsWith("CONTENT_SOURCE_") || "CONNECTION_POLICY".equals(fieldName)) {
            return GROUP_CONNECTION;
        }
        if (fieldName.startsWith("SSL_") || fieldName.startsWith("PRIVATE_KEY_") || "DECRYPTER".equals(fieldName) || "JASYPT_PROPERTIES_FILE".equals(fieldName)) {
            return GROUP_SECURITY;
        }
        if (fieldName.startsWith("METRICS_")
            || "JOB_NAME".equals(fieldName) || "JOB_SERVER_PORT".equals(fieldName)
            || "NUM_TPS_FOR_ETC".equals(fieldName)
            || "URIS_REDACTED".equals(fieldName) || "URIS_REPLACE_PATTERN".equals(fieldName)) {
            return GROUP_MONITORING;
        }
        if (fieldName.startsWith("URIS_") || fieldName.startsWith("LOADER_")
            || fieldName.startsWith("JSON_")
            || fieldName.startsWith("XML_") || fieldName.startsWith("METADATA")
            || "ZIP_FILE".equals(fieldName)) {
            return GROUP_INPUT;
        }
        if (fieldName.startsWith("INIT_") || fieldName.startsWith("PRE_") || fieldName.startsWith("POST_")
            || fieldName.startsWith("PROCESS_")
            || fieldName.endsWith("_TASK")
            || "MODULE_ROOT".equals(fieldName) || "MODULES_DATABASE".equals(fieldName)
            || "INSTALL".equals(fieldName)) {
            return GROUP_PROCESSING;
        }
        if (fieldName.startsWith("EXPORT_") || "ERROR_FILE_NAME".equals(fieldName)) {
            return GROUP_EXPORT;
        }
        if (fieldName.startsWith("EXIT_CODE_") || fieldName.startsWith("BATCH_") || "FAIL_ON_ERROR".equals(fieldName)
            || "THREAD_COUNT".equals(fieldName) || "TEMP_DIR".equals(fieldName) || fieldName.startsWith("DISK_QUEUE")
            || fieldName.startsWith("COMMAND") || fieldName.startsWith("OPTIONS_FILE")) {
            return GROUP_EXECUTION;
        }
        return GROUP_ADVANCED;
    }

    private String determineInputType(String optionName) {
        if (TEXTAREA_OPTIONS.contains(optionName)) {
            return "textarea";
        }
        if (PASSWORD_OPTIONS.contains(optionName) || optionName.endsWith("-PASSWORD")) {
            return "password";
        }
        if (BOOLEAN_OPTIONS.contains(optionName)) {
            return "boolean";
        }
        if (NUMBER_OPTIONS.contains(optionName)) {
            return "number";
        }
        return "text";
    }

    private String determineSubgroupTitle(String optionName) {
        if (optionName.startsWith("EXPORT-FILE-")) {
            return "EXPORT-FILE";
        }
        if (optionName.startsWith("XCC-CONNECTION-")) {
            return "XCC-CONNECTION";
        }
        if (optionName.startsWith("QUERY-RETRY-")) {
            return "QUERY-RETRY";
        }
        if (optionName.startsWith("COMMAND")) {
            return "COMMAND";
        }
        if (optionName.startsWith("CONTENT-SOURCE-")) {
            return "CONTENT-SOURCE";
        }
        if (optionName.startsWith("DISK-QUEUE")) {
            return "DISK-QUEUE";
        }
        if (optionName.startsWith("OPTIONS-FILE-")) {
            return "OPTIONS-FILE";
        }
        if (optionName.startsWith("RESTART")) {
            return "RESTART";
        }
        if (optionName.startsWith("SSL-")) {
            return "SSL";
        }
        if (optionName.startsWith("XCC-")) {
            return "XCC";
        }
        if (optionName.startsWith("XML") || optionName.startsWith("METADATA")) {
            return "XML";
        }
        if (optionName.startsWith("URIS-")) {
            return "URIS";
        }
        if (optionName.startsWith("LOADER-")) {
            return "LOADER";
        }
        if (optionName.startsWith("METRICS-")) {
            return "METRICS";
        }
        if (optionName.startsWith("MODULE")) {
            return "MODULES";
        }
        if (optionName.startsWith("PRE-BATCH-")) {
            return "PRE-BATCH";
        }
        if (optionName.startsWith("POST-BATCH-")) {
            return "POST-BATCH";
        }
        if (optionName.startsWith("INIT-")) {
            return "INIT";
        }
        if (optionName.startsWith("OPTIONS-FILE")) {
            return "OPTIONS-FILE";
        }
        if (optionName.startsWith("PRIVATE-KEY-")) {
            return "PRIVATE-KEY";
        }
        if (optionName.startsWith("PROCESS-")) {
            return "PROCESS";
        }
        if ("NUM-TPS-FOR-ETC".equals(optionName)) {
            return "ETC";
        }
        if (optionName.startsWith("MODULE-")) {
            return "MODULE";
        }
        if (optionName.startsWith("BATCH-")) {
            return "BATCH";
        }
        if (optionName.startsWith("EXIT-CODE-")) {
            return "EXIT-CODE";
        }
        int index = optionName.indexOf('-');
        if (index > 0) {
            return optionName.substring(0, index);
        }
        return "General";
    }

    private Map<String, String> loadOptionDescriptions() {
        Map<String, String> descriptions = new HashMap<>();
        for (Field field : Options.class.getDeclaredFields()) {
            try {
                Usage usage = field.getAnnotation(Usage.class);
                if (usage != null && String.class.equals(field.getType()) && Modifier.isPublic(field.getModifiers())) {
                    descriptions.put((String) field.get(null), usage.description());
                }
            } catch (IllegalAccessException ex) {
                LOG.log(Level.WARNING, "Unable to read option descriptions via reflection", ex);
            }
        }
        return descriptions;
    }

    static String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (ch <= 0x1F) {
                        escaped.append(String.format("\\u%04x", (int) ch));
                    } else {
                        escaped.append(ch);
                    }
            }
        }
        return escaped.toString();
    }

    interface JobLauncher {
        JobLaunchResult launch(Properties properties) throws Exception;
    }

    static class JobLaunchResult {
        private final String jobId;
        private final String jobName;
        private final String jobPath;

        JobLaunchResult(String jobId, String jobName, String jobPath) {
            this.jobId = jobId;
            this.jobName = jobName;
            this.jobPath = jobPath;
        }

        String toJson() {
            StringBuilder json = new StringBuilder();
            json.append('{');
            json.append("\"jobId\":\"").append(escapeJson(jobId)).append("\",");
            json.append("\"jobName\":\"").append(escapeJson(jobName == null ? "" : jobName)).append("\",");
            json.append("\"jobPath\":\"").append(escapeJson(jobPath)).append("\"");
            json.append('}');
            return json.toString();
        }
    }

    static class OptionGroup {
        private final String id;
        private final String title;
        private final String description;
        private final List<OptionSubgroup> subgroups;
        private final List<OptionDefinition> options;

        OptionGroup(String id, String title, String description, List<OptionSubgroup> subgroups, List<OptionDefinition> options) {
            this.id = id;
            this.title = title;
            this.description = description;
            this.subgroups = subgroups;
            this.options = options;
        }

        String toJson() {
            StringBuilder json = new StringBuilder();
            json.append('{');
            json.append("\"id\":\"").append(escapeJson(id)).append("\",");
            json.append("\"title\":\"").append(escapeJson(title)).append("\",");
            json.append("\"description\":\"").append(escapeJson(description)).append("\",");
            json.append("\"subgroups\":[");
            for (int i = 0; i < subgroups.size(); i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append(subgroups.get(i).toJson());
            }
            json.append("],");
            json.append("\"options\":[");
            for (int i = 0; i < options.size(); i++) {
                if (i > 0) {
                    json.append(',');
                }
                json.append(options.get(i).toJson());
            }
            json.append(']');
            json.append('}');
            return json.toString();
        }

        int getOptionCount() {
            return options.size();
        }
    }

    static class OptionDefinition {
        private final String name;
        private final String description;
        private final String inputType;
        private final String placeholder;
        private final String subgroupId;
        private final String subgroupTitle;

        OptionDefinition(String name, String description, String inputType, String placeholder, String subgroupId, String subgroupTitle) {
            this.name = name;
            this.description = description == null ? "" : description;
            this.inputType = inputType;
            this.placeholder = placeholder == null ? "" : placeholder;
            this.subgroupId = subgroupId;
            this.subgroupTitle = subgroupTitle;
        }

        String toJson() {
            StringBuilder json = new StringBuilder();
            json.append('{');
            json.append("\"name\":\"").append(escapeJson(name)).append("\",");
            json.append("\"label\":\"").append(escapeJson(name)).append("\",");
            json.append("\"description\":\"").append(escapeJson(description)).append("\",");
            json.append("\"inputType\":\"").append(escapeJson(inputType)).append("\",");
            json.append("\"subgroupId\":\"").append(escapeJson(subgroupId)).append("\",");
            json.append("\"subgroupTitle\":\"").append(escapeJson(subgroupTitle)).append("\",");
            json.append("\"placeholder\":\"").append(escapeJson(placeholder)).append("\"");
            json.append('}');
            return json.toString();
        }
    }

    static class OptionSubgroup {
        private final String id;
        private final String title;

        OptionSubgroup(String id, String title) {
            this.id = id;
            this.title = title;
        }

        String toJson() {
            StringBuilder json = new StringBuilder();
            json.append('{');
            json.append("\"id\":\"").append(escapeJson(id)).append("\",");
            json.append("\"title\":\"").append(escapeJson(title)).append("\"");
            json.append('}');
            return json.toString();
        }
    }

    private static class OptionGroupBuilder {
        private final String id;
        private final String title;
        private final String description;
        private final Map<String, Integer> preferredOrder = new HashMap<>();
        private final List<OptionDefinition> options = new ArrayList<>();
        private final LinkedHashMap<String, OptionSubgroup> subgroups = new LinkedHashMap<String, OptionSubgroup>();

        OptionGroupBuilder(String id, String title, String description, List<String> orderedOptionNames) {
            this.id = id;
            this.title = title;
            this.description = description;
            for (int i = 0; i < orderedOptionNames.size(); i++) {
                preferredOrder.put(orderedOptionNames.get(i), Integer.valueOf(i));
            }
        }

        java.util.Comparator<OptionDefinition> newComparator() {
            return (left, right) -> {
                int leftOrder = preferredOrder.containsKey(left.name) ? preferredOrder.get(left.name).intValue() : Integer.MAX_VALUE;
                int rightOrder = preferredOrder.containsKey(right.name) ? preferredOrder.get(right.name).intValue() : Integer.MAX_VALUE;
                if (leftOrder != rightOrder) {
                    return leftOrder < rightOrder ? -1 : 1;
                }
                return left.name.compareTo(right.name);
            };
        }

        OptionGroup toOptionGroup() {
            return new OptionGroup(id, title, description, new ArrayList<>(subgroups.values()), options);
        }

        void registerSubgroup(OptionDefinition option) {
            if (!subgroups.containsKey(option.subgroupId)) {
                subgroups.put(option.subgroupId, new OptionSubgroup(option.subgroupId, option.subgroupTitle));
            }
        }
    }

    private static Set<String> setOf(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}
