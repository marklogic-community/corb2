/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.AbstractManager.getAdhocQuery;
import static com.marklogic.developer.corb.Options.INIT_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.XCC_TIME_ZONE;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.getInlineModuleCode;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isInlineModule;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Factory for creating and configuring Task instances for CoRB job execution.
 * <p>
 * TaskFactory is responsible for instantiating and setting up all types of tasks used
 * during a CoRB job:
 * </p>
 * <ul>
 * <li><b>Init Tasks</b> - Execute once before URI processing ({@link #newInitTask()})</li>
 * <li><b>Pre-Batch Tasks</b> - Execute once before batch processing ({@link #newPreBatchTask()})</li>
 * <li><b>Process Tasks</b> - Execute for each URI or batch ({@link #newProcessTask(String...)})</li>
 * <li><b>Post-Batch Tasks</b> - Execute once after batch processing ({@link #newPostBatchTask()})</li>
 * </ul>
 * <p>
 * The factory handles:
 * </p>
 * <ul>
 * <li>Task instantiation from configured classes or default Transform</li>
 * <li>Module resolution (file paths, inline code, adhoc queries)</li>
 * <li>Configuration injection (properties, content sources, URIs)</li>
 * <li>Query language detection (XQuery vs. JavaScript)</li>
 * <li>Module caching for performance</li>
 * </ul>
 * <p>
 * <b>Module Resolution:</b>
 * The factory resolves modules in the following order:
 * </p>
 * <ol>
 * <li>Inline modules: {@code INLINE-XQUERY|xquery code} or {@code INLINE-JAVASCRIPT|js code}</li>
 * <li>Adhoc queries: {@code /path/to/query.xqy|ADHOC} (loaded from classpath or filesystem)</li>
 * <li>Module URIs: {@code /corb/transform.xqy} (resolved with MODULE-ROOT prefix)</li>
 * </ol>
 * <p>
 * <b>Task Configuration:</b>
 * Each task created by the factory is automatically configured with:
 * </p>
 * <ul>
 * <li>Content source pool for database connections</li>
 * <li>Module type (INIT-MODULE, PRE-BATCH-MODULE, PROCESS-MODULE, POST-BATCH-MODULE)</li>
 * <li>Module URI or adhoc query</li>
 * <li>Query language (auto-detected from module extension)</li>
 * <li>Properties (job configuration and custom inputs)</li>
 * <li>Time zone (if configured via XCC-TIME-ZONE)</li>
 * <li>Input URIs (for process tasks)</li>
 * <li>Fail-on-error flag</li>
 * <li>Export directory (for export tasks)</li>
 * </ul>
 * <p>
 * <b>Caching:</b>
 * The factory caches:
 * </p>
 * <ul>
 * <li>Adhoc query content (loaded from files)</li>
 * <li>Resolved module paths (with MODULE-ROOT prefix)</li>
 * </ul>
 * <p>
 * This improves performance by avoiding repeated file I/O and string operations.
 * </p>
 *
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see Task
 * @see Transform
 * @see Manager
 */
public class TaskFactory {

    /** The Manager coordinating the CoRB job, providing access to configuration, content sources, and properties */
    protected Manager manager;
    /** Cache mapping adhoc module specifications to their loaded query content to avoid repeated file I/O */
    private final Map<String, String> moduleToAdhocQueryMap = new HashMap<>();
    /** Cache mapping module paths to their resolved paths (with MODULE-ROOT prefix) to avoid repeated string operations */
    private final Map<String, String> moduleToPathMap = new HashMap<>();
    /** Error message prefix for adhoc query read failures */
    private static final String EXCEPTION_MSG_UNABLE_READ_ADHOC = "Unable to read adhoc query ";
    /** Error message for null content source */
    private static final String EXCEPTION_MSG_NULL_CONTENT = "null content source";

    /**
     * Constructs a TaskFactory for the specified Manager.
     * <p>
     * The factory uses the Manager to access:
     * </p>
     * <ul>
     * <li>TransformOptions (task classes, module paths)</li>
     * <li>ContentSourcePool (database connections)</li>
     * <li>Properties (job configuration)</li>
     * </ul>
     *
     * @param manager the Manager coordinating the CoRB job
     */
    public TaskFactory(Manager manager) {
        this.manager = manager;
    }

    /**
     * Creates a new process task for the specified URIs with default error handling.
     * <p>
     * This is a convenience method equivalent to {@code newProcessTask(uris, true)}.
     * </p>
     *
     * @param uris the URIs for the task to process
     * @return a configured process task
     * @throws NullPointerException if process task/module is not configured or required dependencies are null
     * @throws IllegalArgumentException if task instantiation fails
     * @see #newProcessTask(String[], boolean)
     */
    public Task newProcessTask(String... uris) {
        return newProcessTask(uris, true);
    }

    /**
     * Creates a new process task for the specified URIs.
     * <p>
     * Process tasks are the main workhorses of CoRB jobs, executing for each URI
     * or batch of URIs. The task can be:
     * </p>
     * <ul>
     * <li>A custom class (specified via {@link Options#PROCESS_TASK})</li>
     * <li>The default {@link Transform} class (if only {@link Options#PROCESS_MODULE} is set)</li>
     * </ul>
     * <p>
     * Requirements:
     * </p>
     * <ul>
     * <li>Either PROCESS-TASK or PROCESS-MODULE must be configured</li>
     * <li>If PROCESS-MODULE is set, URIs and ContentSourcePool must be available</li>
     * </ul>
     *
     * @param uris the URIs for the task to process (single URI or batch)
     * @param failOnError whether the task should throw exceptions on errors
     * @return a configured process task ready for execution
     * @throws NullPointerException if process task/module is not configured or required dependencies are null
     * @throws IllegalArgumentException if task instantiation or configuration fails
     * @see Options#PROCESS_TASK
     * @see Options#PROCESS_MODULE
     * @see Transform
     */
    public Task newProcessTask(String[] uris, boolean failOnError) {
        TransformOptions options = manager.getOptions();
        if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
            throw new NullPointerException("null process task and xquery module");
        }
        if (null != options.getProcessModule()
                && (null == uris || uris.length == 0 || null == manager.getContentSourcePool())) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT + " or input uri");
        }
        try {
            Task task = options.getProcessTaskClass() == null ? new Transform() : options.getProcessTaskClass().newInstance();
            setupTask(task, PROCESS_MODULE, options.getProcessModule(), uris, failOnError);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    /**
     * Creates a new pre-batch task.
     * <p>
     * Pre-batch tasks execute once before batch processing begins. Common uses:
     * </p>
     * <ul>
     * <li>Initializing export files with headers ({@link PreBatchUpdateFileTask})</li>
     * <li>Performing setup operations</li>
     * <li>Validating preconditions</li>
     * </ul>
     * <p>
     * The task can be:
     * </p>
     * <ul>
     * <li>A custom class (specified via {@link Options#PRE_BATCH_TASK})</li>
     * <li>The default {@link Transform} class (if only {@link Options#PRE_BATCH_MODULE} is set)</li>
     * <li>null (if neither PRE-BATCH-TASK nor PRE-BATCH-MODULE is configured)</li>
     * </ul>
     *
     * @return a configured pre-batch task, or null if not configured
     * @throws NullPointerException if PRE-BATCH-MODULE is set but ContentSourcePool is null
     * @throws IllegalArgumentException if task instantiation or configuration fails
     * @see Options#PRE_BATCH_TASK
     * @see Options#PRE_BATCH_MODULE
     * @see PreBatchUpdateFileTask
     */
    public Task newPreBatchTask() {
        TransformOptions options = manager.getOptions();
        if (null == options.getPreBatchTaskClass() && null == options.getPreBatchModule()) {
            return null;
        }
        if (null != options.getPreBatchModule() && null == manager.getContentSourcePool()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getPreBatchTaskClass() == null ? new Transform() : options.getPreBatchTaskClass().newInstance();
            setupTask(task, PRE_BATCH_MODULE, options.getPreBatchModule());
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    /**
     * Creates a new post-batch task.
     * <p>
     * Post-batch tasks execute once after batch processing completes. Common uses:
     * </p>
     * <ul>
     * <li>Finalizing export files ({@link PostBatchUpdateFileTask})</li>
     * <li>Sorting and deduplicating results</li>
     * <li>Compressing output files</li>
     * <li>Sending notifications</li>
     * </ul>
     * <p>
     * The task can be:
     * </p>
     * <ul>
     * <li>A custom class (specified via {@link Options#POST_BATCH_TASK})</li>
     * <li>The default {@link Transform} class (if only {@link Options#POST_BATCH_MODULE} is set)</li>
     * <li>null (if neither POST-BATCH-TASK nor POST-BATCH-MODULE is configured)</li>
     * </ul>
     *
     * @return a configured post-batch task, or null if not configured
     * @throws NullPointerException if POST-BATCH-MODULE is set but ContentSourcePool is null
     * @throws IllegalArgumentException if task instantiation or configuration fails
     * @see Options#POST_BATCH_TASK
     * @see Options#POST_BATCH_MODULE
     * @see PostBatchUpdateFileTask
     */
    public Task newPostBatchTask() {
        TransformOptions options = manager.getOptions();
        if (null == options.getPostBatchTaskClass() && null == options.getPostBatchModule()) {
            return null;
        }
        if (null != options.getPostBatchModule() && null == manager.getContentSourcePool()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getPostBatchTaskClass() == null ? new Transform() : options.getPostBatchTaskClass().newInstance();
            setupTask(task, POST_BATCH_MODULE, options.getPostBatchModule());
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    /**
     * Creates a new initialization task.
     * <p>
     * Initialization tasks execute once before URI loading and processing begins.
     * Common uses:
     * </p>
     * <ul>
     * <li>Creating database structures</li>
     * <li>Loading configuration data</li>
     * <li>Validating environment</li>
     * <li>Performing setup operations</li>
     * </ul>
     * <p>
     * The task can be:
     * </p>
     * <ul>
     * <li>A custom class (specified via {@link Options#INIT_TASK})</li>
     * <li>The default {@link Transform} class (if only {@link Options#INIT_MODULE} is set)</li>
     * <li>null (if neither INIT-TASK nor INIT-MODULE is configured)</li>
     * </ul>
     *
     * @return a configured initialization task, or null if not configured
     * @throws NullPointerException if INIT-MODULE is set but ContentSourcePool is null
     * @throws IllegalArgumentException if task instantiation or configuration fails
     * @see Options#INIT_TASK
     * @see Options#INIT_MODULE
     */
    public Task newInitTask() {
        TransformOptions options = manager.getOptions();
        if (null == manager.getOptions().getInitTaskClass() && null == options.getInitModule()) {
            return null;
        }
        if (null != options.getInitModule() && null == manager.getContentSourcePool()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getInitTaskClass() == null ? new Transform() : options.getInitTaskClass().newInstance();
            setupTask(task, INIT_MODULE, options.getInitModule());
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    /**
     * Sets up a task with default error handling.
     * <p>
     * This is a convenience method equivalent to {@code setupTask(task, moduleType, module, uris, true)}.
     * </p>
     *
     * @param task the task to configure
     * @param moduleType the type of module (INIT-MODULE, PRE-BATCH-MODULE, etc.)
     * @param module the module path, inline code, or adhoc query
     * @param uris the URIs for the task to process
     */
    private void setupTask(Task task, String moduleType, String module, String... uris) {
        setupTask(task, moduleType, module, uris, true);
    }

    /**
     * Configures a task with all necessary dependencies and settings.
     * <p>
     * Configuration steps:
     * </p>
     * <ol>
     * <li>Resolve module (inline, adhoc, or URI) and set on task</li>
     * <li>Detect and set query language (XQuery or JavaScript)</li>
     * <li>Set module type for custom input resolution</li>
     * <li>Set content source pool for database connections</li>
     * <li>Set properties (job configuration and custom inputs)</li>
     * <li>Set time zone (if configured)</li>
     * <li>Set input URIs</li>
     * <li>Set fail-on-error flag</li>
     * <li>Set export directory</li>
     * <li>Validate export task configuration</li>
     * </ol>
     * <p>
     * <b>Module Resolution:</b>
     * </p>
     * <ul>
     * <li>Inline modules: Extract code from {@code INLINE-XQUERY|code} or {@code INLINE-JAVASCRIPT|code}</li>
     * <li>Adhoc queries: Load from file path before the pipe: {@code /path/to/query.xqy|ADHOC}</li>
     * <li>Module URIs: Resolve with MODULE-ROOT prefix: {@code /corb/transform.xqy}</li>
     * </ul>
     * <p>
     * Adhoc queries and module paths are cached to avoid repeated I/O and string operations.
     * </p>
     *
     * @param task the task to configure
     * @param moduleType the type of module (INIT-MODULE, PRE-BATCH-MODULE, PROCESS-MODULE, POST-BATCH-MODULE)
     * @param module the module path, inline code, or adhoc query (may be null for Java-only tasks)
     * @param uris the URIs for the task to process (may be empty for non-process tasks)
     * @param failOnError whether the task should throw exceptions on errors
     * @throws IllegalStateException if an adhoc query cannot be read
     * @throws IllegalArgumentException if an ExportBatchToFileTask has no filename configured
     */
    private void setupTask(Task task, String moduleType, String module, String[] uris, boolean failOnError) {
        if (module != null) {
            if (isInlineOrAdhoc(module)) {
                String adhocQuery;
                if (isInlineModule(module)) {
                    adhocQuery = getInlineModuleCode(module);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException(EXCEPTION_MSG_UNABLE_READ_ADHOC + module);
                    }
                } else {
                    adhocQuery = moduleToAdhocQueryMap.get(module);
                    if (adhocQuery == null) {
                        String modulePath = module.substring(0, module.indexOf('|'));
                        adhocQuery = getAdhocQuery(modulePath);
                        if (isEmpty(adhocQuery)) {
                            throw new IllegalStateException(EXCEPTION_MSG_UNABLE_READ_ADHOC + module + " from classpath or filesystem");
                        }
                        moduleToAdhocQueryMap.put(module, adhocQuery);
                    }
                }
                task.setAdhocQuery(adhocQuery);
            } else {
                String modulePath = moduleToPathMap.get(module);
                if (modulePath == null) {
                    String root = manager.getOptions().getModuleRoot();
                    modulePath = buildModulePath(root, module);
                    moduleToPathMap.put(module, modulePath);
                }
                task.setModuleURI(modulePath);
            }
            if (isJavaScriptModule(module)) {
                task.setQueryLanguage("javascript");
            }
        }
        task.setModuleType(moduleType);
        task.setContentSourcePool(manager.getContentSourcePool());

        Properties managerProperties = manager.getProperties();
        task.setProperties(managerProperties);

        String timeZoneId = Options.findOption(managerProperties, XCC_TIME_ZONE);
        if (isNotBlank(timeZoneId)) {
            TimeZone timeZone = TimeZone.getTimeZone(timeZoneId);
            task.setTimeZone(timeZone);
        }
        task.setInputURI(uris);
        task.setFailOnError(failOnError);
        task.setExportDir(manager.getOptions().getExportFileDir());

        if (task instanceof ExportBatchToFileTask) {
            String fileName = ((ExportToFileTask) task).getFileName();
            if (isBlank(fileName)) {
                throw new IllegalArgumentException("No file name for ExportBatchToFileTask");
            }
        }
    }
}
