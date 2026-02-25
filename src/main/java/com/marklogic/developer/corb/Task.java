/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import java.util.concurrent.Callable;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Interface for CoRB tasks that process URIs or perform batch operations.
 * <p>
 * Task defines the contract for all executable units of work in CoRB. Tasks can be:
 * </p>
 * <ul>
 * <li><b>Process Tasks</b> - Execute for each URI or batch of URIs (PROCESS-TASK)</li>
 * <li><b>Initialization Tasks</b> - Execute once before URI processing begins (INIT-TASK)</li>
 * <li><b>Pre-Batch Tasks</b> - Execute once before batch processing starts (PRE-BATCH-TASK)</li>
 * <li><b>Post-Batch Tasks</b> - Execute once after batch processing completes (POST-BATCH-TASK)</li>
 * </ul>
 * <p>
 * Tasks extend {@link Callable} to support concurrent execution in a thread pool.
 * The {@link #call()} method returns an array of strings representing the result,
 * typically URIs that were processed.
 * </p>
 * <p>
 * <b>Common Task Implementations:</b>
 * </p>
 * <ul>
 * <li>{@link AbstractTask} - Base implementation with XCC session management</li>
 * <li>{@link Transform} - Executes XQuery/JavaScript modules against URIs</li>
 * <li>{@link ExportToFileTask} - Exports results to individual files</li>
 * <li>{@link ExportBatchToFileTask} - Exports results to a single batch file</li>
 * <li>{@link PreBatchUpdateFileTask} - Initializes export files with headers</li>
 * <li>{@link PostBatchUpdateFileTask} - Finalizes export files (sorting, compression)</li>
 * </ul>
 * <p>
 * <b>Lifecycle:</b>
 * </p>
 * <ol>
 * <li>Task instance is created by {@link TaskFactory}</li>
 * <li>Configuration methods are called (setContentSourcePool, setProperties, setInputURI, etc.)</li>
 * <li>Task is submitted to the thread pool executor</li>
 * <li>{@link #call()} method is invoked to execute the task</li>
 * <li>Results are collected by {@link Monitor}</li>
 * </ol>
 * <p>
 * <b>Custom Task Implementation:</b>
 * </p>
 * <pre>
 * public class MyCustomTask extends AbstractTask {
 *     {@literal @}Override
 *     public String[] call() throws Exception {
 *         // Custom processing logic
 *         String result = processUri(inputUris[0]);
 *         return new String[]{result};
 *     }
 * }
 * </pre>
 * <p>
 * Configuration example:
 * </p>
 * <pre>
 * PROCESS-TASK=com.example.MyCustomTask
 * PROCESS-MODULE=my-module.xqy
 * THREAD-COUNT=10
 * </pre>
 *
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see AbstractTask
 * @see Transform
 * @see TaskFactory
 * @see Manager
 * @see Callable
 */
public interface Task extends Callable<String[]> {

	/**
	 * Sets the content source pool for database connections.
	 * <p>
	 * The ContentSourcePool provides XCC connections to MarkLogic for executing
	 * queries and modules. Tasks use this pool to obtain sessions for database
	 * operations.
	 * </p>
	 * <p>
	 * This method is called by the framework during task initialization before
	 * {@link #call()} is invoked.
	 * </p>
	 *
	 * @param csp the content source pool for obtaining MarkLogic connections
	 * @see ContentSourcePool
	 * @see com.marklogic.xcc.ContentSource
	 */
	void setContentSourcePool(ContentSourcePool csp);

	/**
	 * Sets the type of module this task will execute.
	 * <p>
	 * Valid module types:
     * </p>
	 * <ul>
	 * <li>{@code INIT-MODULE} - Initialization module</li>
	 * <li>{@code PRE-BATCH-MODULE} - Pre-batch module</li>
	 * <li>{@code PROCESS-MODULE} - Main processing module</li>
	 * <li>{@code POST-BATCH-MODULE} - Post-batch module</li>
	 * </ul>
	 * <p>
	 * The module type determines which configuration options are used and
	 * affects how custom inputs are resolved (e.g., PROCESS-MODULE.varName).
	 * </p>
	 *
	 * @param moduleType the type of module (INIT-MODULE, PRE-BATCH-MODULE, PROCESS-MODULE, or POST-BATCH-MODULE)
	 * @see Options#INIT_MODULE
	 * @see Options#PRE_BATCH_MODULE
	 * @see Options#PROCESS_MODULE
	 * @see Options#POST_BATCH_MODULE
	 */
	void setModuleType(String moduleType);

	/**
	 * Sets the URI of the module to execute.
	 * <p>
	 * The module URI is the path to an XQuery (.xqy) or JavaScript (.sjs) module
	 * in the modules database. The URI is resolved relative to the module root
	 * configured via {@link Options#MODULE_ROOT}.
	 * </p>
	 * <p>
	 * Example: {@code /corb/transform.xqy}
	 * </p>
	 * <p>
	 * This method is mutually exclusive with {@link #setAdhocQuery(String)}.
	 * If an adhoc query is set, the module URI is ignored.
	 * </p>
	 *
	 * @param moduleURI the URI of the module in the modules database
	 * @see Options#MODULE_ROOT
	 * @see Options#MODULES_DATABASE
	 */
	void setModuleURI(String moduleURI);

	/**
	 * Sets an adhoc query to execute instead of a module URI.
	 * <p>
	 * Adhoc queries are XQuery or JavaScript code executed directly without
	 * being stored in a module. The query code can be:
     * </p>
	 * <ul>
	 * <li>Inline code: {@code INLINE-XQUERY|xquery version "1.0-ml"; ...}</li>
	 * <li>File reference: {@code /path/to/query.xqy|ADHOC}</li>
	 * </ul>
	 * <p>
	 * Adhoc queries are useful for:
     * </p>
	 * <ul>
	 * <li>Quick testing without deploying modules</li>
	 * <li>Dynamic query generation</li>
	 * <li>Embedding logic directly in configuration</li>
	 * </ul>
	 * <p>
	 * This method is mutually exclusive with {@link #setModuleURI(String)}.
	 * If set, the adhoc query takes precedence over the module URI.
	 * </p>
	 *
	 * @param adhocQuery the XQuery or JavaScript code to execute
	 * @see com.marklogic.developer.corb.util.StringUtils#isInlineOrAdhoc(String)
	 */
	void setAdhocQuery(String adhocQuery);

	/**
	 * Sets the query language for the module or adhoc query.
	 * <p>
	 * Valid values:
     * </p>
	 * <ul>
	 * <li>{@code xquery} - XQuery (default)</li>
	 * <li>{@code javascript} - Server-Side JavaScript</li>
	 * </ul>
	 * <p>
	 * The language is typically auto-detected from the module extension:
     * </p>
	 * <ul>
	 * <li>.xqy → XQuery</li>
	 * <li>.sjs → JavaScript</li>
	 * </ul>
	 * <p>
	 * This method allows explicit language specification for adhoc queries or
	 * when the extension-based detection is not sufficient.
	 * </p>
	 *
	 * @param language the query language ("xquery" or "javascript")
	 * @see com.marklogic.xcc.RequestOptions#setQueryLanguage(String)
	 */
	void setQueryLanguage(String language);

    /**
     * Sets the time zone for XCC request execution.
     * <p>
     * The time zone affects how date/time values are interpreted in XQuery/JavaScript
     * modules. If not set, XCC uses the JVM's default time zone.
     * </p>
     * <p>
     * This is useful when:
     * </p>
     * <ul>
     * <li>Processing data from different time zones</li>
     * <li>Ensuring consistent date/time handling across environments</li>
     * <li>Working with timestamps that need specific zone interpretation</li>
     * </ul>
     *
     * @param timeZone the time zone for request execution
     * @see Options#XCC_TIME_ZONE
     * @see com.marklogic.xcc.RequestOptions#setTimeZone(TimeZone)
     */
    void setTimeZone(TimeZone timeZone);

	/**
	 * Sets configuration properties for the task.
	 * <p>
	 * Properties include:
     * </p>
	 * <ul>
	 * <li>Job configuration options (EXPORT-FILE-NAME, BATCH-SIZE, etc.)</li>
	 * <li>Custom input variables for modules (PROCESS-MODULE.myVar=value)</li>
	 * <li>Batch reference from URIS-MODULE (accessible via {@link Manager#URIS_BATCH_REF})</li>
	 * <li>System properties</li>
	 * </ul>
	 * <p>
	 * If the URIS-MODULE returns additional data (a batch reference), it is available
	 * in the properties with the key {@link Manager#URIS_BATCH_REF}. This can be used
	 * to pass context or identifiers between the URIS-MODULE and processing tasks.
	 * </p>
	 * <p>
	 * Custom inputs prefixed with the module type (e.g., PROCESS-MODULE.myVar) are
	 * automatically extracted and set as external variables in the XQuery/JavaScript
	 * module.
	 * </p>
	 *
	 * @param props configuration properties for the task
	 * @see Manager#URIS_BATCH_REF
	 * @see Options
	 */
	void setProperties(Properties props);

	/**
	 * Sets the input URI(s) for this task to process.
	 * <p>
	 * For process tasks:
     * </p>
	 * <ul>
	 * <li>When BATCH-SIZE=1 (default): Single URI to process</li>
	 * <li>When BATCH-SIZE&gt;1: Multiple URIs to process as a batch</li>
	 * </ul>
	 * <p>
	 * The URIs are passed as external variables to the module:
     * </p>
	 * <ul>
	 * <li>Single URI: {@code $URI} variable contains the URI string</li>
	 * <li>Multiple URIs: {@code $URI} variable contains a delimited string (default delimiter: ";")</li>
	 * </ul>
	 * <p>
	 * For init, pre-batch, and post-batch tasks, this may be empty or null.
	 * </p>
	 * <p>
	 * Example module handling multiple URIs:
     * </p>
	 * <pre>
	 * declare variable $URI as xs:string external;
	 * let $uris := fn:tokenize($URI, ";")
	 * for $uri in $uris
	 * return ...
	 * </pre>
	 *
	 * @param inputUri one or more URIs for the task to process
	 * @see Options#BATCH_SIZE
	 * @see Options#BATCH_URI_DELIM
	 */
	void setInputURI(String... inputUri);

	/**
	 * Sets whether the task should fail (throw exception) on errors.
	 * <p>
	 * Behavior:
     * </p>
	 * <ul>
	 * <li>{@code true} (default) - Task throws exception on error, job stops</li>
	 * <li>{@code false} - Task logs error, marks URI as failed, job continues</li>
	 * </ul>
	 * <p>
	 * When false, failed URIs are:
     * </p>
	 * <ul>
	 * <li>Tracked in job statistics (failed count)</li>
	 * <li>Optionally written to an error file (ERROR-FILE-NAME)</li>
	 * <li>Included in job metrics</li>
	 * </ul>
	 * <p>
	 * This setting allows jobs to continue processing even when some URIs fail,
	 * which is useful for:
     * </p>
	 * <ul>
	 * <li>Large batch jobs where some failures are acceptable</li>
	 * <li>Generating error reports for later analysis</li>
	 * <li>Ensuring maximum throughput despite errors</li>
	 * </ul>
	 *
	 * @param failOnError true to stop on errors, false to continue
	 * @see Options#FAIL_ON_ERROR
	 * @see Options#ERROR_FILE_NAME
	 */
	void setFailOnError(boolean failOnError);

	/**
	 * Sets the directory for exporting files.
	 * <p>
	 * This directory is used by export tasks to write output files:
     * </p>
	 * <ul>
	 * <li>{@link ExportToFileTask} - Individual files per URI</li>
	 * <li>{@link ExportBatchToFileTask} - Single batch file</li>
	 * <li>{@link PreBatchUpdateFileTask} - File initialization</li>
	 * <li>{@link PostBatchUpdateFileTask} - File finalization</li>
	 * </ul>
	 * <p>
	 * The directory must:
     * </p>
	 * <ul>
	 * <li>Exist or be creatable</li>
	 * <li>Be writable by the CoRB process</li>
	 * <li>Have sufficient disk space for expected output</li>
	 * </ul>
	 *
	 * @param exportFileDir the directory path for export files
	 * @see Options#EXPORT_FILE_DIR
	 * @see Options#EXPORT_FILE_NAME
	 */
	void setExportDir(String exportFileDir);
}
