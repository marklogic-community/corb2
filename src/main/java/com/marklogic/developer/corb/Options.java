/*
  * * Copyright (c) 2004-2017 MarkLogic Corporation
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Properties;

import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;

/**
 * Options that allow users to configure CoRB and control various aspects of execution.
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.3.0
 */
public final class Options {

    /**
     * The number of URIs to be executed in single transform.
     * <p>
     * Default is `1`. If greater than 1, the {@value #PROCESS_MODULE} will
     * receive a delimited string as the URI variable value, which needs to be
     * tokenized to get the individual URIs.
     * </p><p>
     * The default delimiter is {@code ;}, which can be overridden with the
     * option {@value #BATCH_URI_DELIM}.
     * </p>
     * Sample code for transform:
     * <pre>{@code
     * declare variable URI as xs:string external;
     * let $all-uris := fn:tokenize($URI,";")
     * }</pre>
     */
    @Usage(description = "The number of uris to be executed in single transform. "
            + "Default is 1. If greater than 1, the PROCESS-MODULE will "
            + "receive a delimited string as the $URI variable value, which needs "
            + "to be tokenized to get individual URIs. The default delimiter is `;`, "
            + "which can be overridden with the option BATCH-URI-DELIM.")
    public static final String BATCH_SIZE = "BATCH-SIZE";

    /**
     * Use if the default delimiter "{@code ;}" cannot be used to join multiple
     * URIS when {@value #BATCH_SIZE} is greater than 1. Default is `;`.
     */
    @Usage(description = "Use if the default delimiter ';' cannot be used to join "
            + "multiple URIS when BATCH-SIZE is greater than 1. Default is ;")
    public static final String BATCH_URI_DELIM = "BATCH-URI-DELIM";

    /**
     * Value of this parameter will be passed into the {@value #URIS_MODULE} via
     * external or global variable with the name URIS.
     */
    @Usage(description = "Value of this parameter will be passed into the URIS-MODULE "
            + "via external or global variable with the name URIS.")
    public static final String COLLECTION_NAME = "COLLECTION-NAME";

    /**
     * Pause, resume, and stop the execution of CoRB2.
     * <p>
     * Possible commands include: PAUSE, RESUME, and STOP.
     * <p>
     * If the {@value #COMMAND_FILE} is modified and either there is no COMMAND
     * or an invalid value is specified, then execution will RESUME.
     *
     * @since 2.3.0
     */
    @Usage(description = "Pause, resume, and stop the execution of CoRB2. "
            + "Possible commands include: PAUSE, RESUME, and STOP. "
            + "If the COMMAND-FILE is modified and either there is no COMMAND or "
            + "an invalid value is specified, then execution will RESUME.")
    public static final String COMMAND = "COMMAND";

    /**
     * A properties file used to configure {@value #COMMAND} and
     * {@value #THREAD_COUNT} while CoRB2 is running.
     * <p>
     * For instance, to temporarily pause execution, or to lower the number of
     * threads in order to throttle execution.
     *
     * @since 2.3.0
     */
    @Usage(description = "A properties file used to configure COMMAND and THREAD-COUNT while CoRB2 is running. "
            + "For instance, to temporarily pause execution, or to lower the number of threads in order to throttle execution.")
    public static final String COMMAND_FILE = "COMMAND-FILE";

    /**
     * The regular interval (seconds) in which the existence of the
     * {@value #COMMAND_FILE} is tested can be controlled by using this
     * property. Default is 1.
     *
     * @since 2.3.0
     */
    @Usage(description = "The regular interval (seconds) in which the existence of "
            + "the COMMAND-FILE is tested can be controlled by using this property. "
            + "Default is 1.")
    public static final String COMMAND_FILE_POLL_INTERVAL = "COMMAND-FILE-POLL-INTERVAL";

    /**
     * Connection policy for allocating connections to tasks used by DefaultConnectionManager
     *
     * @see com.marklogic.developer.corb.DefaultContentSourcePool
     *
     * @since 2.4.0
     */
    @Usage(description = "Connection policy for allocating connections to tasks while using DefaultConnectionPool. Acceptable values ROUNBD-ROBIN, RANDOM and LOAD. Default is ROUND-ROBIN")
    public static final String CONNECTION_POLICY = "CONNECTION-POLICY";

    /**
     * Java class to manage marklogic connections. If none specified, DefaultConnectionManager is used.
     *
     * @see com.marklogic.developer.corb.ContentSourcePool
     * @see com.marklogic.developer.corb.DefaultContentSourcePool
     *
     * @since 2.4.0
     */
    @Usage(description = "Java class to manage marklogic connections. If none specified, DefaultConnectionPool is used.")
    public static final String CONTENT_SOURCE_POOL="CONTENT-SOURCE-POOL";

    /**
     * Boolean value indicating whether to create a new ContentSource when connection errors are encountered.
     * This can help transparently deal with proxies that have dynamic pools of IP addresses.
     * Default is true.
     *
     * @since 2.5.4
     */
    @Usage(description = "Boolean value indicating whether to create a new ContentSource when connection errors are encountered. " +
        "This can help transparently deal with proxies that have dynamic pools of IP addresses. Default is `true`")
    public static final String CONTENT_SOURCE_RENEW = "CONTENT-SOURCE-RENEW";

    /**
     * The class name of the options value decrypter, which must implement
     * {@link com.marklogic.developer.corb.Decrypter}.
     * <p>
     * Encryptable options include {@value #XCC_CONNECTION_URI}, {@value #XCC_USERNAME},
     * {@value #XCC_PASSWORD}, {@value #XCC_HOSTNAME}, {@value #XCC_PORT}, and
     * {@value #XCC_DBNAME}
     * </p>
     * <ul>
     * <li>{@link com.marklogic.developer.corb.PrivateKeyDecrypter} (Included)
     * Requires private key file</li>
     * <li>{@link com.marklogic.developer.corb.JasyptDecrypter} (Included)
     * Requires jasypt-*.jar in classpath</li>
     * <li>{@link com.marklogic.developer.corb.HostKeyDecrypter} (Included)
     * Requires Java Cryptography Extension (JCE) Unlimited Strength
     * Jurisdiction Policy Files</li>
     * </ul>
     *
     * @see com.marklogic.developer.corb.Decrypter
     * @see com.marklogic.developer.corb.AbstractDecrypter
     * @see #PRIVATE_KEY_ALGORITHM
     * @see #PRIVATE_KEY_FILE
     */
    @Usage(description = "The class name of the options value decrypter, which must "
            + "implement com.marklogic.developer.corb.Decrypter. Encryptable options "
            + "include XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT, and XCC-DBNAME. "
            + "com.marklogic.developer.corb.PrivateKeyDecrypter (Included) Requires private key file. "
            + "com.marklogic.developer.corb.JasyptDecrypter (Included) Requires jasypt-*.jar in classpath. "
            + "com.marklogic.developer.corb.HostKeyDecrypter (Included) Requires "
            + "Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files.")
    public static final String DECRYPTER = "DECRYPTER";

    /**
     * Boolean value indicating whether the CoRB job should spill to disk when a
     * maximum number of URIs have been loaded in memory, in order to control
     * memory consumption and avoid Out of Memory exceptions for extremely large
     * sets of URIs.
     *
     * @since 2.3.1
     * @see #DISK_QUEUE_MAX_IN_MEMORY_SIZE
     * @see #DISK_QUEUE_TEMP_DIR
     */
    @Usage(description = "Boolean value indicating whether the CoRB job should "
            + "spill to disk when a maximum number of URIs have been loaded in "
            + "memory, in order to control memory consumption and avoid Out of "
            + "Memory exceptions for extremely large sets of URIs.")
    public static final String DISK_QUEUE = "DISK-QUEUE";

    /**
     * The maximum number of URIs to hold in memory before spilling over to
     * disk. Default is 1,000.
     *
     * @since 2.3.1
     * @see #DISK_QUEUE
     * @see #DISK_QUEUE_TEMP_DIR
     */
    @Usage(description = "The maximum number of URIs to hold in memory before spilling over to disk. "
            + "Default is 1,000.")
    public static final String DISK_QUEUE_MAX_IN_MEMORY_SIZE = "DISK-QUEUE-MAX-IN-MEMORY-SIZE";

    /**
     * The directory where the URIs queue can write to disk when the maximum
     * in-memory items has been exceeded. Default behavior is to use
     * {@code java.io.tmpdir}.
     *
     * @since 2.3.1
     * @see #DISK_QUEUE
     * @see #DISK_QUEUE_MAX_IN_MEMORY_SIZE
     */
    @Usage(description = "The directory where the URIs queue can write to disk when "
            + "the maximum in-memory items has been exceeded. "
            + "If not specified then **TEMP-DIR** value will be used. "
            + "If neither are specified, then the default Java java.io.tmpdir will be used.")
    public static final String DISK_QUEUE_TEMP_DIR = "DISK-QUEUE-TEMP-DIR";

    /**
     * Used when {@value #FAIL_ON_ERROR} is {@code false}. If specified true,
     * removes duplicates from, the errored URIs along with error messages will
     * be written to this file. Uses {@value #BATCH_URI_DELIM} or default
     * '{@code ;}' to separate URIs and error messages.
     */
    @Usage(description = "Used when FAIL-ON-ERROR is false. If specified true, "
            + "removes duplicates from, the errored URIs along with error messages "
            + "will be written to this file. "
            + "Uses BATCH-URI-DELIM or default ';' to separate URIs and error messages.")
    public static final String ERROR_FILE_NAME = "ERROR-FILE-NAME";

    /**
     * Default is 0. Returns this exit code when there were errors and FAIL-ON-ERROR=false.
     * @since 2.5.3
     */
    @Usage(description = "Default is 0. Returns this exit code when there were errors and FAIL-ON-ERROR=false")
    public static final String EXIT_CODE_IGNORED_ERRORS = "EXIT-CODE-IGNORED-ERRORS";

    /**
     * Default is 0. Returns this exit code when there is nothing to process.
     */
    @Usage(description = "Default is 0. Returns this exit code when there is nothing to process.")
    public static final String EXIT_CODE_NO_URIS = "EXIT-CODE-NO-URIS";

    /**
     * If true, {@link com.marklogic.developer.corb.PostBatchUpdateFileTask}
     * compresses the output file as a zip file.
     */
    @Usage(description = "If true, PostBatchUpdateFileTask compresses the output file as a zip file.")
    public static final String EXPORT_FILE_AS_ZIP = "EXPORT_FILE_AS_ZIP";

    /**
     * Used by {@link com.marklogic.developer.corb.PostBatchUpdateFileTask} to
     * append content to {@value #EXPORT_FILE_NAME} after batch process is
     * complete.
     */
    @Usage(description = "Used by com.marklogic.developer.corb.PostBatchUpdateFileTask "
            + "to append content to EXPORT-FILE-NAME after batch process is complete.")
    public static final String EXPORT_FILE_BOTTOM_CONTENT = "EXPORT-FILE-BOTTOM-CONTENT";

    /**
     * Export directory parameter is used by
     * {@link com.marklogic.developer.corb.ExportBatchToFileTask} or similar
     * custom task implementations.
     * <p>
     * Optional: Alternatively, {@value #EXPORT_FILE_NAME} can be specified with
     * a full path.
     * </p>
     *
     * @see #EXPORT_FILE_NAME
     */
    @Usage(description = "Export directory parameter is used by "
            + "com.marklogic.developer.corb.ExportBatchToFileTask or similar custom task implementations."
            + "Optional: Alternatively, EXPORT-FILE-NAME can be specified with a full path.")
    public static final String EXPORT_FILE_DIR = "EXPORT-FILE-DIR";

    /**
     * Used to track the line count for {@value #EXPORT_FILE_TOP_CONTENT} by
     * {@link com.marklogic.developer.corb.PostBatchUpdateFileTask}.
     */
    @Usage
    public static final String EXPORT_FILE_HEADER_LINE_COUNT = "EXPORT-FILE-HEADER-LINE-COUNT";

    /**
     * Shared file to write output of
     * {@link com.marklogic.developer.corb.ExportBatchToFileTask} - should be a
     * file name with our without full path.
     * <ul>
     * <li>{@value #EXPORT_FILE_DIR} Is not required if a full path is
     * used.</li>
     * <li>If {@value #EXPORT_FILE_NAME} is not specified, CoRB attempts to use
     * {@value #URIS_BATCH_REF} as the file name and this is especially useful
     * in case of automated jobs where file name can only be determined by the
     * {@value #URIS_MODULE} - refer to {@value #URIS_BATCH_REF}.</li>
     * </ul>
     */
    @Usage(description = "Shared file to write output of com.marklogic.developer.corb.ExportBatchToFileTask "
            + "- should be a file name with our without full path."
            + "EXPORT-FILE-DIR Is not required if a full path is used."
            + "If EXPORT-FILE-NAME is not specified, CoRB attempts to use URIS_BATCH_REF "
            + "as the file name and this is especially useful in case of automated "
            + "jobs where file name can only be determined by the URIS-MODULE - refer to URIS_BATCH_REF.")
    public static final String EXPORT_FILE_NAME = "EXPORT-FILE-NAME";

    /**
     * The file extension for export files being processed. For example: ".tmp".
     * <p>
     * If specified, {@link com.marklogic.developer.corb.PreBatchUpdateFileTask}
     * adds this temporary extension to the export file name to indicate
     * {@value #EXPORT_FILE_NAME} is being actively modified. To remove this
     * temporary extension after {@value #EXPORT_FILE_NAME} is complete,
     * {@link com.marklogic.developer.corb.PostBatchUpdateFileTask} must be
     * specified as {@value #POST_BATCH_TASK}.
     */
    @Usage(description = "The file extension for export files being processed. "
            + "ex: .tmp - if specified, com.marklogic.developer.corb.PreBatchUpdateFileTask "
            + "adds this temporary extension to the export file name to indicate "
            + "EXPORT-FILE-NAME is being actively modified. To remove this temporary "
            + "extension after EXPORT-FILE-NAME is complete, com.marklogic.developer.corb.PostBatchUpdateFileTask "
            + "must be specified as POST-BATCH-TASK.")
    public static final String EXPORT_FILE_PART_EXT = "EXPORT-FILE-PART-EXT";

    /**
     * If "{@code ascending}" or "{@code descending}", lines will be sorted. If
     * "{@code |distinct}" is specified after the sort direction, duplicate
     * lines from {@value #EXPORT_FILE_NAME} will be removed. i.e.
     * "{@code ascending|distinct}" or "{@code descending|distinct}"
     *
     * @since 2.2.1
     */
    @Usage(description = "If ascending or descending, lines will be sorted. If '|distinct' "
            + "is specified after the sort direction, duplicate lines from EXPORT-FILE-NAME "
            + "will be removed. i.e. ascending|distinct or descending|distinct")
    public static final String EXPORT_FILE_SORT = "EXPORT-FILE-SORT";

    /**
     * A java class that must implement {@link java.util.Comparator}.
     * <p>
     * If specified, CoRB will use this class for sorting in place of ascending
     * or descending string comparator even if a value was specified for
     * {@value #EXPORT_FILE_SORT}.
     *
     * @since 2.2.1
     */
    @Usage(description = "A java class that must implement java.util.Comparator. "
            + "If specified, CoRB will use this class for sorting in place of ascending "
            + "or descending string comparator even if a value was specified for EXPORT-FILE-SORT.")
    public static final String EXPORT_FILE_SORT_COMPARATOR = "EXPORT-FILE-SORT-COMPARATOR";

    /**
     * Used by {@link com.marklogic.developer.corb.PreBatchUpdateFileTask} to
     * insert content at the top of {@value #EXPORT_FILE_NAME} before batch
     * process starts.
     * <p>
     * If it includes the string "{@code @URIS_BATCH_REF}", it is replaced by
     * the batch reference returned by {@value #URIS_MODULE}.
     */
    @Usage(description = "Used by com.marklogic.developer.corb.PreBatchUpdateFileTask "
            + "to insert content at the top of EXPORT-FILE-NAME before batch process starts. "
            + "If it includes the string @URIS\\_BATCH\\_REF, it is replaced by the "
            + "batch reference returned by URIS-MODULE.")
    public static final String EXPORT_FILE_TOP_CONTENT = "EXPORT-FILE-TOP-CONTENT";

    /**
     * Boolean value indicating whether to convert doc URI to a filepath.
     * Default is true.
     *
     * @since 2.3.0
     */
    @Usage(description = "Boolean value indicating whether to convert doc URI to a filepath. "
            + "Default is true.")
    public static final String EXPORT_FILE_URI_TO_PATH = "EXPORT-FILE-URI-TO-PATH";

    /**
     * Boolean value indicating whether the CoRB job should fail and exit if a
     * process module throws an error.
     * <p>
     * Default is true. This option will not handle repeated connection
     * failures.
     */
    @Usage(description = "Boolean value indicating whether the CoRB job should fail "
            + "and exit if a process module throws an error. Default is true. "
            + "This option will not handle repeated connection failures.")
    public static final String FAIL_ON_ERROR = "FAIL-ON-ERROR";

    /**
     * An XQuery or JavaScript module which, if specified, will be invoked prior
     * to {@value #URIS_MODULE}.
     * <p>
     * XQuery and JavaScript modules need to have "{@code .xqy}" and
     * "{@code .sjs}" extensions respectively.
     */
    @Usage(description = "	An XQuery or JavaScript module which, if specified, "
            + "will be invoked prior to URIS-MODULE. XQuery and JavaScript modules "
            + "need to have .xqy and .sjs extensions respectively.")
    public static final String INIT_MODULE = "INIT-MODULE";

    /**
     * Java Task which, if specified, will be called prior to
     * {@value #URIS_MODULE}. This can be used addition to {@value #INIT_MODULE}
     * for custom implementations.
     *
     * @see com.marklogic.developer.corb.Task
     * @see com.marklogic.developer.corb.AbstractTask
     */
    @Usage(description = "Java Task which, if specified, will be called prior to "
            + "URIS-MODULE. This can be used addition to INIT-MODULE for custom implementations.")
    public static final String INIT_TASK = "INIT-TASK";

    /**
     * Whether to install the Modules in the Modules database. Specify 'true' or
     * '1' for installation. Default is false.
     */
    @Usage(description = "Whether to install the Modules in the Modules database. "
            + "Specify 'true' or '1' for installation. "
            + "Default is false.")
    public static final String INSTALL = "INSTALL";

    /**
     * Name of the current Job.
     * If {@value #JOB_NAME} is specified then the metrics document is added to a collection with the Job Name, if not it defaults to the Job Run Location.
     *
     * @since 2.4.0
     */
    @Usage(description = "Name of the current Job. If it is specified then the metrics document is added to a collection with the Job Name, if not it defaults to the Job Run Location.")
    public static final String JOB_NAME = "JOB-NAME";

    /**
     * Optional Port number to start a light weight HTTP Server which can be used to monitor, change the number of threads, pause/resume the current CORB job.
     * Port number must be a valid port(s) or a valid range of ports. Ex: 9080 Ex: 9080,9083,9087 Ex: 9080-9090 Ex: 9080-9083,9085-9090
     * The job server has a service URL http://host:port/metrics
     * It supports the following params:
     * <ul><li>
     * paused=true|false will pause/resume the Corb job. </li>
     * <li>threads=# will adjust the number of threads for the executing job. </li>
     * <li>json=true returns metrics in json format</li>
     * <li>xml=true returns in xml format</li>
     * <li>concise=true returns a concise format.</li>
     * </ul>
     * By Default if this property is not specified, the Job server is not started.
     *
     * @since 2.4.0
     */
    @Usage(description = "Port number to start a light weight HTTP Server which can be used to monitor ,change the number of threads, pause/resume the current corb job. Port number must be a valid port(s) or a valid range of ports. Ex: 9080 Ex: 9080,9083,9087 Ex: 9080-9090 Ex: 9080-9083,9085-9090")
    public static final String JOB_SERVER_PORT = "JOB-SERVER-PORT";

    /**
     * Boolean option specifying whether the content loaded by
     * FileUrisStreamingXMLLoader or FileUrisXMLLoader (with the option
     * FILE-LOADER-USE-ENVELOPE=true) should be base64 encoded, or appended as
     * the child of the `/corb-loader/content` element.
     * Default is `false`
     *
     * @since 2.4.0
     */
    @Usage(description = "Boolean option specifying whether the content loaded by "
            + "FileUrisStreamingXMLLoader or FileUrisXMLLoader (with the option LOADER-USE-ENVELOPE=true) "
            + "should be base64 encoded, or appended as the child of the `/corb-loader/content` element. "
            + " Default is `false`")
    public static final String LOADER_BASE64_ENCODE = "LOADER-BASE64-ENCODE";

    /**
     * TThe path to the resource (file or folder) that will be the input source for a loader class that extends
     * AbstractFileUrisLoader, such as FileUrisDirectoryLoader, FileUrisLoader, FileUrisStreamingXmlLoader,
     * FileUrisXmlLoader, and FileUrisZipLoader
     *
     * @since 2.4.0
     */
    @Usage(description = "The path to the resource (file or folder) that will be the input source for a loader class that extends AbstractFileUrisLoader, such as FileUrisDirectoryLoader, FileUrisLoader, FileUrisStreamingXmlLoader, FileUrisXmlLoader, and FileUrisZipLoader")
    public static final String LOADER_PATH = "LOADER-PATH";

    /**
     * Boolean option indicating whether a loader should set the
     * #URIS_BATCH_REF(https://github.com/marklogic-community/corb2#uris_batch_ref) with information about the source of the items.
     * Default is false
     *
     * @since 2.4.0
     */
    @Usage(description = "Boolean option indicating whether a loader should set the URIS_BATCH_REF with information about the source of the items. Default is false")
    public static final String LOADER_SET_URIS_BATCH_REF = "LOADER-SET-URIS-BATCH-REF";

    /**
     * Boolean value indicating whether loader should use an XML envelope,
     * in order to send file metadata in addition to the file content.
     * Default is true.
     *
     * @since 2.4.0
     */
    @Usage(description = "Boolean value indicating whether a loader should use an XML envelope, "
            + "in order to send file metadata in addition to the file content. Default is true.")
    public static final String LOADER_USE_ENVELOPE = "LOADER-USE-ENVELOPE";

    /**
     * Specify which external variable to set when invoking the loader process module.
     * Choices are: URI or DOC.
     * Default is URI.
     *
     * @since 2.4.0
     */
    @Usage(description = "Specify which external variable to set when invoking the loader process module. Choices are URI or DOC. Default is URI.")
    public static final String LOADER_VARIABLE = "LOADER-VARIABLE";
    /**
     * (Optional) Property file for the
     * {@link com.marklogic.developer.corb.JasyptDecrypter}.
     * <p>
     * If not specified, it uses default jasypt.proeprties file, which should be
     * accessible in the classpath or file system.
     */
    @Usage(description = "(Optional) Property file for the JasyptDecrypter. "
            + "If not specified, it uses default jasypt.proeprties file, "
            + "which should be accessible in the classpath or file system.")
    public static final String JASYPT_PROPERTIES_FILE = "JASYPT-PROPERTIES-FILE";

    /**
     * Default is 10. Max number of custom inputs from the {@value #URIS_MODULE}
     * to other modules.
     * @since 2.4.5
     */
    @Usage(description = "Default is 10. Max number of custom inputs from the URIS-MODULE to other modules.")
    public static final String MAX_OPTS_FROM_MODULE = "MAX-OPTS-FROM-MODULE";

    /**
     * The variable name that needs to be defined in the server side query to use the metadata set by the {@value #URIS_LOADER}
     *
     * @see #XML_METADATA
     * @since 2.4.5
     */
    @Usage(description = "The external variable name that needs to be defined in the server side query to use the metadata set by the URIS-LOADER")
    public static final String METADATA = "METADATA";

    /**
     * If this option is set to 'true', {@value #XML_METADATA} is set as an external variable with
     * name {@value #METADATA} to {@value #PROCESS_MODULE} as well. Default is 'false'
     *
     * @see #XML_METADATA
     * @since 2.4.5
     */
    @Usage(description = "If this option is set to 'true', XML-METADATA is set as an external variable with "
            + "name METADATA to PROCESS-MODULE as well. Default is 'false'")
    public static final String METADATA_TO_PROCESS_MODULE = "METADATA-TO-PROCESS-MODULE";

    /**
     * Adds the metrics document to the specified collection.
     * If {@value #JOB_NAME} is specified then the metrics document is added to a collection with the Job Name, if not it defaults to the Job Run Location.
     * If {@value #METRICS_DATABASE} is not specified then {@value #METRICS_COLLECTIONS} is ignored.
     *
     * @since 2.4.0
     */
    @Usage(description = "Adds the metrics document to the specified collection.")
    public static final String METRICS_COLLECTIONS = "METRICS-COLLECTIONS";

    /**
     * Uses the value provided to save the metrics document to the specified Database.
     * Does not save metrics document to the ML database if this property is not populated.
     *
     * @since 2.4.0
     */
    @Usage(description = " Uses the value provided to save the metrics document to the specified Database.")
    public static final String METRICS_DATABASE = "METRICS-DATABASE";

    //public static final String METRICS_DOC_FORMAT = "XML";

    /**
     * NONE,INFO,DEBUG,...
     * String value indicating the log level that the CoRB job should use to log metrics to ML Server Error log.
     * Default is `none`.
     *
     * @since 2.4.0
     */
    @Usage(description = "LOG Level the CoRB job should log metrics to ML Server Error Log."
        + "Possible values are one of: " + Options.ML_LOG_LEVELS
        + "Default is none (which means metrics are not logged ).")
    public static final String METRICS_LOG_LEVEL = "METRICS-LOG-LEVEL";

    /**
     * XQuery or JavaScript to be executed at the end of the Corb Job to save the metrics document to the Database.
     * There is an XQuery module (save-metric-to-db.xqy) and a JavaScript module (saveMetrics.sjs) provided with CoRB2 Distribution.
     * The default value is save-metric-to-db.xqy and it saves the metrics document as XML to the specified DB.
     * You can use these modules as a template to customize the the document can be saved to the DB.
     * XQuery and JavaScript modules need to have "{@code .xqy}" and "{@code .sjs}" extensions respectively.
     * If {@value #METRICS_DATABASE} is not specified then {@value #METRICS_MODULE} is ignored.
     *
     * @since 2.4.0
     */
    @Usage(description = "XQuery or JavaScript to be executed at the end of the Corb Job to save the metrics document to the database."
        + "There is an XQuery module (save-metrics.xqy) and a JavaScript module (saveMetrics.sjs) provided with the CoRB2 distribution."
        + "You can use these modules as a template to customize the the document can be saved to the database."
        + "XQuery and JavaScript modules need to have '{@code .xqy}' and"
        + "{@code .sjs} extensions respectively.")
    public static final String METRICS_MODULE = "METRICS-MODULE";

    /**
     * Maximum number of failed transaction to be logged in the metrics.
     * Default is `0`.
     *
     * @since 2.4.0
     */
    @Usage(description = "Maximum number of failed transaction to be logged in the metrics. Default is 0.")
    public static final String METRICS_NUM_FAILED_TRANSACTIONS = "METRICS-NUM-FAILED-TRANSACTIONS";

    /**
     * Maximum number of Slow transaction to be logged in the metrics.
     * Default is `0`.
     */
    @Usage(description = "Maximum number of slow transaction to be logged in the metrics. Default is 0.")
    public static final String METRICS_NUM_SLOW_TRANSACTIONS = "METRICS-NUM-SLOW-TRANSACTIONS";

    /**
     * Uses the value provided as the URI Root for saving the metrics document.
     * Default is "/ServiceMetrics/"
     * If {@value #METRICS_DATABASE} is not specified then {@value #METRICS_ROOT} is ignored.
     *
     * @since 2.4.0
     */
    @Usage(description = "Uses the value provided as the URI Root for saving the metrics document. Default is `/ServiceMetrics/`")
    public static final String METRICS_ROOT = "METRICS-ROOT";

    /**
     * Frequency (in seconds) at which the Metrics document needs to be updated in the database.
     * By Default the metrics document is not periodically updated and is written once at the end of the job.
     * If {@value #METRICS_DATABASE} is not specified then {@value #METRICS_SYNC_FREQUENCY} is ignored.
     */

    @Usage(description = "Frequency ( in seconds) at which the Metrics document needs to be updated in the Database. This value is ignored if METRICS-DB-NAME is not specified")
    public static final String METRICS_SYNC_FREQUENCY = "METRICS-SYNC-FREQUENCY";

    /**
     * A list of the MarkLogic logging levels.
     * @since 2.4.0
     * @see <a href="https://docs.marklogic.com/guide/admin/logfiles#id_37841">https://docs.marklogic.com/guide/admin/logfiles</a>
     */
    protected static final String ML_LOG_LEVELS = "none|emergency|alert|critical|error|warning|notice|info|config|debug|fine|finer|finest";

    /**
     * Uses the {@value #XCC_CONNECTION_URI} if not provided; use 0 for file
     * system.
     */
    @Usage(description = "Uses the XCC-CONNECTION-URI if not provided; use 0 for file system.")
    public static final String MODULES_DATABASE = "MODULES-DATABASE";

    /**
     * Default is '/'.
     */
    @Usage(description = "Default is '/'.")
    public static final String MODULE_ROOT = "MODULE-ROOT";

    /**
     * Default is 10. Max number of recent tps (transaction per second) values
     * used to calculate ETC (estimated time to completion)
     */
    @Usage(description = "Default is 10. Max number of recent tps values used to calculate ETC")
    public static final String NUM_TPS_FOR_ETC = "NUM-TPS-FOR-ETC";

    /**
     * A properties file containing any of the CoRB2 options. Relative and full
     * file system paths are supported.
     */
    @Usage(description = "A properties file containing any of the CoRB2 options. "
            + "Relative and full file system paths are supported.")
    public static final String OPTIONS_FILE = "OPTIONS-FILE";

    /**
     * The minimum number of results that must be returned for the POST-BATCH-MODULE
     *  or POST-BATCH-TASK to be executed.
     * Default is 1
     * @since 2.4.0
     */
    @Usage(description = "The minimum number of results that must be returned "
            + "for the POST-BATCH-MODULE or POST-BATCH-TASK to be executed. Default is 1")
    public static final String POST_BATCH_MINIMUM_COUNT = "POST-BATCH-MINIMUM-COUNT";

    /**
     * An XQuery or JavaScript module which, if specified, will be run after
     * batch processing is completed. XQuery and JavaScript modules need to have
     * "{@code .xqy}" and "{@code .sjs}" extensions respectively.
     */
    @Usage(description = "An XQuery or JavaScript module which, if specified, "
            + "will be run after batch processing is completed. XQuery and JavaScript "
            + "modules need to have .xqy and .sjs extensions respectively.")
    public static final String POST_BATCH_MODULE = "POST-BATCH-MODULE";

    /**
     * Java Class that implements {@link com.marklogic.developer.corb.Task} or
     * extends {@link com.marklogic.developer.corb.AbstractTask}.
     * <p>
     * If {@value #POST_BATCH_MODULE} is also specified, the implementation is
     * expected to invoke the XQuery and process the result if any. It can also
     * be specified without {@value #POST_BATCH_MODULE} and an example of this
     * is to add static content to the bottom of the report.
     * <ul>
     * <li>{@link com.marklogic.developer.corb.PostBatchUpdateFileTask}
     * (included) - Writes the data returned by the {@value #POST_BATCH_MODULE}
     * to {@value #EXPORT_FILE_NAME}. Also, if
     * {@value #EXPORT_FILE_BOTTOM_CONTENT} is specified, this task will write
     * this value to the {@value #EXPORT_FILE_NAME}. If
     * {@value #EXPORT_FILE_NAME} is not specified, CoRB uses
     * {@value #URIS_BATCH_REF} returned by {@value #URIS_MODULE} as the file
     * name.
     * </li>
     * </ul>
     *
     * @see com.marklogic.developer.corb.Task
     * @see com.marklogic.developer.corb.AbstractTask
     * @see com.marklogic.developer.corb.PostBatchUpdateFileTask
     */
    @Usage(description = "Java Class that implements com.marklogic.developer.corb.Task "
            + "or extends com.marklogic.developer.corb.AbstractTask. If POST-BATCH-MODULE "
            + "is also specified, the implementation is expected to invoke the XQuery "
            + "and process the result if any. It can also be specified without POST-BATCH-MODULE "
            + "and an example of this is to add static content to the bottom of the report."
            + "com.marklogic.developer.corb.PostBatchUpdateFileTask (included) "
            + "- Writes the data returned by the POST-BATCH-MODULE to EXPORT-FILE-NAME. "
            + "Also, if EXPORT-FILE-BOTTOM-CONTENT is specified, this task will write "
            + "this value to the EXPORT-FILE-NAME. If EXPORT-FILE-NAME is not specified, "
            + "CoRB uses URIS_BATCH_REF returned by URIS-MODULE as the file name.")
    public static final String POST_BATCH_TASK = "POST-BATCH-TASK";

    /**
     * @deprecated Use the {@link #POST_BATCH_MODULE} option instead.
     * @see #POST_BATCH_MODULE
     */
    @Deprecated
    @Usage
    public static final String POST_BATCH_XQUERY_MODULE = "POST-BATCH-XQUERY-MODULE";

    /**
     * The minimum number of results that must be returned for the PRE-BATCH-MODULE
     * or PRE-BATCH-TASK to be executed.
     * Default is 1
     * @since 2.4.0
     */
    @Usage(description = "The minimum number of results that must be returned "
            + "for the PRE-BATCH-MODULE or PRE-BATCH-TASK to be executed. Default is 1")
    public static final String PRE_BATCH_MINIMUM_COUNT = "PRE-BATCH-MINIMUM-COUNT";

    /**
     * An XQuery or JavaScript module which, if specified, will be run before
     * batch processing starts.
     * <p>
     * XQuery and JavaScript modules need to have "{@code .xqy}" and
     * "{@code .sjs}" extensions respectively.
     */
    @Usage(description = "An XQuery or JavaScript module which, if specified, will "
            + "be run before batch processing starts. XQuery and JavaScript modules "
            + "need to have .xqy and .sjs extensions respectively.")
    public static final String PRE_BATCH_MODULE = "PRE-BATCH-MODULE";

    /**
     * Java Class that implements {@link com.marklogic.developer.corb.Task} or
     * extends {@link com.marklogic.developer.corb.AbstractTask}.
     * <p>
     * If {@value #PRE_BATCH_MODULE} is also specified, the implementation is
     * expected to invoke the XQuery and process the result if any. It can also
     * be specified without {@value #PRE_BATCH_MODULE} and an example of this is
     * to add a static header to a report.
     * <ul>
     * <li>{@link com.marklogic.developer.corb.PreBatchUpdateFileTask}
     * (included) - Writes the data returned by the {@value #PRE_BATCH_MODULE}
     * to {@value #EXPORT_FILE_NAME}, which can particularly be used to to write
     * dynamic headers for CSV output. Also, if
     * {@value #EXPORT_FILE_TOP_CONTENT} is specified, this task will write this
     * value to the {@value #EXPORT_FILE_NAME} - this option is especially
     * useful for writing fixed headers to reports. If
     * {@value #EXPORT_FILE_NAME} is not specified, CoRB uses
     * {@value #URIS_BATCH_REF} returned by {@value #URIS_MODULE} as the file
     * name.
     * </li>
     * </ul>
     *
     * @see com.marklogic.developer.corb.Task
     * @see com.marklogic.developer.corb.AbstractTask
     * @see com.marklogic.developer.corb.PreBatchUpdateFileTask
     */
    @Usage(description = "Java Class that implements com.marklogic.developer.corb.Task "
            + "or extends com.marklogic.developer.corb.AbstractTask. If PRE-BATCH-MODULE "
            + "is also specified, the implementation is expected to invoke the XQuery "
            + "and process the result if any. It can also be specified without PRE-BATCH-MODULE "
            + "and an example of this is to add a static header to a report."
            + "com.marklogic.developer.corb.PreBatchUpdateFileTask included "
            + "- Writes the data returned by the PRE-BATCH-MODULE to EXPORT-FILE-NAME, "
            + "which can particularly be used to to write dynamic headers for CSV output. "
            + "Also, if EXPORT-FILE-TOP-CONTENT is specified, this task will write this "
            + "value to the EXPORT-FILE-NAME - this option is especially useful for writing "
            + "fixed headers to reports. If EXPORT-FILE-NAME is not specified, CoRB uses "
            + "URIS_BATCH_REF returned by URIS-MODULE as the file name.")
    public static final String PRE_BATCH_TASK = "PRE-BATCH-TASK";

    /**
     * @deprecated Use the {@link #PRE_BATCH_MODULE} option instead.
     * @see #PRE_BATCH_MODULE
     */
    @Deprecated
    @Usage
    public static final String PRE_BATCH_XQUERY_MODULE = "PRE-BATCH-XQUERY-MODULE";

    /**
     * Boolean value indicating whether the PRE_BATCH and POST_BATCH module or task
     * should always be executed without evaluating how many URIs were returned by the URI selector.
     * Default is false
     * @see #POST_BATCH_MINIMUM_COUNT
     * @see #PRE_BATCH_MINIMUM_COUNT
     * @since 2.4.0
     */
    @Usage(description="Boolean value indicating whether the PRE_BATCH and POST_BATCH module or task \n" +
        " should always be executed without evaluating how many URIs were returned by the URI selector.\n" +
        " Default is false")
    public static final String PRE_POST_BATCH_ALWAYS_EXECUTE = "PRE-POST-BATCH-ALWAYS-EXECUTE";

    /**
     * (Optional)
     * <ul>
     * <li>Default algorithm for PrivateKeyDecrypter is RSA.</li>
     * <li>Default algorithm for JasyptDecrypter is PBEWithMD5AndTripleDES</li>
     * </ul>
     *
     * @see #PRIVATE_KEY_FILE
     * @see #DECRYPTER
     * @see #JASYPT_PROPERTIES_FILE
     */
    @Usage(description = "(Optional)"
            + "Default algorithm for PrivateKeyDecrypter is RSA."
            + "Default algorithm for JasyptDecrypter is PBEWithMD5AndTripleDES")
    public static final String PRIVATE_KEY_ALGORITHM = "PRIVATE-KEY-ALGORITHM";

    /**
     * Required property for
     * {@link com.marklogic.developer.corb.PrivateKeyDecrypter}. This file
     * should be accessible in the classpath or on the file system
     *
     * @see #DECRYPTER
     * @see #PRIVATE_KEY_ALGORITHM
     * @see #JASYPT_PROPERTIES_FILE
     */
    @Usage(description = "Required property for PrivateKeyDecrypter. This file should "
            + "be accessible in the classpath or on the file system")
    public static final String PRIVATE_KEY_FILE = "PRIVATE-KEY-FILE";

    /**
     * XQuery or JavaScript to be executed in a batch for each URI from the
     * {@value #URIS_MODULE} or {@value #URIS_FILE}.
     * <p>
     * Module is expected to have at least one external or global variable with
     * name URI. XQuery and JavaScript modules need to have "{@code .xqy}" and
     * "{@code .sjs}" extensions respectively. If returning multiple values from
     * a JavaScript module, values must be returned as
     * <a href="https://docs.marklogic.com/js/ValueIterator">ValueIterator</a>.
     */
    @Usage(description = "XQuery or JavaScript to be executed in a batch for each URI "
            + "from the URIS-MODULE or URIS-FILE. Module is expected to have at least "
            + "one external or global variable with name URI. XQuery and JavaScript "
            + "modules need to have .xqy and .sjs extensions respectively. If returning "
            + "multiple values from a JavaScript module, values must be returned as ValueIterator.")
    public static final String PROCESS_MODULE = "PROCESS-MODULE";

    /**
     * Java Class that implements {@link com.marklogic.developer.corb.Task} or
     * extends {@link com.marklogic.developer.corb.AbstractTask}.
     * <p>
     * Typically, it can talk to {@value #PROCESS_MODULE} and the do additional
     * processing locally such save a returned value.
     * <ul>
     * <li>{@link com.marklogic.developer.corb.ExportBatchToFileTask} Generates
     * a single file, typically used for reports. Writes the data returned by
     * the {@value #PROCESS_MODULE} to a single file specified by
     * {@value #EXPORT_FILE_NAME}. All returned values from entire CoRB will be
     * streamed into the single file. If {@value #EXPORT_FILE_NAME} is not
     * specified, CoRB uses {@value #URIS_BATCH_REF} returned by
     * {@value #URIS_MODULE} as the file name.</li>
     * <li>{@link com.marklogic.developer.corb.ExportToFileTask} Generates
     * multiple files. Saves the documents returned by each invocation of
     * {@value #PROCESS_MODULE} to a separate local file within
     * {@value #EXPORT_FILE_DIR} where the file name for each document will be
     * the based on the URI.</li>
     * </ul>
     *
     * @see com.marklogic.developer.corb.Task
     * @see com.marklogic.developer.corb.AbstractTask
     */
    @Usage(description = "Java Class that implements com.marklogic.developer.corb.Task "
            + "or extends com.marklogic.developer.corb.AbstractTask. Typically, it "
            + "can talk to XQUERY-MODULE and the do additional processing locally such save a returned value."
            + "com.marklogic.developer.corb.ExportBatchToFileTask Generates a single file, "
            + "typically used for reports. Writes the data returned by the PROCESS-MODULE "
            + "to a single file specified by EXPORT-FILE-NAME. All returned values from "
            + "entire CoRB will be streamed into the single file. If EXPORT-FILE-NAME is not "
            + "specified, CoRB uses URIS_BATCH_REF returned by URIS-MODULE as the file name."
            + "com.marklogic.developer.corb.ExportToFileTask Generates multiple files. "
            + "Saves the documents returned by each invocation of PROCESS-MODULE to a "
            + "separate local file within EXPORT-FILE-DIR where the file name for each "
            + "document will be the based on the URI.")
    public static final String PROCESS_TASK = "PROCESS-TASK";

    /**
     * A comma separated list of MarkLogic error codes for which a
     * QueryException should be retried.
     *
     * @since 2.3.1
     */
    @Usage(description = "A comma separated list of MarkLogic error codes for which a QueryException should be retried.")
    public static final String QUERY_RETRY_ERROR_CODES = "QUERY-RETRY-ERROR-CODES";

    /**
     * A comma separated list of values that if contained in an exception
     * message a QueryException should be retried.
     *
     * @since 2.3.1
     */
    @Usage(description = "A comma separated list of values that if contained in an exception message a QueryException should be retried.")
    public static final String QUERY_RETRY_ERROR_MESSAGE = "QUERY-RETRY-ERROR-MESSAGE";

    /**
     * Time interval, in seconds, between re-query attempts. Default is 20.
     */
    @Usage(description = "Time interval, in seconds, between re-query attempts. "
            + "Default is 20.")
    public static final String QUERY_RETRY_INTERVAL = "QUERY-RETRY-INTERVAL";

    /**
     * Number of re-query attempts before giving up. Default is 2.
     */
    @Usage(description = "Number of re-query attempts before giving up. "
            + "Default is 2.")
    public static final String QUERY_RETRY_LIMIT = "QUERY-RETRY-LIMIT";

    /**
     * A comma separated list of acceptable cipher suites used.
     */
    @Usage(description = "A comma separated list of acceptable cipher suites used.")
    public static final String SSL_CIPHER_SUITES = "SSL-CIPHER-SUITES";

    /**
     * A java class that must implement
     * {@link com.marklogic.developer.corb.SSLConfig}. If not specified, CoRB
     * defaults to @{link com.marklogic.developer.corb.TrustAnyoneSSLConfig} for
     * xccs connections.
     *
     * @see com.marklogic.developer.corb.SSLConfig
     */
    @Usage(description = "A java class that must implement com.marklogic.developer.corb.SSLConfig. "
            + "If not specified, CoRB defaults to com.marklogic.developer.corb.TrustAnyoneSSLConfig for xccs connections.")
    public static final String SSL_CONFIG_CLASS = "SSL-CONFIG-CLASS";

    /**
     * (Optional) A comma separated list of acceptable SSL protocols
     */
    @Usage(description = "(Optional) A comma separated list of acceptable SSL protocols")
    public static final String SSL_ENABLED_PROTOCOLS = "SSL-ENABLED-PROTOCOLS";

    /**
     * Location of the keystore certificate.
     */
    @Usage(description = "Location of the keystore certificate.")
    public static final String SSL_KEYSTORE = "SSL-KEYSTORE";

    /**
     * (Encryptable) Password of the private key.
     */
    @Usage(description = "(Encryptable) Password of the private key.")
    public static final String SSL_KEY_PASSWORD = "SSL-KEY-PASSWORD";

    /**
     * (Encrytable) Password of the keystore file.
     */
    @Usage(description = "(Encrytable) Password of the keystore file.")
    public static final String SSL_KEYSTORE_PASSWORD = "SSL-KEYSTORE-PASSWORD";

    /**
     * Type of the keystore such as 'JKS' or 'PKCS12'.
     */
    @Usage(description = "Type of the keystore such as 'JKS' or 'PKCS12'.")
    public static final String SSL_KEYSTORE_TYPE = "SSL-KEYSTORE-TYPE";

    /**
     * (Optional) A properties file that can be used to load a common SSL
     * configuration.
     */
    @Usage(description = "(Optional) A properties file that can be used to load a common SSL configuration.")
    public static final String SSL_PROPERTIES_FILE = "SSL-PROPERTIES-FILE";

    /**
     * Path to a directory that can be used for temporary storage while processing records.
     */
    @Usage(description = "Path to a directory that can be used for temporary processing files.")
    public static final String TEMP_DIR = "TEMP-DIR";

    /**
     * The number of worker threads. Default is 1.
     */
    @Usage(description = "The number of worker threads. Default is 1.")
    public static final String THREAD_COUNT = "THREAD-COUNT";

    /**
     * <a href="https://github.com/marklogic-community/corb2#uris_batch_ref">URIS_BATCH_REF</a>
     */
    @Usage
    public static final String URIS_BATCH_REF = "URIS_BATCH_REF";

    /**
    * Variable representing total count of uris set to PRE-BATCH-MODULE and POST-BATCH-MODULE
    *
    * @since 2.4.5
    */
   @Usage
   public static final String URIS_TOTAL_COUNT = "URIS_TOTAL_COUNT";

    /**
     * If defined instead of {@value #URIS_MODULE}, URIs will be loaded from the
     * file located on the client. There should only be one URI per line. This
     * path may be relative or absolute.
     * <p>
     * For example, a file containing a list of document identifiers can be used
     * as a {@value #URIS_FILE} and the {@value #PROCESS_MODULE} can query for
     * the document based on this document identifier.
     */
    @Usage(description = "If defined instead of URIS-MODULE, URIs will be loaded "
            + "from the file located on the client. There should only be one URI per line. "
            + "This path may be relative or absolute. For example, a file containing "
            + "a list of document identifiers can be used as a URIS-FILE and the PROCESS-MODULE "
            + "can query for the document based on this document identifier.")
    public static final String URIS_FILE = "URIS-FILE";

    /**
     * Java class that implements
     * {@link com.marklogic.developer.corb.UrisLoader}. A custom class to load
     * URIs instead of built-in loaders for {@value #URIS_MODULE} or
     * {@value #URIS_FILE} options.
     * <p>
     * Example: {@link com.marklogic.developer.corb.FileUrisXMLLoader}
     *
     * @see com.marklogic.developer.corb.AbstractUrisLoader
     * @see com.marklogic.developer.corb.FileUrisLoader
     * @see com.marklogic.developer.corb.FileUrisXMLLoader
     * @see com.marklogic.developer.corb.UrisLoader
     *
     */
    @Usage(description = "Java class that implements com.marklogic.developer.corb.UrisLoader. "
            + "A custom class to load URIs instead of built-in loaders for URIS-MODULE "
            + "or URIS-FILE options. Example: com.marklogic.developer.corb.FileUrisXMLLoader")
    public static final String URIS_LOADER = "URIS-LOADER";

    /**
     * URI selector module written in XQuery or JavaScript. Expected to return a
     * sequence containing the URIs count followed by all the URIs. Optionally,
     * it can also return an arbitrary string as a first item in this sequence -
     * refer to
     * <a href="https://github.com/marklogic-community/corb2#uris_batch_ref">URIS_BATCH_REF</a>
     * section.
     * <p>
     * XQuery and JavaScript modules need to have "{@code .xqy}" and
     * "{@code .sjs}" extensions respectively. JavaScript modules must return a
     * <a href="https://docs.marklogic.com/js/ValueIterator">ValueIterator</a>.
     */
    @Usage(description = "URI selector module written in XQuery or JavaScript. "
            + "Expected to return a sequence containing the URIs count followed by all the URIs. "
            + "Optionally, it can also return an arbitrary string as a first item in this sequence - "
            + "refer to URIS_BATCH_REF section below. XQuery and JavaScript modules "
            + "need to have .xqy and .sjs extensions respectively. "
            + "JavaScript modules must return a ValueIterator.")
    public static final String URIS_MODULE = "URIS-MODULE";

    /**
     * Optional boolean flag indicating whether URIs should be excluded from logging, console, and JobStats metrics.
     * Default is `false`.
     * @since 2.4.1
     */
    @Usage(description = "Optional boolean flag indicating whether URIs should be excluded from logging, console, " +
        "and JobStats metrics. Default is false.")
    public static final String URIS_REDACTED = "URIS-REDACTED";

    /**
     * One or more replace patterns for URIs - Used by java to truncate the
     * length of URIs on the client side, typically to reduce java heap size in
     * very large batch jobs, as the CoRB java client holds all the URIS in
     * memory while processing is in progress.
     * <p>
     * If truncated, {@value #PROCESS_MODULE} needs to reconstruct the URI
     * before trying to use {@code fn:doc()} to fetch the document.
     * <p>
     * Usage:
     * {@code URIS_REPLACE_PATTERN=pattern1,replace1,pattern2,replace2,...)}
     * <p>
     * <b>Example:</b>
     * {@code URIS-REPLACE-PATTERN=/com/marklogic/sample/,,.xml}, - Replaces
     * "{@code /com/marklogic/sample/}" and "{@code .xml}" with empty strings.
     * So, the CoRB client only needs to cache the id "{@code 1234}" instead of
     * the entire URI "{@code /com/marklogic/sample/1234.xml}".
     * <P>
     * In the transform {@value #PROCESS_MODULE}, we need to do
     * {@code let $URI := fn:concat("/com/marklogic/sample/",$URI,".xml")}.
     */
    @Usage(description = "One or more replace patterns for URIs - Used by java "
            + "to truncate the length of URIs on the client side, typically to "
            + "reduce java heap size in very large batch jobs, as the CoRB java "
            + "client holds all the URIS in memory while processing is in progress. "
            + "If truncated, PROCESS-MODULE needs to reconstruct the URI before "
            + "trying to use fn:doc() to fetch the document. "
            + "Usage: URIS-REPLACE-PATTERN=pattern1,replace1,pattern2,replace2,...)"
            + "Example:"
            + "URIS-REPLACE-PATTERN=/com/marklogic/sample/,,.xml, - Replace /com/marklogic/sample/ "
            + "and .xml with empty strings. So, CoRB client only needs to cache the id '1234' "
            + "instead of the entire URI /com/marklogic/sample/1234.xml. In the transform "
            + "PROCESS-MODULE, we need to do let $URI := fn:concat(\"/com/marklogic/sample/\",$URI,\".xml\")")
    public static final String URIS_REPLACE_PATTERN = "URIS-REPLACE-PATTERN";

    /**
     * Indicate whether or not the XCC connection string components should be URL encoded. Possible values are always, never, and auto. Default is `auto`.
     * @since 2.5.0
     */
    @Usage(description = "Indicate whether or not the XCC connection string components should be URL encoded. Possible values are always, never, and auto. Default is auto.")
    public static final String XCC_URL_ENCODE_COMPONENTS = "XCC-URL-ENCODE-COMPONENTS";

    /**
     * Number attempts to connect to ML before giving up. Default is 3
     */
    @Usage(description = "Number attempts to connect to ML before giving up. "
            + "Default is 3")
    public static final String XCC_CONNECTION_RETRY_LIMIT = "XCC-CONNECTION-RETRY-LIMIT";

    /**
     * Time interval, in seconds, between retry attempts. Default is 60.
     */
    @Usage(description = "Time interval, in seconds, between retry attempts. "
            + "Default is 60.")
    public static final String XCC_CONNECTION_RETRY_INTERVAL = "XCC-CONNECTION-RETRY-INTERVAL";

    /**
     * Number attempts to connect to ML before giving up on a given host. Default is 3
     */
    @Usage(description = "Number attempts to connect to ML before giving up. "
            + "Default is XCC-CONNECTION-RETRY-LIMIT")
    public static final String XCC_CONNECTION_HOST_RETRY_LIMIT = "XCC-CONNECTION-HOST-RETRY-LIMIT";

    /**
     * Connection string to MarkLogic XDBC Server.
     */
    @Usage(description = "Connection string to MarkLogic XDBC Server. Supports multiple connection strings separated by comma.")
    public static final String XCC_CONNECTION_URI = "XCC-CONNECTION-URI";

    /**
     * (Optional) Name of the content database to execute against
     */
    @Usage(description = "(Optional) Name of the content database to execute against")
    public static final String XCC_DBNAME = "XCC-DBNAME";

    /**
     * Required if {@value #XCC_CONNECTION_URI} is not specified.
     */
    @Usage(description = "Required if XCC-CONNECTION-URI is not specified. Supports multiple hostnames separated by comma")
    public static final String XCC_HOSTNAME = "XCC-HOSTNAME";

    /**
     * Optional boolean flag to indicate whether to enable HTTP 1.1 compliance in XCC.
     * If this option is set, the "xcc.httpcompliant" System property will be set. Default true
     *
     * @see <a href="https://docs.marklogic.com/guide/xcc/concepts#id_28335">XCC
     * Developer's Guide</a>
     * @since 2.4.0
     */
    @Usage(description = "Optional boolean flag to indicate whether to enable HTTP 1.1 Compliance in XCC. "
            + "If this option is set, the \"xcc.httpcompliant\" System property will be set.")
    public static final String XCC_HTTPCOMPLIANT = "XCC-HTTPCOMPLIANT";

    /**
     * Required if {@value #XCC_CONNECTION_URI} is not specified.
     */
    @Usage(description = "Required if XCC-CONNECTION-URI is not specified.")
    public static final String XCC_PASSWORD = "XCC-PASSWORD";

    /**
     * Required if {@value #XCC_CONNECTION_URI} is not specified.
     */
    @Usage(description = "Required if XCC-CONNECTION-URI is not specified.")
    public static final String XCC_PORT = "XCC-PORT";

    /**
     * Optional if {@value #XCC_CONNECTION_URI} is not specified. The XCC scheme to use; either xcc or xccs. Default is xcc.
     */
    @Usage(description = "Used if XCC-CONNECTION-URI is not specified. The XCC scheme to use; either xcc or xccs. Default is xcc")
    public static final String XCC_PROTOCOL = "XCC-PROTOCOL";

    /**
     * The ID for the TimeZone that should be set on XCC RequestOption. When a
     * value is specified, it is parsed using TimeZone.getTimeZone() and set on
     * XCC RequestOption for each Task. Invalid ID values will produce the GMT
     * TimeZone. If not specified, XCC uses the JVM default TimeZone.
     *
     * @see java.util.TimeZone
     * @since 2.4.0
     */
    @Usage(description = "The ID for the TimeZone that should be set on XCC RequestOption. When a\n"
            + " value is specified, it is parsed using TimeZone.getTimeZone() and set on\n"
            + " XCC RequestOption for each Task. Invalid ID values will produce the GMT\n"
            + " TimeZone. If not specified, XCC uses the JVM default TimeZone.")
    public static final String XCC_TIME_ZONE = "XCC-TIME-ZONE";

    /**
     * Required if {@value #XCC_CONNECTION_URI} is not specified.
     */
    @Usage(description = "Required if XCC-CONNECTION-URI is not specified.")
    public static final String XCC_USERNAME = "XCC-USERNAME";

    /**
     * In order to use this option a class
     * {@link com.marklogic.developer.corb.FileUrisXMLLoader} has to be
     * specified in the {@value #URIS_LOADER} option. If defined instead of
     * {@value #URIS_MODULE}, XML nodes will be used as URIs from the file
     * located on the client. The file path may be relative or absolute.
     * <p>
     * Default processing will select all of the child elements of the document
     * element (i.e. {@code \/*\/*)}.
     * <p>
     * The {@value #XML_NODE} option can be specified with an XPath to address a
     * different set of nodes.
     *
     * @see #XML_NODE
     * @since 2.3.1
     */
    @Usage(description = "In order to use this option a class com.marklogic.developer.corb.FileUrisXMLLoader "
            + "has to be specified in the URIS-LOADER option. If defined instead of "
            + "URIS-MODULE, XML nodes will be used as URIs from the file located on the client. "
            + "The file path may be relative or absolute. Default processing will "
            + "select all of the child elements of the document element (i.e. /*/*). "
            + "The XML-NODE option can be specified with an XPath to address a different set of nodes.")
    public static final String XML_FILE = "XML-FILE";

    /**
     * An XPath to address the node that contains metadata portion of the XML. This must be different from
     * the {@value #XML_NODE}. If the implementation supports, multiple comma separated paths can be specified.
     * The metadata is set as an external variable with name {@value #METADATA} to {@value #PRE_BATCH_MODULE} and
     * {@value #POST_BATCH_MODULE} and also {@value #PROCESS_MODULE} if enabled by {@value #METADATA_TO_PROCESS_MODULE}
     *
     * @see #XML_FILE
     * @since 2.4.5
     */
    @Usage(description = "An XPath to address the node that contains metadata portion of the XML. This must be different from "
            + "the XML-NODE. The metadata is set as an external variable with name METADATA to PRE-BATCH-MODULE and "
            + "AND POST-BATCH-MODULE and also PROCESS-MODULE if enabled by METADATA-TO-PROCESS-MODULE")
    public static final String XML_METADATA = "XML-METADATA";


    /**
     * An XPath to address the nodes to be returned in an {@value #XML_FILE} by
     * the {@link com.marklogic.developer.corb.FileUrisXMLLoader}.
     * <p>
     * For example, a file containing a list of nodes wrapped by a parent
     * element can be used as a {@value #XML_FILE} and the
     * {@value #PROCESS_MODULE} can unquote the URI string as node to do further
     * processing with the node.
     * <p>
     * If not specified, the default behavior is to select the child elements of
     * the document element (i.e. {@code \/*\/*)}
     *
     * @see #XML_FILE
     * @since 2.3.1
     */
    @Usage(description = "An XPath to address the nodes to be returned in an XML-FILE "
            + "by the com.marklogic.developer.corb.FileUrisXMLLoader. For example, "
            + "a file containing a list of nodes wrapped by a parent element can "
            + "be used as a XML-FILE and the XQUERY-MODULE can unquote the URI "
            + "string as node to do further processing with the node. If not specified, "
            + "the default behavior is to select the child elements of the document element (i.e. /*/*)")
    public static final String XML_NODE = "XML-NODE";

    /**
     * @since 2.4.0
     */
    @Usage(description = "Path to a W3C XML Schema to be used by com.marklogic.developer.corb.FileUrisStreamingXMLLoader "
            + "or com.marklogic.developer.corb.FileUrisXMLLoader to validate an XML-FILE.")
    public static final String XML_SCHEMA = "XML-SCHEMA";

    /**
     * Boolean value indicating whether to set the feature http://apache.org/xml/features/honour-all-schemaLocations. Default is true
     * @see <a href="https://xerces.apache.org/xerces2-j/features.html#honour-all-schemaLocations">https://xerces.apache.org/xerces2-j/features.html#honour-all-schemaLocations</a>
     * @since 2.5.2
     */
    @Usage(description = "Boolean value indicating whether to set the feature http://apache.org/xml/features/honour-all-schemaLocations. Default is true")
    public static final String XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS = "XML-SCHEMA-HONOUR-ALL-SCHEMALOCATIONS";


    /**
     *
     * @since 2.4.0
     */
    @Usage(description = "Temporary directory used by com.marklogic.developer.corb.FileUrisStreamingXMLLoader to store " +
        "files extracted from the XML-FILE. " +
        "If not specified, TEMP-DIR value will be used. " +
        "If not specified, then the default Java java.tmp.dir will be used.")
    public static final String XML_TEMP_DIR = "XML-TEMP-DIR";

    @Usage(description = "In order to use this option a class com.marklogic.developer.corb.FileUrisZipLoader "
            + "has to be specified in the URIS-LOADER option. If defined instead of "
            + "URIS-MODULE, each file will be base64 encoded and set as the "
            + "content of corb-loader XML files and sent as a serialized string in the URI parameter of "
            + "the process module. "
            + "The zip file path may be relative or absolute. Default processing will "
            + "select all of the files in the zip file. ")
    public static final String ZIP_FILE = "ZIP-FILE";

    /**
     *
     * @deprecated Use the {@link #PROCESS_MODULE} option instead.
     * @see #PROCESS_MODULE
     */
    @Deprecated
    @Usage(description = "Use PROCESS-MODULE instead")
    public static final String XQUERY_MODULE = "XQUERY-MODULE";

    private Options() {
    }

    /**
     * Look for the property first in System properties, then in the properties object specified.
     * Look for the propertyName as specified, then normalized to snake_case, and then kebab-case.
     * This ensures that properties are found if specified with either case, and ensures that old jobs still work since
     * renaming the MAX_OPTS_FROM_MODULE option to MAX-OPTS_FROM-MODULE.
     * @param properties
     * @param propertyName
     * @return
     * @since 2.5.1
     */
    public static String findOption(final Properties properties, final String propertyName) {
        if (isNotBlank(propertyName)) {
            final String snakeCase = propertyName.replace("-", "_");
            final String kebabCase = propertyName.replace("_", "-");
            for (String key : Arrays.asList(propertyName, snakeCase, kebabCase)) {
                if (System.getProperty(key) != null) {
                    return System.getProperty(key).trim();
                } else if (properties.containsKey(key) && properties.getProperty(key) != null) {
                    return properties.getProperty(key).trim();
                }
            }
        }
        return null;
    }
}

/**
 * Annotation used to document attributes of the CoRB Options.
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@interface Usage {

    String description() default "";

}
