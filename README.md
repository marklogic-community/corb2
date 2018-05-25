[![Travis-ci gradle build status](https://travis-ci.org/marklogic-community/corb2.svg?branch=development)](https://travis-ci.org/marklogic-community/corb2)
[![CircleCI maven build status](https://circleci.com/gh/marklogic-community/corb2/tree/development.svg?style=shield)](https://circleci.com/gh/marklogic-community/corb2/tree/development)
[![Codecov code coverage](https://codecov.io/gh/marklogic-community/corb2/branch/development/graph/badge.svg)](https://codecov.io/gh/marklogic-community/corb2/branch/development)
[![SonarQube Quality](https://sonarcloud.io/api/project_badges/measure?project=com.marklogic%3Amarklogic-corb%3Adevelopment&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.marklogic%3Amarklogic-corb%3Adevelopment)
[![SonarQube Maintainability](https://sonarcloud.io/api/project_badges/measure?project=com.marklogic%3Amarklogic-corb%3Adevelopment&metric=sqale_rating)](https://sonarcloud.io/component_measures/domain/Maintainability?id=com.marklogic%3Amarklogic-corb%3Adevelopment)
[![Codacy Grade Badge](https://api.codacy.com/project/badge/Grade/c0195f063ae34c7ea17bb4c97ab7ff2c)](https://www.codacy.com/app/mads-hansen/corb2?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=marklogic/corb2&amp;utm_campaign=Badge_Grade)

[![Download the latest version](https://api.bintray.com/packages/marklogic/maven/marklogic-corb/images/download.svg)](https://bintray.com/marklogic/maven/marklogic-corb/_latestVersion)
[![Javadocs](https://www.javadoc.io/badge/com.marklogic/marklogic-corb.svg?color=blue)](https://www.javadoc.io/doc/com.marklogic/marklogic-corb)

### What is CORB?
CORB is a Java tool designed for bulk content-reprocessing of documents stored in [MarkLogic](http://www.marklogic.com/). 
CORB stands for **CO**ntent **R**eprocessing in **B**ulk and is a multi-threaded workhorse tool at your disposal.
In a nutshell, CORB works off a list of documents in a database and performs operations against those documents. 
CORB operations can include generating a report across all documents, manipulating the individual documents, or a combination thereof. 

### User Guide
This document provides a comprehensive overview of CORB and the options available to customize the execution of a CORB job, as well as the [ModuleExecutor Tool](#moduleexecutor-tool), which can be used to execute a single (XQuery or JavaScript) module in MarkLogic.

For additional information, please refer to the [CORB Wiki](https://github.com/marklogic-community/corb2/wiki).

### Downloads

Download the latest release directly from https://github.com/marklogic-community/corb2/releases or resolve dependencies through [Maven Central](http://mvnrepository.com/artifact/com.marklogic/marklogic-corb) or [JCenter](https://bintray.com/marklogic/maven/marklogic-corb/view).

- CORB v2.4.0 (or later) requires Java 8 (or later) to run.
- [CORB v2.3.2](https://github.com/marklogic-community/corb2/releases/tag/2.3.2) is the last release compatable with Java 7 and 6.
- [CORB v2.2.0](https://github.com/marklogic-community/corb2/releases/tag/2.2.0) (or later) requires [marklogic-xcc 8.0.* (or later)](https://developer.marklogic.com/products/xcc) to run.

> Note: marklogic-xcc 8 is backwards compatible to MarkLogic 5 and runs on Java 1.6 or later.


### Getting Help

To get help with CORB

* [Post a question to Stack Overflow](http://stackoverflow.com/questions/ask?tags=marklogic+corb) with the [<code>markogic</code>](https://stackoverflow.com/questions/tagged/marklogic) and [<code>corb</code>](https://stackoverflow.com/questions/tagged/corb) tags.  
* Submit issues or feature requests at https://github.com/marklogic-community/corb2/issues


### Running CORB
The entry point is the main method in the `com.marklogic.developer.corb.Manager` class. CORB requires the MarkLogic XCC JAR in the classpath,
preferably the version that corresponds to the MarkLogic server version, which can be downloaded from https://developer.marklogic.com/products/xcc.
This version has been tested with XCC 9.0.* talking to Marklogic 8 and 9. Use Java 1.8 or later.

CORB needs options specified through one or more of the following mechanisms:

1. command-line parameters
2. Java system properties ex: `-DXCC-CONNECTION-URI=xcc://user:password@localhost:8202`
3. As properties file in the class path specified using `-DOPTIONS-FILE=myjob.properties`. Relative and full file system paths are also supported.

If specified in more than one place, a command line parameter takes precedence over a Java system property, which take precedence over a property from the **OPTIONS-FILE** properties file.

> Note: Any or all of the properties can be specified as Java system properties or key value pairs in properties file.


> Note: CORB exit codes `0` - successful, `0` - nothing to process (ref: EXIT-CODE-NO-URIS), `1` - initialization or connection error and `2` - execution error

> Note: CoRB now supports [Logging Job Metrics](METRICS.md) back to the MarkLogic database log and/or as document in the database.

### Options
Option | Description
---|---
**<a name="INIT-MODULE"></a>INIT-MODULE** | An XQuery or JavaScript module which, if specified, will be invoked prior to **URIS-MODULE**. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively.
**<a name="INIT-TASK"></a>INIT-TASK** | Java Task which, if specified, will be called prior to **URIS-MODULE**. This can be used addition to **INIT-MODULE** for custom implementations.
**<a name="OPTIONS-FILE"></a>OPTIONS-FILE** | A properties file containing any of the CORB options. Relative and full file system paths are supported.
**<a name="PROCESS-MODULE"></a>PROCESS-MODULE** | XQuery or JavaScript to be executed in a batch for each URI from the **URIS-MODULE** or **URIS-FILE**. Module is expected to have at least one external or global variable with name URI. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively. If returning multiple values from a JavaScript module, values must be returned as ValueIterator.
**<a name="PROCESS-TASK"></a>PROCESS-TASK** | <div>Java Class that implements `com.marklogic.developer.corb.Task` or extends `com.marklogic.developer.corb.AbstractTask`. Typically, it can talk to **PROCESS-MODULE** and the do additional processing locally such save a returned value.  <ul><li> `com.marklogic.developer.corb.ExportBatchToFileTask` Generates _**a single file**_, typically used for reports. Writes the data returned by the **PROCESS-MODULE** to a single file specified by **EXPORT-FILE-NAME**. All returned values from entire CORB will be streamed into the single file. If **EXPORT-FILE-NAME** is not specified, CORB uses **URIS\_BATCH\_REF** returned by **URIS-MODULE** as the file name.  <li> `com.marklogic.developer.corb.ExportToFileTask` Generates _**multiple files**_. Saves the documents returned by each invocation of **PROCESS-MODULE** to a separate local file within **EXPORT-FILE-DIR** where the file name for each document will be the based on the URI.</ul>
**<a name="PRE-BATCH-MODULE"></a>PRE-BATCH-MODULE** | An XQuery or JavaScript module which, if specified, will be run before batch processing starts. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively.
**<a name="PRE-BATCH-TASK"></a>PRE-BATCH-TASK** | Java Class that implements `com.marklogic.developer.corb.Task` or extends `com.marklogic.developer.corb.AbstractTask`. If **PRE-BATCH-MODULE** is also specified, the implementation is expected to invoke the XQuery and process the result if any. It can also be specified without **PRE-BATCH-MODULE** and an example of this is to add a static header to a report. <ul><li> `com.marklogic.developer.corb.PreBatchUpdateFileTask` included - Writes the data returned by the **PRE-BATCH-MODULE** to **EXPORT-FILE-NAME**, which can particularly be used to to write dynamic headers for CSV output. Also, if **EXPORT-FILE-TOP-CONTENT** is specified, this task will write this value to the **EXPORT-FILE-NAME** - this option is especially useful for writing fixed headers to reports. If **EXPORT-FILE-NAME** is not specified, CORB uses **URIS\_BATCH\_REF** returned by **URIS-MODULE** as the file name.</li><ul>
**<a name="POST-BATCH-MODULE"></a>POST-BATCH-MODULE** | An XQuery or JavaScript module which, if specified, will be run after batch processing is completed. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively.
**<a name="POST-BATCH-TASK"></a>POST-BATCH-TASK** | Java Class that implements `com.marklogic.developer.corb.Task` or extends `com.marklogic.developer.corb.AbstractTask`. If **POST-BATCH-MODULE** is also specified, the implementation is expected to invoke the XQuery and process the result if any. It can also be specified without **POST-BATCH-MODULE** and an example of this is to add static content to the bottom of the report. <ul><li> `com.marklogic.developer.corb.PostBatchUpdateFileTask` included - Writes the data returned by the **POST-BATCH-MODULE** to **EXPORT-FILE-NAME**. Also, if **EXPORT-FILE-BOTTOM-CONTENT** is specified, this task will write this value to the **EXPORT-FILE-NAME**. If **EXPORT-FILE-NAME** is not specified, CORB uses **URIS\_BATCH\_REF** returned by **URIS-MODULE** as the file name.</li></ul>
**<a name="THREAD-COUNT"></a>THREAD-COUNT** | The number of worker threads. Default is 1.
**<a name="URIS-MODULE"></a>URIS-MODULE** | URI selector module written in XQuery or JavaScript. Expected to return a sequence containing the uris count followed by all the uris. Optionally, it can also return an arbitrary string as a first item in this sequence - refer to **URIS\_BATCH\_REF** section below. XQuery and JavaScript modules need to have .xqy and .sjs extensions respectively. JavaScript modules must return a [ValueIterator](https://docs.marklogic.com/js/ValueIterator).
**<a name="URIS-FILE"></a>URIS-FILE** | If defined instead of **URIS-MODULE**, URIs will be loaded from the file located on the client. There should only be one URI per line. This path may be relative or absolute. For example, a file containing a list of document identifiers can be used as a **URIS-FILE** and the **PROCESS-MODULE** can query for the document based on this document identifier.
**<a name="XCC-CONNECTION-URI"></a>XCC-CONNECTION-URI** | Connection string to MarkLogic XDBC Server. Multiple connection strings can be specified with comma as a separator. 

### Additional options
Option | Description
---|---
**<a name="BATCH-SIZE"></a>BATCH-SIZE** | The number of URIs to be executed in single transform. Default is 1. If more than 1, **PROCESS-MODULE** will receive a delimited string as the `$URI` variable, which needs to be tokenized to get individual URIs. The default delimiter is `;`, which can be overridden with the option **BATCH-URI-DELIM** described below. <br/>**Sample code for transform:**<br/>`declare variable URI as xs:string exernal;`<br/>`let $all-uris := fn:tokenize($URI,";")`  
**<a name="BATCH-URI-DELIM"></a>BATCH-URI-DELIM** | Use if the default delimiter `';'` cannot be used to join multiple URIS when **BATCH-SIZE** is greater than 1.
**<a name="DECRYPTER"></a>DECRYPTER** | The class name of the options value dycrypter, which must implement `com.marklogic.developer.corb.Decrypter`. Encryptable options include **XCC-CONNECTION-URI**, **XCC-USERNAME**, **XCC-PASSWORD**, **XCC-HOSTNAME**, **XCC-PORT**, and **XCC-DBNAME**.
**<a name="COLLECTION-NAME"></a>COLLECTION-NAME** | Value of this parameter will be passed into the URIS-MODULE via external or global variable with the name URIS.
**<a name="COMMAND"></a>COMMAND** | Pause, resume, and stop the execution of CORB. Possible commands include: PAUSE, RESUME, and STOP. If the **COMMAND-FILE** is modified and either there is no **COMMAND** or an invalid value is specified, then execution will RESUME.
**<a name="COMMAND-FILE"></a>COMMAND-FILE** | A properties file used to configure **COMMAND** and **THREAD-COUNT** while CORB is running. For instance, to temporarily pause execution, or to lower the number of threads in order to throttle execution.
**<a name="COMMAND-FILE-POLL-INTERVAL"></a>COMMAND-FILE-POLL-INTERVAL** | Default is 1. The regular interval (seconds) in which the existence of the **COMMAND-FILE** is tested can be controlled by using this property.
**<a name="CONNECTION-POLICY"></a>CONNECTION-POLICY** | Algorithm for balancing load across multiple hosts used by `com.marklogic.developer.corb.DefaultContentSourcePool`. Options include **ROUND-ROBIN**, **RANDOM** and **LOAD**. Default option is **ROUND-ROBIN**. **LOAD** option returns the ContentSource or Connection with least number of active sessions.  
**<a name="CONTENT-SOURCE-POOL"></a>CONTENT-SOURCE-POOL** | Class that implements `com.marklogic.developer.corb.ContentSourcePool` and used to manage ContentSource instances or connections. The default is `com.marklogic.developer.corb.DefaultContentSourcePool`.
**<a name="DISK-QUEUE"></a>DISK-QUEUE** | Boolean value indicating whether the CORB job should spill to disk when a maximum number of URIs have been loaded in memory, in order to control memory consumption and avoid Out of Memory exceptions for extremely large sets of URIs.
**<a name="DISK-QUEUE-MAX-IN-MEMORY-SIZE"></a>DISK-QUEUE-MAX-IN-MEMORY-SIZE** | The maximum number of URIs to hold in memory before spilling over to disk. Default is 1000.
**<a name="DISK-QUEUE-TEMP-DIR"></a>DISK-QUEUE-TEMP-DIR** | The directory where the URIs queue can write to disk when the maximum in-memory items has been exceeded. If not specified then **TEMP-DIR** value will be used. If neither are specified, then the default behavior is to use java.io.tmpdir.
**<a name="ERROR-FILE-NAME"></a>ERROR-FILE-NAME** | Used when FAIL-ON-ERROR is false. If specified true, removes duplicates from the errored URIs along with error messages will be written to this file. Uses BATCH-URI-DELIM or default `';'` to separate URI and error message.
**<a name="EXIT-CODE-NO-URIS"></a>EXIT-CODE-NO-URIS** | Default is 0. Returns this exit code when there is nothing to process.
**<a name="XPORT_FILE_AS_ZIP"></a>EXPORT_FILE_AS_ZIP** | If true, PostBatchUpdateFileTask compresses the output file as a zip file.
**<a name="EXPORT-FILE-BOTTOM-CONTENT"></a>EXPORT-FILE-BOTTOM-CONTENT** | Used by `com.marklogic.developer.corb.PostBatchUpdateFileTask` to append content to **EXPORT-FILE-NAME** after batch process is complete.
**<a name="EXPORT-FILE-DIR"></a>EXPORT-FILE-DIR** | Export directory parameter is used by `com.marklogic.developer.corb.ExportBatchToFileTask` or similar custom task implementations. <br/>Optional: Alternatively, **EXPORT-FILE-NAME** can be specified with a full path.
**<a name="EXPORT-FILE-NAME"></a>EXPORT-FILE-NAME** | Shared file to write output of `com.marklogic.developer.corb.ExportBatchToFileTask` - should be a file name with our without full path. <ul><li>**EXPORT-FILE-DIR** Is not required if a full path is used.</li><li>If **EXPORT-FILE-NAME** is not specified, CORB attempts to use **URIS\_BATCH\_REF** as the file name and this is especially useful in case of automated jobs where file name can only be determined by the **URIS-MODULE** - refer to **URIS\_BATCH\_REF** section below.</li></ul>
**<a name="EXPORT-FILE-PART-EXT"></a>EXPORT-FILE-PART-EXT** | The file extension for export files being processed. ex: .tmp - if specified, `com.marklogic.developer.corb.PreBatchUpdateFileTask` adds this temporary extension to the export file name to indicate **EXPORT-FILE-NAME** is being actively modified. To remove this temporary extension after **EXPORT-FILE-NAME** is complete, `com.marklogic.developer.corb.PostBatchUpdateFileTask` must be specified as **POST-BATCH-TASK**.
**<a name="EXPORT-FILE-SORT"></a>EXPORT-FILE-SORT** | If `ascending` or `descending`, lines will be sorted. If <code>&#124;distinct</code> is specified after the sort direction, duplicate lines from **EXPORT-FILE-NAME** will be removed. i.e. <code>ascending&#124;distinct</code> or <code>descending&#124;distinct</code>
**<a name="EXPORT-FILE-SORT-COMPARATOR"></a>EXPORT-FILE-SORT-COMPARATOR** | A java class that must implement `java.util.Comparator`. If specified, CORB will use this class for sorting in place of ascending or descending string comparator even if a value was specified for **EXPORT-FILE-SORT**.
**<a name="EXPORT-FILE-TOP-CONTENT"></a>EXPORT-FILE-TOP-CONTENT** | Used by `com.marklogic.developer.corb.PreBatchUpdateFileTask` to insert content at the top of **EXPORT-FILE-NAME** before batch process starts. If it includes the string `@URIS_BATCH_REF`, it is replaced by the batch reference returned by **URIS-MODULE**.
**<a name="EXPORT-FILE-URI-TO-PATH"></a>EXPORT-FILE-URI-TO-PATH** | Default is true. Boolean value indicating whether to convert doc URI to a filepath.
**<a name="FAIL-ON-ERROR"></a>FAIL-ON-ERROR** | Boolean value indicating whether the CORB job should fail and exit if a process module throws an error. Default is true. This option will not handle repeated connection failures.
**<a name="INSTALL"></a>INSTALL** | Whether to install the Modules in the Modules database. Specify 'true' or '1' for installation. Default is false.
**<a name="LOADER-BASE64-ENCODE"></a>LOADER-BASE64-ENCODE** | Boolean option specifying whether the content loaded by FileUrisStreamingXMLLoader or FileUrisXMLLoader (with the option `LOADER-USE-ENVELOPE=true`) should be base64 encoded, or appended as the child of the `/corb-loader/content` element. Default is `false`
**<a name="LOADER-PATH"></a>LOADER-PATH** | The path to the resource (file or folder) that will be the input source for a loader class that extends AbstractFileUrisLoader, such as FileUrisDirectoryLoader, FileUrisLoader, FileUrisStreamingXmlLoader, FileUrisXmlLoader, and FileUrisZipLoader
**<a name="LOADER-SET-URIS-BATCH-REF"></a>LOADER-SET-URIS-BATCH-REF** | Boolean option indicating whether a file loader should set the [URIS_BATCH_REF](https://github.com/marklogic-community/corb2#uris_batch_ref). Default is false
**<a name="LOADER-USE-ENVELOPE"></a>LOADER-USE-ENVELOPE** | Boolean value indicating whether FileUris loaders should use an XML envelope, in order to send file metadata in addition to the file content.
**<a name="JOB-NAME"></a>JOB-NAME** | Name of the current Job.
**<a name="JOB-SERVER-PORT"></a>JOB-SERVER-PORT** | Optional port number to start a lightweight HTTP server which can be used to monitor, change the number of threads, and pause/resume the CORB job. Port number must be a valid port(s) or a valid range of ports.  <ul><li>Ex: 9080</li><li> Ex: 9080,9083,9087</li><li> Ex: 9080-9090</li><li> Ex: 9080-9083,9085-9090</li></ul>  The job server will bind to a port from the configured port number(s). By default, if the **JOB-SERVER-PORT** option is not specified, a job server is not started. <p> When a port is specified and available, the job server URL will be logged to the console with both the UI `http://<host>:<port>` and metrics URL `http://<host>:<port>/metrics`. (grep for string *com.marklogic.developer.corb.JobServer logUsage*)  <p>The metrics URL supports the following parameters:<ul><li>**COMMAND**=pause (or resume). </li><li>**CONCISE**=true limits the amound of data returned</li><li>**FORMAT**=json (or xml) returns job stats in the requested format</li><li>**THREAD-COUNT**=<#> will adjust the number of threads for the executing job</li></ul>  
**<a name="MAX_OPTS_FROM_MODULE"></a>MAX_OPTS_FROM_MODULE** | Default is 10. Max number of custom inputs from the URIS-MODULE to other modules.
**<a name="METRICS-COLLECTIONS"></a>METRICS-COLLECTIONS** | Adds the metrics document to the specified collection.|
**<a name="METRICS-DATABASE"></a>METRICS-DATABASE** | Uses the value provided to save the metrics document to the specified Database. The XCC connection specified should have the following privilege `http://marklogic.com/xdmp/privileges/xdmp-invoke`|
**<a name="METRICS-LOG-LEVEL"></a>METRICS-LOG-LEVEL**|String value indicating the log level that the CORB job should use to log metrics to ML Server Error log. Possible values are *none, emergency, alert, critical, error, warning, notice, info, config, debug, fine, finer, finest*. Default value is "none", which means metrics are not logged.|
**<a name="METRICS-MODULE"></a>METRICS-MODULE** | XQuery or JavaScript to be executed at the end of the CORB Job to save the metrics document to the database. There is an XQuery module ([save-metrics.xqy](src/main/resources/save-metrics.xqy)) and a JavaScript module ([saveMetrics.sjs](src/main/resources/saveMetrics.sjs)) provided. You can use these modules as a template to customize the the metrics document saved to the database. XQuery and JavaScript modules need to have '{@code .xqy}' and{@code .sjs} extensions respectively.|
**<a name="METRICS-NUM-FAILED-TRANSACTIONS"></a>METRICS-NUM-FAILED-TRANSACTIONS** | Maximum number of failed transaction to be logged in the metrics. The default value is 0.
**<a name="METRICS-NUM-SLOW-TRANSACTIONS"></a>METRICS-NUM-SLOW-TRANSACTIONS** | Maximum number of slow transaction to be logged in the metrics. The default value is 5.
**<a name="METRICS-ROOT"></a>METRICS-ROOT** | Uses the value provided to as the URI Root for saving the metrics document.|
**<a name="METRICS-SYC-FREQUENCY"></a>METRICS-SYNC-FREQUENCY** | Frequency (in seconds) at which the metrics document needs to be updated in the database. By default the metrics document is not periodically updated and is only written once at the end of the job. |
**<a name="MODULE-ROOT"></a>MODULE-ROOT** | Default is '/'.
**<a name="MODULES-DATABASE"></a>MODULES-DATABASE** | Uses the **XCC-CONNECTION-URI** if not provided; use 0 for file system.
**<a name="NUM-TPS-FOR-ETC"></a>NUM-TPS-FOR-ETC** | Default is 10. Number of recent transactions per second (tps) values used to calculate estimated completion time (ETC).
**<a name="POST-BATCH-MINIMUM-COUNT"></a>POST-BATCH-MINIMUM-COUNT** | The minimum number of results that must be returned for the **POST-BATCH-MODULE** or **POST-BATCH-TASK** to be executed. Default is 1
**<a name="PRE-POST-BATCH-ALWAYS-EXECUTE"></a>PRE-POST-BATCH-ALWAYS-EXECUTE** | Boolean value indicating whether the PRE_BATCH and POST_BATCH module or task should be executed without evaluating how many URIs were returned by the URI selector.
**<a name="PRE-BATCH-MINIMUM-COUNT"></a>PRE-BATCH-MINIMUM-COUNT** | The minimum number of results that must be returned for the **PRE-BATCH-MODULE** or **PRE-BATCH-TASK** to be executed. Default is 1
**<a name="QUERY-RETRY-LIMIT"></a>QUERY-RETRY-LIMIT** | Number of re-query attempts before giving up. Default is 2.
**<a name="QUERY-RETRY-INTERVAL"></a>QUERY-RETRY-INTERVAL** | Time interval, in seconds, between re-query attempts. Default is 20 seconds.
**<a name="QUERY-RETRY-ERROR-CODES"></a>QUERY-RETRY-ERROR-CODES** | A comma separated list of MarkLogic error codes for which a QueryException should be retried.
**<a name="QUERY-RETRY-ERROR-MESSAGE"></a>QUERY-RETRY-ERROR-MESSAGE** | A comma separated list of values that if contained in an exception message a QueryException should be retried.
**<a name="SSL-CONFIG-CLASS"></a>SSL-CONFIG-CLASS** | A java class that must implement `com.marklogic.developer.corb.SSLConfig`. If not specified, CORB defaults to `com.marklogic.developer.corb.TrustAnyoneSSLConfig` for `xccs` connections.
**<a name="URIS-LOADER"></a>URIS-LOADER** | Java class that implements `com.marklogic.developer.corb.UrisLoader`. A custom class to load URIs instead of built-in loaders for **URIS-MODULE** or **URIS-FILE** options. Example: com.marklogic.developer.corb.FileUrisXMLLoader
**<a name="URIS-REDACTED"></a>URIS-REDACTED** | Optional boolean flag indicating whether URIs should be excluded from logging, console, and JobStats metrics. The default value is false.
**<a name="URIS-REPLACE-PATTERN"></a>URIS-REPLACE-PATTERN** | One or more replace patterns for URIs - Used by java to truncate the length of URIs on the client side, typically to reduce java heap size in very large batch jobs, as the CORB java client holds all the URIS in memory while processing is in progress. If truncated, PROCESS-MODULE needs to reconstruct the URI before trying to do `fn:doc()` to fetch the document. <br/>Usage: `URIS-REPLACE-PATTERN=pattern1,replace1,pattern2,replace2,...)`<br/>**Example:**<br/>`URIS-REPLACE-PATTERN=/com/marklogic/sample/,,.xml,` - Replace /com/marklogic/sample/ and .xml with empty strings. So, CORB client only needs to cache the id '1234' instead of the entire URI /com/marklogic/sample/1234.xml. In the transform **PROCESS-MODULE**, we need to do `let $URI := fn:concat("/com/marklogic/sample/",$URI,".xml")`
**<a name="XCC-CONNECTION-RETRY-LIMIT"></a>XCC-CONNECTION-RETRY-LIMIT** | Number attempts to connect to ML before giving up. Default is 3
**<a name="XCC-CONNECTION-RETRY-INTERVAL"></a>XCC-CONNECTION-RETRY-INTERVAL** | Time interval, in seconds, between retry attempts. Default is 60 seconds.
**<a name="XCC-CONNECTION-HOST-RETRY-LIMIT"></a>XCC-CONNECTION-HOST-RETRY-LIMIT** | Number attempts to connect to ML before giving up on a host. If not specified, it defaults to **XCC-CONNECTION-RETRY-LIMIT**
**<a name="XCC-HTTPCOMPLIANT"></a>XCC-HTTPCOMPLIANT** | Optional boolean flag to indicate whether to enable HTTP 1.1 compliance in XCC. If this option is set, the [`xcc.httpcompliant`](https://docs.marklogic.com/guide/xcc/concepts#id_28335) System property will be set.
**<a name="XCC-TIME-ZONE"></a>XCC-TIME-ZONE** | The ID for the TimeZone that should be set on XCC RequestOption. When a value is specified, it is parsed using [`TimeZone.getTimeZone()`](https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html#getTimeZone-java.lang.String-) and set on XCC RequestOption for each Task. Invalid ID values will produce the GMT TimeZone. If not specified, XCC uses the JVM default TimeZone.
**<a name="XML-FILE"></a>XML-FILE** | In order to use this option a class `com.marklogic.developer.corb.FileUrisXMLLoader` has to be specified in the **URIS-LOADER** option. If defined instead of **URIS-MODULE**, XML nodes will be used as URIs from the file located on the client. The file path may be relative or absolute. Default processing will select all of the child elements of the document element (i.e. `/*/*`). The **XML-NODE** option can be specified with an XPath to address a different set of nodes.
**<a name="XML-NODE"></a>XML-NODE** | An XPath to address the nodes to be returned in an **XML-FILE** by the `com.marklogic.developer.corb.FileUrisXMLLoader`. For example, a file containing a list of nodes wrapped by a parent element can be used as a **XML-FILE** and the **PROCESS-MODULE** can unquote the URI string as node to do further processing with the node. If not specified, the default behavior is to select the child elements of the document element (i.e. `/*/*`)
**<a name="XML-SCHEMA"></a>XML-SCHEMA** | Path to a W3C XML Schema to be used by `com.marklogic.developer.corb.FileUrisStreamingXMLLoader` or `com.marklogic.developer.corb.FileUrisXMLLoader` to validate an **XML-FILE**, and used by `com.marklogic.developer.corb.SchemaValidateBatchToFileTask` and `com.marklogic.corb.SchemaValidateToFileTask` post-process tasks to validate documents returned from a process module.
**<a name="XML-TEMP-DIR"></a>XML-TEMP-DIR** | Temporary directory used by `com.marklogic.developer.corb.FileUrisStreamingXMLLoader` to store files extracted from the **XML-FILE**. If not specified, **TEMP-DIR** value will be used. If neither are specified, then the default Java temp directory will be used.

### Alternate XCC connection configuration
Option | Description
---|---
**<a name="XCC-USERNAME"></a>XCC-USERNAME** | Required if **XCC-CONNECTION-URI** is not specified.
**<a name="XCC-PASSWORD"></a>XCC-PASSWORD** | Required if **XCC-CONNECTION-URI** is not specified.
**<a name="XCC-HOSTNAME"></a>XCC-HOSTNAME** | Required if **XCC-CONNECTION-URI** is not specified. Multiple host can be specified with comma as a separator. 
**<a name="XCC-PORT"></a>XCC-PORT** | Required if **XCC-CONNECTION-URI** is not specified.
**<a name="XCC-DBNAME"></a>XCC-DBNAME** | (Optional)
**<a name="XCC-PROTOCOL"></a>XCC-PROTOCOL** | (Optional)

#### URIS\_BATCH\_REF
If a module, including those specified by **PRE-BATCH-MODULE**, **PROCESS-MODULE** or **POST-BATCH-MODULE** have an external or global variable named **URIS\_BATCH\_REF**, the variable will be set to the first **non-numeric** item in the sequence or [ValueIterator](https://docs.marklogic.com/js/ValueIterator) returned by **URIS-MODULE**. This means that, when used, the **URIS-MODULE** must return a sequence or [ValueIterator](https://docs.marklogic.com/js/ValueIterator) with the special string value first, then the URI count, then the sequence of URIs to process.  

As an example, a batch ref can be a link/id of a document that manages the status of the batch job, where pre-batch module updates the status to start and post-batch module can set it to complete. This example can be used to manage status and errors in automated batch jobs.   

ExportBatchToFileTask, PreBatchUpdateFileTask and PostBatchUpdateFileTask use **URIS\_BATCH\_REF** as the file name if **EXPORT-FILE-NAME** is not specified. This is useful for automated jobs where name of the output file name can be determined only by the **URIS-MODULE**.  

### Custom Inputs to XQuery or JavaScript Modules
Any property specified with prefix (with '.') **INIT-MODULE**, **URIS-MODULE**, **PRE-BATCH-MODULE**, **PROCESS-MODULE**, **POST-BATCH-MODULE** will be set as an external variable in the corresponding XQuery module (if that variable is defined as an external string variable in XQuery module). For JavaScript modules the variables need be defined as global variables.  

#### Custom Input Examples:
* `URIS-MODULE.maxLimit=1000` Expects an external string variable  _maxLimit_ in URIS-MODULE XQuery or global variable for JavaScript.  
* `PROCESS-MODULE.startDate=2015-01-01` Expects an external string variable _startDate_ in PROCESS-MODULE XQuery or global variable for JavaScript.  

Alternatively, **URIS-MODULE** can pass custom inputs to **PRE-BATCH-MODULE**, **PROCESS-MODULE**, **POST-BATCH-MODULE** by returning one or more of the property values in above format before the count the of URIs. If the **URIS-MODULE** needs **URIS\_BATCH\_REF** (above) as well, it needs to be just before the URIs count.  

#### Custom Input From URIS-MODULE Example:
```xquery
let $uris := cts:uris()
return ("PROCESS-MODULE.foo=bar","POST-BATCH-MODULE.alpha=10",fn:count($uris),$uris)
```

### Adhoc Modules
Appending `|ADHOC` to the name or path of a XQuery module (with .xqy extension) or JavaScript (with .sjs or .js extension) module will cause the module to be read from the file system and executed in MarkLogic without being uploaded to Modules database. This simplifies running CORB jobs by not requiring deployment of any code to MarkLogic, and makes the set of CORB files and configuration more self contained.   

**INIT-MODULE**, **URIS-MODULE**, **PROCESS-MODULE**, **PRE-BATCH-MODULE** and **POST-BATCH-MODULE** can be specified adhoc by adding the suffix `|ADHOC` for XQuery or JavaScript (with .sjs or .js extension) at the end. Adhoc XQuery or JavaScript remains local to the CORB and is not deployed to MarkLogic. The XQuery or JavaScript module should be in its named file and that file should be available on the file system, including being on the java classpath for CORB.

##### Adhoc Examples:
* `PRE-BATCH-MODULE=adhoc-pre-batch.xqy|ADHOC` adhoc-pre-batch.xqy must be on the classpath or in the current directory.
* `PROCESS-MODULE=/path/to/file/adhoc-transform-module.xqy|ADHOC` XQuery module file with full path in the file system.  
* `URIS-MODULE=adhoc-uris.sjs|ADHOC` Adhoc JavaScript module in the classpath or current directory.

#### Inline Adhoc Modules
It is also possible to set a module option with inline code blocks, rather than a file path. This can be done by prepending either `INLINE-XQUERY|` or `INLINE-JAVASCRIPT|` to the option value, followed by the XQuery or JavaScript code to execute. Inline code blocks are executed as "adhoc" modules and are not uploaded to the Modules database. The `|ADHOC` suffix is optional for inline code blocks.

##### Inline Adhoc Example:
```xquery
URIS-MODULE=INLINE-XQUERY|xquery version '1.0-ml'; let $uris := cts:uris('', ('document'), cts:collection-query('foo')) return (count($uris), $uris)
```
### JavaScript Modules
JavaScript modules are supported with Marklogic 8 and can be used in place of an XQuery module. However, if returning multiple values (ex: URIS-MODULE), values must be returned as a [ValueIterator](https://docs.marklogic.com/js/ValueIterator). MarkLogic JavaScript API has helper functions to convert Arrays into ValueIterator ([`xdmp.arrayValues()`](https://docs.marklogic.com/xdmp.arrayValues)) and inserting values into another ValueIterator ([`fn.insertBefore()`](https://docs.marklogic.com/fn.insertBefore)).

JavaScript module must have an .sjs file extension when deployed to Modules database. However, adhoc JavaScript modules support both .sjs or .js file extensions.

For example, a simple URIS-MODULE may look like this:
```javascript
var uris = cts.uris();
fn.insertBefore(uris,0,fn.count(uris));
```

To return URIS\_BATCH\_REF, we can do the following:
```xquery
fn.insertBefore(fn.insertBefore(uris,0,fn.count(uris)),0,"batch-ref")
```

> Note: Do not use single quotes within (adhoc) JavaScript modules. If you must use a single quote, escape it with a quote (ex: ''text'')

### Encryption
It is often required to protect the database connection string or password from unauthorized access. So, CORB optionally supports encryption of the entire XCC URL or any parts of the XCC URL (if individually specified), such as **XCC-PASSWORD**.

Option | Description
---|---
**<a name="DECRYPTER"></a>DECRYPTER** | Must implement `com.marklogic.developer.corb.Decrypter`. Encryptable options include **XCC-CONNECTION-URI**, **XCC-USERNAME**, **XCC-PASSWORD**, **XCC-HOSTNAME**, **XCC-PORT**, and **XCC-DBNAME** <ul><li>`com.marklogic.developer.corb.PrivateKeyDecrypter` (Included) Requires private key file</li><li>`com.marklogic.developer.corb.JasyptDecrypter` (Included) Requires jasypt-*.jar in classpath</li><li>`com.marklogic.developer.corb.HostKeyDecrypter` (Included) Requires Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files</li></ul>
**<a name="PRIVATE-KEY-FILE"></a>PRIVATE-KEY-FILE**  | Required property for PrivateKeyDecrypter. This file should be accessible in the classpath or on the file system
**<a name="PRIVATE-KEY-ALGORITHM"></a>PRIVATE-KEY-ALGORITHM** | (Optional) <ul><li>Default algorithm for PrivateKeyDecrypter is RSA.</li><li>Default algorithm for JasyptDecrypter is PBEWithMD5AndTripleDES</li><ul>
**<a name="JASYPT-PROPERTIES-FILE"></a>JASYPT-PROPERTIES-FILE** | (Optional) Property file for the JasyptDecrypter. If not specified, it uses default `jasypt.proeprties` file, which should be accessible in the classpath or file system.

#### com.marklogic.developer.corb.PrivateKeyDecrypter
PrivateKeyDecrypter automatically detects if the text is encrypted. Unencrypted text or clear text is returned as-is. Although not required, encrypted text can be optionally enclosed with "ENC" ex: ENC(xxxxxx) to clearly indicate that it is encrypted.  

Generate keys and encrypt XCC URL or password using one of the options below.   

#### Java Crypt
* Use the PrivateKeyDecrypter class inside the CORB JAR with the gen-keys option to generate a key.  
  `java -cp /path/to/lib/* com.marklogic.developer.corb.PrivateKeyDecrypter gen-keys /path/to/private.key /path/to/public.key RSA 1024`  
> Note: if not specified, default algorithm: RSA, default key-length: 1024
* Use the PrivateKeyDecrypter class inside the CORB JAR with the encrypt option to encrypt the clear text such as an xcc URL or password.  
  `java -cp /path/to/lib/* com.marklogic.developer.corb.PrivateKeyDecrypter encrypt /path/to/public.key clearText RSA`  
> Note: if not specified, default algorithm: RSA

#### RSA keys
* `openssl genrsa -out private.pem 1024` Generate a private key in PEM format
* `openssl pkcs8 -topk8 -nocrypt -in private.pem -out private.pkcs8.key` Create a PRIVATE-KEY-FILE in PKCS8 standard for java
* `openssl rsa -in private.pem -pubout > public.key`  Extract public key
* `echo "uri or password" | openssl rsautl -encrypt -pubin -inkey public.key | base64` Encrypt URI or password. Optionally, the encrypted text can be enclosed with "ENC" ex: ENC(xxxxxx)

#### ssh-keygen  
* `ssh-keygen` ex:key as id_rsa after selecting a passphrase
* `openssl pkcs8 -topk8 -nocrypt -in id_rsa -out id_rsa.pkcs8.key` (asks for passphrase)
* `openssl rsa -in id_rsa -pubout > public.key` (asks for passphrase)
* `echo "password or uri" | openssl rsautl -encrypt -pubin -inkey public.key | base64`

#### com.marklogic.developer.corb.JasyptDecrypter
JasyptDecrypter automatically detects if the text is encrypted. Unencrypted text or clear text is returned as-is. Though, not required, encrypted text can be optionally enclosed with "ENC" ex: ENC(xxxxxx) to clearly indicate that it is encrypted.    

Encrypt the URI or password as below. It is assumed that jasypt distribution is available on your machine.   

`jasypt-1.9.2/bin/encrypt.sh input="uri or password" password="passphrase" algorithm="algorithm" (ex: PBEWithMD5AndTripleDES or PBEWithMD5AndDES)`  

**jasypt.properties file**  
```properties
jasypt.algorithm=PBEWithMD5AndTripleDES #(If not specified, default is PBEWithMD5AndTripleDES)
jasypt.password=passphrase  
```

#### com.marklogic.developer.corb.HostKeyDecrypter
HostKeyDecrypter uses internal server identifiers to generate a private key unique to the host server. It then uses that private key as input to AES-258 encryption algorithm. Due to the use of AES-258, it requires JCE Unlimited Strength Jurisdiction Policy Files.
> Note: certain server identifiers used may change in cases of driver installation or if underlying hardware changes. In such cases, passwords will need to be regenerated. Encrypted passwords will be always be unique to the server they are generated on.

Encrypt the password as follows:  
`java -cp /path/to/lib/* com.marklogic.developer.corb.HostKeyDecrypter encrypt clearText`  

To test if server is properly configured to use the HostKeyDecrypter:  
`java -cp /path/to/lib/* com.marklogic.developer.corb.HostKeyDecrypter test`  

### SSL Support
CORB provides support for SSL over XCC. As a prerequisite to enabling CORB SSL support, the XDBC server must be configured to use SSL. It is necessary to specify **XCC-CONNECTION-URI** property with a protocol of 'xccs'. To configure a particular type of SSL configuration use the following property:

Option | Description
---|---
**<a name="SSL-CONFIG-CLASS"></a>SSL-CONFIG-CLASS** | Must implement `com.marklogic.developer.corb.SSLConfig` <ul><li>`com.marklogic.developer.corb.TrustAnyoneSSLConfig` (Included)</li><li>`com.marklogic.developer.corb.TwoWaySSLConfig` (Included) supports 2-way SSL</li></ul>

#### com.marklogic.developer.corb.TrustAnyoneSSLConfig
TrustAnyoneSSLConfig is the default implementation of the SSLContext. It will accept any certificate presented by the MarkLogic server.

#### com.marklogic.developer.corb.TwoWaySSLConfig
TwoWaySSLConfig is more complete and configurable implementation of the SSLContext. It supports SSL with mutual authentication. It is configurable via the following properties:

Option | Description
---|---
**<a name="SSL-PROPERTIES-FILE"></a>SSL-PROPERTIES-FILE** | (Optional) A properties file that can be used to load a common SSL configuration.
**<a name="SSL-KEYSTORE"></a>SSL-KEYSTORE** | Location of the keystore certificate.
**<a name="SSL-KEYSTORE-PASSWORD"></a>SSL-KEYSTORE-PASSWORD** | (Encrytable) Password of the keystore file.
**<a name="SSL-KEY-PASSWORD"></a>SSL-KEY-PASSWORD** | (Encryptable) Password of the private key.
**<a name="SSL-KEYSTORE-TYPE"></a>SSL-KEYSTORE-TYPE** | Type of the keystore such as 'JKS' or 'PKCS12'.
**<a name="SSL-ENABLED-PROTOCOLS"></a>SSL-ENABLED-PROTOCOLS** | (Optional) A comma separated list of acceptable SSL protocols.
**<a name="SSL-CIPHER-SUITES"></a>SSL-CIPHER-SUITES** | A comma separated list of acceptable cipher suites used.

### Load Balancing and Failover with Multiple Hosts
Corb 2.4+ supports load balancing and failover using `com.marklogic.developer.corb.ContentSourcePool`. This is automatically enabled when multiple comma separated values (supports encryption) are specified for for **XCC-CONNECTION-URI** or **XCC-HOSTNAME**.

```properties
XCC-CONNECTION-URI=xcc://hostname1:8000/dbname,xcc://hostname2:8000/dbname,..
```
OR
```properties
XCC-HOST-NAME=hostname1,hostname2,..
```

The default implementation for `com.marklogic.developer.corb.ContentSourcePool` is `com.marklogic.developer.corb.DefaultContentSourcePool`. It uses below options for **CONNECTION-POLICY** for allocating connections to callers. 
* ROUND-ROBIN - (Default) Connections are allocated using round-robin algorithm. 
* RANDOM - Connections are randomly allocated.
* LOAD - Host with least number of active connections is allocated to caller.    

### Query and Connection Retries
Corb automatically retries the requests a given URI when it encounters `com.marklogic.xcc.exceptions.ServerConnectionException` from MarkLogic. If necessary, the number of retry attempts can be configured using **XCC-CONNECTION-RETRY-LIMIT**. If multiple hosts are specified, we can optionally configure retries per each host using **XCC-CONNECTION-HOST-RETRY-LIMIT**. Corb waits at least **XCC-CONNECTION-RETRY-INTERVAL** seconds before a connection is retried on a failed host. 

Corb also supports retries of requests failed due to query errors. This feature is only intended for sporadic query errors which are not specific to a particular URI. A good example may include occasional time out exceptions from MarkLogic when the ML is too busy and request time limit is low. We can configure which queries can be retried using **QUERY-RETRY-ERROR-CODES** or **QUERY-RETRY-ERROR-MESSAGE** (when error codes are not available). If necessary, the number of query retry attempts can be configured using **QUERY-RETRY-LIMIT**. Corb waits at least **QUERY-RETRY-INTERVAL** seconds before retrying a query.

```properties
QUERY-RETRY-ERROR-CODES=XDMP-EXTIME,SVC-EXTIME
QUERY-RETRY-ERROR-MESSAGE=ErrorMsg1,ErrorMsg2
```

### Usage
#### Usage 1 - Command line options:
```
java -server -cp .:marklogic-xcc-8.0.5.jar:marklogic-corb-2.3.2.jar
        com.marklogic.developer.corb.Manager
        XCC-CONNECTION-URI
        [COLLECTION-NAME [PROCESS-MODULE [ THREAD-COUNT [ URIS-MODULE [ MODULE-ROOT
          [ MODULES-DATABASE [ INSTALL [ PROCESS-TASK [ PRE-BATCH-MODULE [ PRE-BATCH-TASK
            [ POST-PROCESS-MODULE [ POST-BATCH-TASK [ EXPORT-FILE-DIR [ EXPORT-FILE-NAME
              [ URIS-FILE ] ] ] ] ] ] ] ] ] ] ] ] ] ] ]
```

#### Usage 2 - Java system properties specifying options:
```
java -server -cp .:marklogic-xcc-8.0.5.jar:marklogic-corb-2.3.2.jar
        -DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ]
        -DPROCESS-MODULE=module-name.xqy -DTHREAD-COUNT=10
        -DURIS-MODULE=get-uris.xqy
        -DPOST-BATCH-PROCESS-MODULE=post-batch.xqy
        -D...
        com.marklogic.developer.corb.Manager
```

#### Usage 3 - Properties file specifying options:
```
java -server -cp .:marklogic-xcc-8.0.5.jar:marklogic-corb-2.3.2.jar
        -DOPTIONS-FILE=myjob.properties com.marklogic.developer.corb.Manager
```
> looks for myjob.properties file in classpath

#### Usage 4 - Combination of properties file with java system properties and command line options:
```
java -server -cp .:marklogic-xcc-8.0.5.jar:marklogic-corb-2.3.2.jar
        -DOPTIONS-FILE=myjob.properties -DTHREAD-COUNT=10
        com.marklogic.developer.corb.Manager XCC-CONNECTION-URI
```

###  Sample myjob.properties
> Note: any of the properties below can be specified as java system property i.e. '-D' option)

##### sample 1 - simple batch
```properties
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy  
PROCESS-MODULE=transform.xqy  
```

##### sample 2 - Use username, password, host and port instead of connection URI
```properties
XCC-USERNAME=username   
XCC-PASSWORD=password   
XCC-HOSTNAME=localhost   
XCC-PORT=9999   
XCC-DBNAME=ML-database   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy  
PROCESS-MODULE=SampleCorbJob.xqy
```

##### sample 3 - simple batch with URIS-FILE (in place of URIS-MODULE)
```properties
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-FILE=input-uris.csv  
PROCESS-MODULE=SampleCorbJob.xqy  
```

##### sample 4 - simple batch with XML-FILE (in place of URIS-MODULE)
```properties
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
XML-FILE=input.xml  
XML-NODE=/rootNode/childNode
URIS-LOADER=com.marklogic.developer.corb.FileUrisXMLLoader
PROCESS-MODULE=SampleCorbJob.xqy  
```

##### sample 5 - report, generates a single file with data from processing each URI
```properties
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB
PROCESS-MODULE=get-data-from-document.xqy   
PROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask   
EXPORT-FILE-NAME=/local/path/to/exportmyfile.csv   
```

##### sample 6 - report with header, add following to sample 4.
```properties
PRE-BATCH-TASK=com.marklogic.developer.corb.PreBatchUpdateFileTask  
EXPORT-FILE-TOP-CONTENT=col1,col2,col3  
```

##### sample 7 - dynamic headers, assuming pre-batch-header.xqy module returns the header row, add the following to sample 4.
```properties
PRE-BATCH-MODULE=pre-batch-header.xqy  
PRE-BATCH-TASK=com.marklogic.developer.corb.PreBatchUpdateFileTask   
```

##### sample 8 - pre and post batch hooks
```properties
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy  
PROCESS-MODULE=transform.xqy  
PRE-BATCH-MODULE=pre-batch.xqy   
POST-BATCH-MODULE=post-batch.xqy   
```

##### sample 9 - adhoc tasks
XQuery modules live local to filesystem where CORB is located. Any XQuery module can be adhoc.
```properties
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy|ADHOC   
PROCESS-MODULE=SampleCorbJob.xqy|ADHOC   
PRE-BATCH-MODULE=/local/path/to/adhoc-pre-batch.xqy|ADHOC
```

##### sample 10 - jasypt encryption
XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and/or XCC-DBNAME properties can be encrypted and optionally enclosed by ENC(). If JASYPT-PROPERTIES-FILE is not specified, it assumes default jasypt.properties.
```properties
XCC-CONNECTION-URI=ENC(encrypted_uri)   
DECRYPTER=com.marklogic.developer.corb.JasyptDecrypter  
```

**sample jasypt.properties**  
```properties
jasypt.password=foo   
jasypt.algorithm=PBEWithMD5AndTripleDES  
```

##### sample 11 - private key encryption with java keys
XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and/or XCC-DBNAME properties can be encrypted and optionally enclosed by ENC()
```properties
XCC-CONNECTION-URI=encrypted_uri    
DECRYPTER=com.marklogic.developer.corb.PrivateKeyDecrypter  
PRIVATE-KEY-FILE=/path/to/key/private.key  
PRIVATE-KEY-ALGORITHM=RSA  
```
##### sample 12 - private key encryption with unix keys
XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and/or XCC-DBNAME properties can be encrypted and optionally enclosed by ENC()
```properties
XCC-CONNECTION-URI=encrypted_uri  
DECRYPTER=com.marklogic.developer.corb.PrivateKeyDecrypter  
PRIVATE-KEY-FILE=/path/to/rsa/key/rivate.pkcs8.key  
```
##### sample 13 - JavaScript modules deployed to modules database
```properties
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB  
URIS-MODULE=get-uris.sjs  
PROCESS-MODULE=transform.sjs  
```
##### sample 14 - Adhoc JavaScript modules
```properties
URIS-MODULE=get-uris.sjs|ADHOC  
PROCESS-MODULE=extract.sjs|ADHOC
```

### ModuleExecutor Tool

Sometimes, a two or more staged CORB job with both a selector and transform isn't necessary to get the job done. Sometimes, only a single query needs to be executed and the output captured to file.  Maybe even to execute only a single query with no output captured?  In these cases, the ModuleExecutor Tool can be used to quickly and efficiently execute your XQuery or JavaScript files.

Like CORB, with Version 8 or higher of the MarkLogic XCC Connection JAR in your classpath, the ModuleExecutor Tool can run JavaScript queries against a MarkLogic8 server.  

So how does the ModuleExecutor Tool differ from CORB?  The key differences are:

+ There is only one stage in the process so it can only run one query unlike CORB which is multi staged (init, pre-batch, uri-module, process-module, post-batch)
+ It is a single threaded application

That's it.  Doesn't seem like a lot but it actually limits its functionality significantly.  So what does the ModuleExecutor Tool have in common with CORB?  Quite a bit:

+ Runs either XQuery or Javascript
+ Supports encryption of passwords using Jasypt
+ Can capture query output to file
+ Can pass custom external variables to the script
+ Supports either ADHOC queries or queries deployed to a modules database
+ Can be configured using:
  - command line program arguments
  - command line -D properties
  - a properties file
  - a combination of any of these

So how do you use it?   For convenience, it can be configured using the same techniques as CORB provides and using the same parameter names. The big difference is that there are far fewer parameters needed and there is a different class used for its execution (`com.marklogic.developer.corb.ModuleExecutor`).

The following parameters are supported and can be used in the same ways as described above for CORB:

+ **PROCESS-MODULE**
+ **PROCESS-MODULE**.*customVariableNameExample*
+ **MODULES-DATABASE**
+ **MODULES-ROOT**
+ **DECRYPTER**
+ **XCC-CONNECTION-URI**
+ **XCC-USERNAME**
+ **XCC-PASSWORD**
+ **XCC-HOSTNAME**
+ **XCC-PORT**
+ **EXPORT-FILE-DIR**
+ **EXPORT-FILE-NAME**

The following are example usages from a Windows console:

##### Usage 1
```
java -cp pathToXCC.jar:pathToCORB.jar com.marklogic.developer.corb.ModuleExecutor  
        xcc://user:password@host:port/[ database ]
        xqueryOrJavascriptModuleName moduleRootName modulesDatabaseName  
        c:\\myPath\\to\\file\\directory\\myFileName
```         
##### Usage 2
```
java -cp pathToXCC.jar:pathToCORB.jar
        -DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ]
        -DPROCESS-MODULE=module-name.xqy
        -DPROCESS-MODULE.collectionName=myCollectionName
        com.marklogic.developer.corb.ModuleExecutor
```         
##### Usage 3
```
java -cp pathToXCC.jar:pathToCORB.jar:pathToJasypt.jar
        -DOPTIONS-FILE=myJob.properties
        com.marklogic.developer.corb.ModuleExecutor
```
Where myJob.properties has:
```properties
MODULES-DATABASE=My-Modules
DECRYPTER=com.marklogic.developer.corb.JasyptDecrypter
PROCESS-MODULE=/test/HelloWorld.xqy
PROCESS-MODULE.lastName=Smith
PROCESS-MODULE.collectionName=myCollectionName
EXPORT-FILE-NAME=C:\\Users\\jon.smith\\Documents\\runXQueryOutput.log
XCC-CONNECTION-URI=ENC(fslfuoifsdofjjwfckmeflkjlj377239843u)
```
##### Usage 4
```
java -cp pathToXCC.jar:pathToCORB.jar:pathToJasypt.jar
        -DOPTIONS-FILE=myJob.properties
        com.marklogic.developer.corb.ModuleExecutor ENC(fslfuoifsdofjjwfckmeflkjlj377239843u)
```
