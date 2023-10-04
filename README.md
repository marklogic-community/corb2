[![Maven Central - download the latest version](https://maven-badges.herokuapp.com/maven-central/com.marklogic/marklogic-corb/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.marklogic/marklogic-corb)
[![Codecov code coverage](https://codecov.io/gh/marklogic-community/corb2/branch/development/graph/badge.svg)](https://codecov.io/gh/marklogic-community/corb2/branch/development)
[![Snyk Known Vulnerabilities Badge](https://snyk.io/test/github/marklogic-community/corb2/badge.svg)](https://snyk.io/test/github/marklogic-community/corb2)

### [What is CoRB?](https://github.com/marklogic-community/corb2/wiki#what-is-corb)
CoRB is a Java tool designed for bulk content-reprocessing of documents stored in [MarkLogic](http://www.marklogic.com/). 
CoRB stands for **Co**ntent **R**eprocessing in **B**ulk and is a multi-threaded workhorse tool at your disposal.
In a nutshell, CoRB works off a list of documents in a database and performs operations against those documents. 
CoRB operations can include generating a report across all documents, manipulating the individual documents, or a combination thereof. 

### User Guide
This document and the [wiki](https://github.com/marklogic-community/corb2/wiki) provide a comprehensive overview of CoRB and the options available to customize the execution of a CoRB job, as well as the [ModuleExecutor Tool](#moduleexecutor-tool), which can be used to execute a single (XQuery or JavaScript) module in MarkLogic.

For additional information, refer to the [CoRB Wiki](https://github.com/marklogic-community/corb2/wiki).

### Downloads
Download the latest release directly from https://github.com/marklogic-community/corb2/releases or resolve dependencies through [Maven Central](http://mvnrepository.com/artifact/com.marklogic/marklogic-corb).

### Compatability 
-  [CoRB v2.4.0](https://github.com/marklogic-community/corb2/releases/tag/2.4.0) (or later) requires Java 8 (or later) to run.
-  [CoRB v2.3.2](https://github.com/marklogic-community/corb2/releases/tag/2.3.2) is the last release compatable with Java 7 and 6.
-  [CoRB v2.2.0](https://github.com/marklogic-community/corb2/releases/tag/2.2.0) (or later) requires [marklogic-xcc 8.0.* (or later)](https://developer.marklogic.com/products/xcc) to run.
  > Note: marklogic-xcc 8 is backwards compatible to MarkLogic 5 and runs on Java 1.6 or later.

### Getting Help
To get help with CoRB
- [Post a question to Stack Overflow](http://stackoverflow.com/questions/ask?tags=marklogic+corb) with the [<code>markogic</code>](https://stackoverflow.com/questions/tagged/marklogic) and [<code>corb</code>](https://stackoverflow.com/questions/tagged/corb) tags.  
- Submit issues or feature requests at https://github.com/marklogic-community/corb2/issues

### [Running CoRB](https://github.com/marklogic-community/corb2/wiki/Running-CoRB)
The entry point is the main method in the `com.marklogic.developer.corb.Manager` class. CoRB requires the MarkLogic XCC JAR in the classpath,
preferably the version that corresponds to the MarkLogic server version, which can be downloaded from https://developer.marklogic.com/products/xcc.
 Use Java 1.8 or later.

CoRB needs options specified through one or more of the following mechanisms:

1. command-line parameters
2. Java system properties ex: `-DXCC-CONNECTION-URI=xcc://user:password@localhost:8202`
3. As properties file in the class path specified using `-DOPTIONS-FILE=myjob.properties`. Relative and full file system paths are also supported.

If specified in more than one place, a command line parameter takes precedence over a Java system property, which take precedence over a property from the **OPTIONS-FILE** properties file.

> Note: Any or all of the properties can be specified as Java system properties or key value pairs in properties file.

> Note: CoRB exit codes `0` - successful, `0` - nothing to process (ref: EXIT-CODE-NO-URIS), `1` - initialization or connection error and `2` - execution error

> Note: CoRB now supports [Logging Job Metrics](METRICS.md) back to the MarkLogic database log and/or as document in the database.

### Options
Option | Description
---|---
**<a name="INIT-MODULE"></a>INIT-MODULE** | An XQuery or JavaScript module which, if specified, will be invoked prior to **URIS-MODULE**. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively.
**<a name="INIT-TASK"></a>INIT-TASK** | Java Task which, if specified, will be called prior to **URIS-MODULE**. This can be used addition to **INIT-MODULE** for custom implementations.
**<a name="OPTIONS-FILE"></a>OPTIONS-FILE** | A properties file containing any of the CoRB options. Relative and full file system paths are supported.
**<a name="PROCESS-MODULE"></a>PROCESS-MODULE** | XQuery or JavaScript to be executed in a batch for each URI from the **URIS-MODULE** or **URIS-FILE**. Module is expected to have at least one external or global variable with name URI. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively. If returning multiple values from a JavaScript module, values must be returned as Sequence.
**<a name="PROCESS-TASK"></a>PROCESS-TASK** | <div>Java Class that implements `com.marklogic.developer.corb.Task` or extends `com.marklogic.developer.corb.AbstractTask`. Typically, it can talk to **PROCESS-MODULE** and the do additional processing locally such save a returned value.  <ul><li> `com.marklogic.developer.corb.ExportBatchToFileTask` Generates _**a single file**_, typically used for reports. Writes the data returned by the **PROCESS-MODULE** to a single file specified by **EXPORT-FILE-NAME**. All returned values from entire CoRB will be streamed into the single file. If **EXPORT-FILE-NAME** is not specified, CoRB uses **URIS\_BATCH\_REF** returned by **URIS-MODULE** as the file name.  <li> `com.marklogic.developer.corb.ExportToFileTask` Generates _**multiple files**_. Saves the documents returned by each invocation of **PROCESS-MODULE** to a separate local file within **EXPORT-FILE-DIR** where the file name for each document will be the based on the URI.</ul>
**<a name="PRE-BATCH-MODULE"></a>PRE-BATCH-MODULE** | An XQuery or JavaScript module which, if specified, will be run before batch processing starts. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively.
**<a name="PRE-BATCH-TASK"></a>PRE-BATCH-TASK** | Java Class that implements `com.marklogic.developer.corb.Task` or extends `com.marklogic.developer.corb.AbstractTask`. If **PRE-BATCH-MODULE** is also specified, the implementation is expected to invoke the XQuery and process the result if any. It can also be specified without **PRE-BATCH-MODULE** and an example of this is to add a static header to a report. <ul><li> `com.marklogic.developer.corb.PreBatchUpdateFileTask` included - Writes the data returned by the **PRE-BATCH-MODULE** to **EXPORT-FILE-NAME**, which can particularly be used to to write dynamic headers for CSV output. Also, if **EXPORT-FILE-TOP-CONTENT** is specified, this task will write this value to the **EXPORT-FILE-NAME** - this option is especially useful for writing fixed headers to reports. If **EXPORT-FILE-NAME** is not specified, CoRB uses **URIS\_BATCH\_REF** returned by **URIS-MODULE** as the file name.</li><ul>
**<a name="POST-BATCH-MODULE"></a>POST-BATCH-MODULE** | An XQuery or JavaScript module which, if specified, will be run after batch processing is completed. XQuery and JavaScript modules need to have `.xqy` and `.sjs` extensions respectively.
**<a name="POST-BATCH-TASK"></a>POST-BATCH-TASK** | Java Class that implements `com.marklogic.developer.corb.Task` or extends `com.marklogic.developer.corb.AbstractTask`. If **POST-BATCH-MODULE** is also specified, the implementation is expected to invoke the XQuery and process the result if any. It can also be specified without **POST-BATCH-MODULE** and an example of this is to add static content to the bottom of the report. <ul><li> `com.marklogic.developer.corb.PostBatchUpdateFileTask` included - Writes the data returned by the **POST-BATCH-MODULE** to **EXPORT-FILE-NAME**. Also, if **EXPORT-FILE-BOTTOM-CONTENT** is specified, this task will write this value to the **EXPORT-FILE-NAME**. If **EXPORT-FILE-NAME** is not specified, CoRB uses **URIS\_BATCH\_REF** returned by **URIS-MODULE** as the file name.</li></ul>
**<a name="THREAD-COUNT"></a>THREAD-COUNT** | The number of worker threads. Default is `1`.
**<a name="URIS-MODULE"></a>URIS-MODULE** | URI selector module written in XQuery or JavaScript. Expected to return a sequence containing the uris count, followed by all the uris. Optionally, it can also return an arbitrary string as a first item in this sequence - refer to **URIS\_BATCH\_REF** section below. XQuery and JavaScript modules need to have .xqy and .sjs extensions respectively. JavaScript modules must return a [Sequence](https://docs.marklogic.com/js/Sequence).
**<a name="URIS-FILE"></a>URIS-FILE** | If defined instead of **URIS-MODULE**, URIs will be loaded from the file located on the client. There should only be one URI per line. This path may be relative or absolute. For example, a file containing a list of document identifiers can be used as a **URIS-FILE** and the **PROCESS-MODULE** can query for the document based on this document identifier.
**<a name="XCC-CONNECTION-URI"></a>XCC-CONNECTION-URI** | Connection string to MarkLogic XDBC Server. Multiple connection strings can be specified with comma as a separator. 

### Additional options
Option | Description
---|---
**<a name="BATCH-SIZE"></a>BATCH-SIZE** | The number of URIs to be executed in single transform. Default is `1`. If more than 1, **PROCESS-MODULE** will receive a delimited string as the `$URI` variable, which needs to be tokenized to get individual URIs. The default delimiter is `;`, which can be overridden with the option **BATCH-URI-DELIM** described below. <br/>**Sample code for transform:**<br/>`declare variable URI as xs:string external;`<br/>`let $all-uris := fn:tokenize($URI,";")`  
**<a name="BATCH-URI-DELIM"></a>BATCH-URI-DELIM** | Use if the default delimiter `';'` cannot be used to join multiple URIS when **BATCH-SIZE** is greater than 1. Default is `;`.
**<a name="DECRYPTER"></a>DECRYPTER** | The class name of the options value dycrypter, which must implement `com.marklogic.developer.corb.Decrypter`. Encryptable options include **XCC-CONNECTION-URI**, **XCC-USERNAME**, **XCC-PASSWORD**, **XCC-HOSTNAME**, **XCC-PORT**, and **XCC-DBNAME**.
**<a name="COLLECTION-NAME"></a>COLLECTION-NAME** | Value of this parameter will be passed into the URIS-MODULE via external or global variable with the name URIS.
**<a name="COMMAND"></a>COMMAND** | Pause, resume, and stop the execution of CoRB. Possible commands include: PAUSE, RESUME, and STOP. If the **COMMAND-FILE** is modified and either there is no **COMMAND** or an invalid value is specified, then execution will RESUME.
**<a name="COMMAND-FILE"></a>COMMAND-FILE** | A properties file used to configure **COMMAND** and **THREAD-COUNT** while CoRB is running. For instance, to temporarily pause execution, or to lower the number of threads in order to throttle execution.
**<a name="COMMAND-FILE-POLL-INTERVAL"></a>COMMAND-FILE-POLL-INTERVAL** | The regular interval (seconds) in which the existence of the **COMMAND-FILE** is tested can be controlled by using this property. Default is `1`.
**<a name="CONNECTION-POLICY"></a>CONNECTION-POLICY** | Algorithm for balancing load across multiple hosts used by `com.marklogic.developer.corb.DefaultContentSourcePool`. Options include **ROUND-ROBIN**, **RANDOM** and **LOAD**. Default option is **ROUND-ROBIN**. **LOAD** option returns the ContentSource or Connection with least number of active sessions.  
**<a name="CONTENT-SOURCE-POOL"></a>CONTENT-SOURCE-POOL** | Class that implements `com.marklogic.developer.corb.ContentSourcePool` and used to manage ContentSource instances or connections. Default is `com.marklogic.developer.corb.DefaultContentSourcePool`.
**<a name="CONTENT-SOURCE-RENEW"></a>CONTENT-SOURCE-RENEW** | Boolean value indicating whether to periodically check to see if a ContentSource resolves to a different IP address and create a new ContentSource to add to the resource pool. This can help transparently deal with proxies that have dynamic pools of IP addresses. Default is `true`
**<a name="CONTENT-SOURCE-RENEW-INTERVAL"></a>CONTENT-SOURCE-RENEW-INTERVAL** | The regular interval (seconds) in which to resolve ContentSource IP address and add to the pool. This can help when a DNS entry may return multiple IP addresses and help spread traffic among multiple endpoints. Default is `60`
**<a name="DISK-QUEUE"></a>DISK-QUEUE** | Boolean value indicating whether the CoRB job should spill to disk when a maximum number of URIs have been loaded in memory, in order to control memory consumption and avoid Out of Memory exceptions for extremely large sets of URIs.
**<a name="DISK-QUEUE-MAX-IN-MEMORY-SIZE"></a>DISK-QUEUE-MAX-IN-MEMORY-SIZE** | The maximum number of URIs to hold in memory before spilling over to disk. Default is `1000`.
**<a name="DISK-QUEUE-TEMP-DIR"></a>DISK-QUEUE-TEMP-DIR** | The directory where the URIs queue can write to disk when the maximum in-memory items has been exceeded. If not specified then **TEMP-DIR** value will be used. If neither are specified, then the default behavior is to use java.io.tmpdir.
**<a name="ERROR-FILE-NAME"></a>ERROR-FILE-NAME** | Used when FAIL-ON-ERROR is false. If specified true, removes duplicates from the errored URIs along with error messages will be written to this file. Uses BATCH-URI-DELIM or default `';'` to separate URI and error message.
**<a name="EXIT-CODE-IGNORED-ERRORS"></a>EXIT-CODE-IGNORED-ERRORS** | Returns this exit code when there were errors and **FAIL-ON-ERROR**=`false`. Default is `0`.
**<a name="EXIT-CODE-NO-URIS"></a>EXIT-CODE-NO-URIS** | Returns this exit code when there is nothing to process. Default is `0`. 
**<a name="XPORT_FILE_AS_ZIP"></a>EXPORT_FILE_AS_ZIP** | If true, PostBatchUpdateFileTask compresses the output file as a zip file.
**<a name="EXPORT-FILE-BOTTOM-CONTENT"></a>EXPORT-FILE-BOTTOM-CONTENT** | Used by `com.marklogic.developer.corb.PostBatchUpdateFileTask` to append content to **EXPORT-FILE-NAME** after batch process is complete.
**<a name="EXPORT-FILE-DIR"></a>EXPORT-FILE-DIR** | Export directory parameter is used by `com.marklogic.developer.corb.ExportBatchToFileTask` or similar custom task implementations. <br/>Optional: Alternatively, **EXPORT-FILE-NAME** can be specified with a full path.
**<a name="EXPORT-FILE-NAME"></a>EXPORT-FILE-NAME** | Shared file to write output of `com.marklogic.developer.corb.ExportBatchToFileTask` - should be a file name with our without full path. <ul><li>**EXPORT-FILE-DIR** Is not required if a full path is used.</li><li>If **EXPORT-FILE-NAME** is not specified, CoRB attempts to use **URIS\_BATCH\_REF** as the file name and this is especially useful in case of automated jobs where file name can only be determined by the **URIS-MODULE** - refer to **URIS\_BATCH\_REF** section below.</li></ul>
**<a name="EXPORT-FILE-PART-EXT"></a>EXPORT-FILE-PART-EXT** | The file extension for export files being processed. ex: .tmp - if specified, `com.marklogic.developer.corb.PreBatchUpdateFileTask` adds this temporary extension to the export file name to indicate **EXPORT-FILE-NAME** is being actively modified. To remove this temporary extension after **EXPORT-FILE-NAME** is complete, `com.marklogic.developer.corb.PostBatchUpdateFileTask` must be specified as **POST-BATCH-TASK**.
**<a name="EXPORT-FILE-REQUIRE-PROCESS-MODULE"></a>EXPORT-FILE-REQUIRE-PROCESS-MODULE** | Boolean value indicating whether or not to require a **PROCESS-MODULE** when an Export*ToFile PROCESS-TASK is specified. This can help avoid confusion when the **PROCESS-MODULE** was accidentally not configured and no files are generated. Default is `true`
**<a name="EXPORT-FILE-SORT"></a>EXPORT-FILE-SORT** | If `ascending` or `descending`, lines will be sorted. If <code>&#124;distinct</code> is specified after the sort direction, duplicate lines from **EXPORT-FILE-NAME** will be removed. i.e. <code>ascending&#124;distinct</code> or <code>descending&#124;distinct</code>
**<a name="EXPORT-FILE-SORT-COMPARATOR"></a>EXPORT-FILE-SORT-COMPARATOR** | A java class that must implement `java.util.Comparator`. If specified, CoRB will use this class for sorting in place of ascending or descending string comparator even if a value was specified for **EXPORT-FILE-SORT**.
**<a name="EXPORT-FILE-TOP-CONTENT"></a>EXPORT-FILE-TOP-CONTENT** | Used by `com.marklogic.developer.corb.PreBatchUpdateFileTask` to insert content at the top of **EXPORT-FILE-NAME** before batch process starts. If it includes the string `@URIS_BATCH_REF`, it is replaced by the batch reference returned by **URIS-MODULE**.
**<a name="EXPORT-FILE-URI-TO-PATH"></a>EXPORT-FILE-URI-TO-PATH** | Boolean value indicating whether to convert doc URI to a filepath. Default is `true`
**<a name="FAIL-ON-ERROR"></a>FAIL-ON-ERROR** | Boolean value indicating whether the CoRB job should fail and exit if a process module throws an error. Default is `true`. This option will not handle repeated connection failures.
**<a name="INSTALL"></a>INSTALL** | Whether to install the Modules in the Modules database. Specify `true` or `1` for installation. Default is `false`.
**<a name="LOADER-BASE64-ENCODE"></a>LOADER-BASE64-ENCODE** | Boolean option specifying whether the content loaded by FileUrisStreamingXMLLoader or FileUrisXMLLoader (with the option `LOADER-USE-ENVELOPE=true`) should be base64 encoded, or appended as the child of the `/corb-loader/content` element. Default is `false`.
**<a name="LOADER-PATH"></a>LOADER-PATH** | The path to the resource (file or folder) that will be the input source for a loader class that extends AbstractFileUrisLoader, such as FileUrisDirectoryLoader, FileUrisLoader, FileUrisStreamingXmlLoader, FileUrisXmlLoader, and FileUrisZipLoader
**<a name="LOADER-SET-URIS-BATCH-REF"></a>LOADER-SET-URIS-BATCH-REF** | Boolean option indicating whether a file loader should set the [URIS_BATCH_REF](https://github.com/marklogic-community/corb2#uris_batch_ref). Default is `false`.
**<a name="LOADER-USE-ENVELOPE"></a>LOADER-USE-ENVELOPE** | Boolean value indicating whether FileUris loaders should use an XML envelope, in order to send file metadata in addition to the file content.
**<a name="JOB-NAME"></a>JOB-NAME** | Name of the current Job.
**<a name="JOB-SERVER-PORT"></a>JOB-SERVER-PORT** | Optional port number to start a lightweight HTTP server which can be used to monitor, change the number of threads, and pause/resume the CoRB job. Port number must be a valid port(s) or a valid range of ports.  <ul><li>Ex: 9080</li><li> Ex: 9080,9083,9087</li><li> Ex: 9080-9090</li><li> Ex: 9080-9083,9085-9090</li></ul>  The job server will bind to a port from the configured port number(s). By default, if the **JOB-SERVER-PORT** option is not specified, a job server is not started. <p> When a port is specified and available, the job server URL will be logged to the console with both the UI `http://<host>:<port>` and metrics URL `http://<host>:<port>/metrics`. (grep for string _com.marklogic.developer.corb.JobServer logUsage_)  <p>The metrics URL supports the following parameters:<ul><li>**COMMAND**=pause (or resume). </li><li>**CONCISE**=true limits the amound of data returned</li><li>**FORMAT**=json (or xml) returns job stats in the requested format</li><li>**THREAD-COUNT**=<#> will adjust the number of threads for the executing job</li></ul>  
**<a name="MAX-OPTS-FROM-MODULE"></a>MAX-OPTS-FROM-MODULE** | Maximum number of custom inputs from the **URIS-MODULE** to other modules. Default is `10`.
**<a name="METADATA"></a>METADATA** | The variable name that needs to be defined in the server side query to use the metadata set by the **URIS-LOADER**.
**<a name="METADATA-TO-PROCESS-MODULE"></a>METADATA-TO-PROCESS-MODULE** | If this option is set to true, **XML-METADATA** is set as an external variable with name **METADATA** to **PROCESS-MODULE** as well. Default is `false`.
**<a name="METRICS-COLLECTIONS"></a>METRICS-COLLECTIONS** | Adds the metrics document to the specified collection.|
**<a name="METRICS-DATABASE"></a>METRICS-DATABASE** | Uses the value provided to save the metrics document to the specified Database. The XCC connection specified should have the following privilege `http://marklogic.com/xdmp/privileges/xdmp-invoke`|
**<a name="METRICS-LOG-LEVEL"></a>METRICS-LOG-LEVEL**|String value indicating the log level that the CoRB job should use to log metrics to ML Server Error log. Possible values are _none, emergency, alert, critical, error, warning, notice, info, config, debug, fine, finer, finest_. Default is `none`, which means metrics are not logged.|
**<a name="METRICS-MODULE"></a>METRICS-MODULE** | XQuery or JavaScript to be executed at the end of the CoRB Job to save the metrics document to the database. There is an XQuery module ([save-metrics.xqy](src/main/resources/save-metrics.xqy)) and a JavaScript module ([saveMetrics.sjs](src/main/resources/saveMetrics.sjs)) provided. You can use these modules as a template to customize the the metrics document saved to the database. XQuery and JavaScript modules need to have '{@code .xqy}' and{@code .sjs} extensions respectively.|
**<a name="METRICS-NUM-FAILED-TRANSACTIONS"></a>METRICS-NUM-FAILED-TRANSACTIONS** | Maximum number of failed transaction to be logged in the metrics. Default is `0`.
**<a name="METRICS-NUM-SLOW-TRANSACTIONS"></a>METRICS-NUM-SLOW-TRANSACTIONS** | Maximum number of slow transaction to be logged in the metrics. Default is `0`.
**<a name="METRICS-ROOT"></a>METRICS-ROOT** | Uses the value provided to as the URI Root for saving the metrics document.|
**<a name="METRICS-SYC-FREQUENCY"></a>METRICS-SYNC-FREQUENCY** | Frequency (in seconds) at which the metrics document needs to be updated in the database. By default the metrics document is not periodically updated and is only written once at the end of the job. |
**<a name="MODULE-ROOT"></a>MODULE-ROOT** | Default is `/`.
**<a name="MODULES-DATABASE"></a>MODULES-DATABASE** | Uses the **XCC-CONNECTION-URI** if not provided; use 0 for file system.
**<a name="NUM-TPS-FOR-ETC"></a>NUM-TPS-FOR-ETC** | Number of recent transactions per second (tps) values used to calculate estimated completion time (ETC). Default is `10`.
**<a name="POST-BATCH-MINIMUM-COUNT"></a>POST-BATCH-MINIMUM-COUNT** | The minimum number of results that must be returned for the **POST-BATCH-MODULE** or **POST-BATCH-TASK** to be executed. Default is `1`.
**<a name="PRE-POST-BATCH-ALWAYS-EXECUTE"></a>PRE-POST-BATCH-ALWAYS-EXECUTE** | Boolean value indicating whether the PRE_BATCH and POST_BATCH module or task should be executed without evaluating how many URIs were returned by the URI selector.
**<a name="PRE-BATCH-MINIMUM-COUNT"></a>PRE-BATCH-MINIMUM-COUNT** | The minimum number of results that must be returned for the **PRE-BATCH-MODULE** or **PRE-BATCH-TASK** to be executed. Default is `1`.
**<a name="QUERY-RETRY-LIMIT"></a>QUERY-RETRY-LIMIT** | Number of re-query attempts before giving up. Default is `2`.
**<a name="QUERY-RETRY-INTERVAL"></a>QUERY-RETRY-INTERVAL** | Time interval, in seconds, between re-query attempts. Default is `20`.
**<a name="QUERY-RETRY-ERROR-CODES"></a>QUERY-RETRY-ERROR-CODES** | A comma separated list of MarkLogic error codes for which a QueryException should be retried.
**<a name="QUERY-RETRY-ERROR-MESSAGE"></a>QUERY-RETRY-ERROR-MESSAGE** | A comma separated list of values that if contained in an exception message a QueryException should be retried.
**<a name="SSL-CONFIG-CLASS"></a>SSL-CONFIG-CLASS** | A java class that must implement `com.marklogic.developer.corb.SSLConfig`. If not specified, CoRB defaults to `com.marklogic.developer.corb.TrustAnyoneSSLConfig` for `xccs` connections.
**<a name="URIS-LOADER"></a>URIS-LOADER** | Java class that implements `com.marklogic.developer.corb.UrisLoader`. A custom class to load URIs instead of built-in loaders for **URIS-MODULE** or **URIS-FILE** options. Example: com.marklogic.developer.corb.FileUrisXMLLoader
**<a name="URIS-REDACTED"></a>URIS-REDACTED** | Optional boolean flag indicating whether URIs should be excluded from logging, console, and JobStats metrics. Default is `false`.
**<a name="URIS-REPLACE-PATTERN"></a>URIS-REPLACE-PATTERN** | One or more replace patterns for URIs - Used by java to truncate the length of URIs on the client side, typically to reduce java heap size in very large batch jobs, as the CoRB java client holds all the URIS in memory while processing is in progress. If truncated, PROCESS-MODULE needs to reconstruct the URI before trying to do `fn:doc()` to fetch the document. <br/>Usage: `URIS-REPLACE-PATTERN=pattern1,replace1,pattern2,replace2,...)`<br/>**Example:**<br/>`URIS-REPLACE-PATTERN=/com/marklogic/sample/,,.xml,` - Replace /com/marklogic/sample/ and .xml with empty strings. So, CoRB client only needs to cache the id '1234' instead of the entire URI /com/marklogic/sample/1234.xml. In the transform **PROCESS-MODULE**, we need to do `let $URI := fn:concat("/com/marklogic/sample/",$URI,".xml")`
**<a name="XCC-CONNECTION-RETRY-LIMIT"></a>XCC-CONNECTION-RETRY-LIMIT** | Number attempts to connect to ML before giving up. Default is `3`.
**<a name="XCC-CONNECTION-RETRY-INTERVAL"></a>XCC-CONNECTION-RETRY-INTERVAL** | Time interval, in seconds, between retry attempts. Default is `60`.
**<a name="XCC-CONNECTION-HOST-RETRY-LIMIT"></a>XCC-CONNECTION-HOST-RETRY-LIMIT** | Number attempts to connect to ML before giving up on a host. If not specified, it defaults to **XCC-CONNECTION-RETRY-LIMIT**
**<a name="XCC-DBNAME"></a>XCC-DBNAME** | (Optional) Name of the content database to execute against
**<a name="XCC-HOSTNAME"></a>XCC-HOSTNAME** | Required if **XCC-CONNECTION-URI** is not specified. Multiple host can be specified with comma as a separator. 
**<a name="XCC-HTTPCOMPLIANT"></a>XCC-HTTPCOMPLIANT** | Optional boolean flag to indicate whether to enable HTTP 1.1 compliance in XCC. If this option is set, the [`xcc.httpcompliant`](https://docs.marklogic.com/guide/xcc/concepts#id_28335) System property will be set. Default is `true`.
**<a name="XCC-PASSWORD"></a>XCC-PASSWORD** | Required if **XCC-CONNECTION-URI** is not specified.
**<a name="XCC-PORT"></a>XCC-PORT** | Required if **XCC-CONNECTION-URI** is not specified.
**<a name="XCC-PROTOCOL"></a>XCC-PROTOCOL** | (Optional) Used if XCC-CONNECTION-URI is not specified. The XCC scheme to use; either `xcc` or `xccs`. Default is `xcc`.
**<a name="XCC-TIME-ZONE"></a>XCC-TIME-ZONE** | The ID for the TimeZone that should be set on XCC RequestOption. When a value is specified, it is parsed using [`TimeZone.getTimeZone()`](https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html#getTimeZone-java.lang.String-) and set on XCC RequestOption for each Task. Invalid ID values will produce the GMT TimeZone. If not specified, XCC uses the JVM default TimeZone.
**<a name="XCC-URL-ENCODE-COMPONENTS"></a>XCC-URL-ENCODE-COMPONENTS** | Indicate whether or not the XCC connection string components should be URL encoded. Possible values are `always`, `never`, and `auto`. Default is `auto`.
**<a name="XCC-USERNAME"></a>XCC-USERNAME** | Required if **XCC-CONNECTION-URI** is not specified.
**<a name="XML-FILE"></a>XML-FILE** | In order to use this option a class `com.marklogic.developer.corb.FileUrisXMLLoader` has to be specified in the **URIS-LOADER** option. If defined instead of **URIS-MODULE**, XML nodes will be used as URIs from the file located on the client. The file path may be relative or absolute. Default processing will select all of the child elements of the document element (i.e. `/*/*`). The **XML-NODE** option can be specified with an XPath to address a different set of nodes.
**<a name="XML-METADATA"></a>XML-METADATA** | An XPath to address the node that contains metadata portion of the XML. This must be different from the **XML-NODE**. The metadata is set as an external variable with name **METADATA** to **PRE-BATCH-MODULE** and **POST-BATCH-MODULE** and also **PROCESS-MODULE** if enabled by **METADATA-TO-PROCESS-MODULE**.
**<a name="XML-NODE"></a>XML-NODE** | An XPath to address the nodes to be returned in an **XML-FILE** by the `com.marklogic.developer.corb.FileUrisXMLLoader`. For example, a file containing a list of nodes wrapped by a parent element can be used as a **XML-FILE** and the **PROCESS-MODULE** can unquote the URI string as node to do further processing with the node. If not specified, the default behavior is to select the child elements of the document element (i.e. `/*/*`)
**<a name="XML-SCHEMA"></a>XML-SCHEMA** | Path to a W3C XML Schema to be used by `com.marklogic.developer.corb.FileUrisStreamingXMLLoader` or `com.marklogic.developer.corb.FileUrisXMLLoader` to validate an **XML-FILE**, and used by `com.marklogic.developer.corb.SchemaValidateBatchToFileTask` and `com.marklogic.corb.SchemaValidateToFileTask` post-process tasks to validate documents returned from a process module.
**<a name="XML-SCHEMA-HONOUR-ALL-SCHEMALOCATIONS"></a>XML-SCHEMA-HONOUR-ALL-SCHEMALOCATIONS** | Boolean value indicating whether to set the feature [`http://apache.org/xml/features/honour-all-schemaLocations`](https://xerces.apache.org/xerces2-j/features.html#honour-all-schemaLocations). Default is `true`
**<a name="XML-TEMP-DIR"></a>XML-TEMP-DIR** | Temporary directory used by `com.marklogic.developer.corb.FileUrisStreamingXMLLoader` to store files extracted from the **XML-FILE**. If not specified, **TEMP-DIR** value will be used. If neither are specified, then the default Java temp directory will be used.

#### [URIS\_BATCH\_REF](https://github.com/marklogic-community/corb2/wiki/URIS_BATCH_REF)
If a module, including those specified by **PRE-BATCH-MODULE**, **PROCESS-MODULE** or **POST-BATCH-MODULE** have an external or global variable named **URIS\_BATCH\_REF**, the variable will be set to the first **non-numeric** item in the sequence returned by **URIS-MODULE**. This means that, when used, the **URIS-MODULE** must return a sequence with the special string value first, then the URI count, then the sequence of URIs to process.  

As an example, a batch ref can be a link/id of a document that manages the status of the batch job, where pre-batch module updates the status to start and post-batch module can set it to complete. This example can be used to manage status and errors in automated batch jobs.   

ExportBatchToFileTask, PreBatchUpdateFileTask and PostBatchUpdateFileTask use **URIS\_BATCH\_REF** as the file name if **EXPORT-FILE-NAME** is not specified. This is useful for automated jobs where name of the output file name can be determined only by the **URIS-MODULE**.  

#### URIS\_TOTAL\_COUNT
Total count of uris is set as an external variable to **PRE-BATCH-MODULE** and **POST-BATCH-MODULE** (since 2.4.5)

### [Custom Inputs to XQuery or JavaScript Modules](https://github.com/marklogic-community/corb2/wiki/Custom-inputs-to-XQuery-or-JavaScript-modules)
Any property specified with prefix (with '.') **INIT-MODULE**, **URIS-MODULE**, **PRE-BATCH-MODULE**, **PROCESS-MODULE**, **POST-BATCH-MODULE** will be set as an external variable in the corresponding XQuery module (if that variable is defined as an external string variable in XQuery module). For JavaScript modules the variables need be defined as global variables.  

#### Custom Input Examples:
- `URIS-MODULE.maxLimit=1000` Expects an external string variable  _maxLimit_ in URIS-MODULE XQuery or global variable for JavaScript.  
- `PROCESS-MODULE.startDate=2015-01-01` Expects an external string variable _startDate_ in PROCESS-MODULE XQuery or global variable for JavaScript.  

Alternatively, **URIS-MODULE** can pass custom inputs to **PRE-BATCH-MODULE**, **PROCESS-MODULE**, **POST-BATCH-MODULE** by returning one or more of the property values in above format before the count the of URIs. If the **URIS-MODULE** needs **URIS\_BATCH\_REF** (above) as well, it needs to be just before the URIs count.  

#### Custom Input From URIS-MODULE Example
```xquery
let $uris := cts:uris()
return ("PROCESS-MODULE.foo=bar", "POST-BATCH-MODULE.alpha=10", fn:count($uris), $uris)
```

### [Adhoc Modules](https://github.com/marklogic-community/corb2/wiki/Adhoc-Modules)
Appending `|ADHOC` to the name or path of a XQuery module (with .xqy extension) or JavaScript (with .sjs or .js extension) module will cause the module to be read from the file system and executed in MarkLogic without being uploaded to Modules database. This simplifies running CoRB jobs by not requiring deployment of any code to MarkLogic, and makes the set of CoRB files and configuration more self contained.   

**INIT-MODULE**, **URIS-MODULE**, **PROCESS-MODULE**, **PRE-BATCH-MODULE** and **POST-BATCH-MODULE** can be specified adhoc by adding the suffix `|ADHOC` for XQuery or JavaScript (with .sjs or .js extension) at the end. Adhoc XQuery or JavaScript remains local to the CoRB and is not deployed to MarkLogic. The XQuery or JavaScript module should be in its named file and that file should be available on the file system, including being on the java classpath for CoRB.

##### Adhoc Examples:
- `PRE-BATCH-MODULE=adhoc-pre-batch.xqy|ADHOC` adhoc-pre-batch.xqy must be on the classpath or in the current directory.
- `PROCESS-MODULE=/path/to/file/adhoc-transform-module.xqy|ADHOC` XQuery module file with full path in the file system.  
- `URIS-MODULE=adhoc-uris.sjs|ADHOC` Adhoc JavaScript module in the classpath or current directory.

#### Inline Adhoc Modules
It is also possible to set a module option with inline code blocks, rather than a file path. This can be done by prepending either `INLINE-XQUERY|` or `INLINE-JAVASCRIPT|` to the option value, followed by the XQuery or JavaScript code to execute. Inline code blocks are executed as "adhoc" modules and are not uploaded to the Modules database. The `|ADHOC` suffix is optional for inline code blocks.

##### Inline Adhoc Example
```xquery
URIS-MODULE=INLINE-XQUERY|xquery version '1.0-ml'; let $uris := cts:uris('', 'document', cts:collection-query('foo')) return (count($uris), $uris)
```
### JavaScript Modules
JavaScript modules are supported and can be used in place of an XQuery module. However, if returning multiple values (ex: URIS-MODULE), values must be returned as a [Sequence](https://docs.marklogic.com/js/Sequence). MarkLogic JavaScript API has helper functions to convert Arrays into Sequence ([`Sequence.from()`](https://docs.marklogic.com/Sequence.from)) and inserting values into another Sequence ([`fn.insertBefore()`](https://docs.marklogic.com/fn.insertBefore)).

JavaScript module must have an `.sjs` file extension when deployed to Modules database. However, adhoc JavaScript modules support both `.sjs` and `.js` file extensions.

For example, a simple URIS-MODULE may look like this:
```javascript
let uris = cts.uris();
fn.insertBefore(uris, 0, fn.count(uris));
```

To return URIS\_BATCH\_REF, we can do the following:
```javascript
fn.insertBefore(fn.insertBefore(uris, 0, fn.count(uris)), 0, "batch-ref")
```

> Note: Do not use single quotes within (adhoc) JavaScript modules. If you must use a single quote, escape it with a quote (ex: ''text'')

### [Encryption](https://github.com/marklogic-community/corb2/wiki/Encryption)
It is often required to protect the database connection string or password from unauthorized access. 
So, CoRB optionally supports encryption of the entire XCC URL or any parts of the XCC URL (if individually specified), such as **XCC-PASSWORD**.

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
- Use the PrivateKeyDecrypter class inside the CoRB JAR with the gen-keys option to generate a key.  
  `java -cp /path/to/lib/* com.marklogic.developer.corb.PrivateKeyDecrypter gen-keys /path/to/private.key /path/to/public.key RSA 1024`  
> Note: if not specified, default algorithm: RSA, default key-length: 1024
- Use the PrivateKeyDecrypter class inside the CoRB JAR with the encrypt option to encrypt the clear text such as an xcc URL or password.  
  `java -cp /path/to/lib/* com.marklogic.developer.corb.PrivateKeyDecrypter encrypt /path/to/public.key clearText RSA`  
> Note: if not specified, default algorithm: RSA

#### RSA keys
- `openssl genrsa -out private.pem 1024` Generate a private key in PEM format
- `openssl pkcs8 -topk8 -nocrypt -in private.pem -out private.pkcs8.key` Create a PRIVATE-KEY-FILE in PKCS8 standard for java
- `openssl rsa -in private.pem -pubout > public.key`  Extract public key
- `echo "uri or password" | openssl rsautl -encrypt -pubin -inkey public.key | base64` Encrypt URI or password. Optionally, the encrypted text can be enclosed with "ENC" ex: ENC(xxxxxx)

#### ssh-keygen  
- `ssh-keygen` ex:key as id_rsa after selecting a passphrase
- `openssl pkcs8 -topk8 -nocrypt -in id_rsa -out id_rsa.pkcs8.key` (asks for passphrase)
- `openssl rsa -in id_rsa -pubout > public.key` (asks for passphrase)
- `echo "password or uri" | openssl rsautl -encrypt -pubin -inkey public.key | base64`

#### com.marklogic.developer.corb.JasyptDecrypter
JasyptDecrypter automatically detects if the text is encrypted. Unencrypted text or clear text is returned as-is. Though, not required, encrypted text can be optionally enclosed with "ENC" ex: ENC(xxxxxx) to clearly indicate that it is encrypted.    

Encrypt the URI or password as below. It is assumed that the [jasypt](http://www.jasypt.org/) distribution is available on your machine.   

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
CoRB provides support for SSL over XCC. As a prerequisite to enabling CoRB SSL support, the XDBC server must be configured to use SSL. It is necessary to specify **XCC-CONNECTION-URI** property with a protocol of 'xccs'. To configure a particular type of SSL configuration use the following property:

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
**<a name="SSL-ENABLED-PROTOCOLS"></a>SSL-ENABLED-PROTOCOLS** | (Optional) A comma or colon separated list of acceptable SSL protocols, in priority order. Default is `TLSv1.2`.
**<a name="SSL-CIPHER-SUITES"></a>SSL-CIPHER-SUITES** | A comma or colon separated list of acceptable cipher suites used.

### Load Balancing and Failover with Multiple Hosts
CoRB 2.4+ supports load balancing and failover using `com.marklogic.developer.corb.ContentSourcePool`. This is automatically enabled when multiple comma separated values (supports encryption) are specified for for **XCC-CONNECTION-URI** or **XCC-HOSTNAME**.

```properties
XCC-CONNECTION-URI=xcc://hostname1:8000/dbname,xcc://hostname2:8000/dbname,..
```
OR
```properties
XCC-HOST-NAME=hostname1,hostname2,..
```

The default implementation for `com.marklogic.developer.corb.ContentSourcePool` is `com.marklogic.developer.corb.DefaultContentSourcePool`. It uses below options for **CONNECTION-POLICY** for allocating connections to callers. 
-  **ROUND-ROBIN** - (Default) Connections are allocated using round-robin algorithm. 
-  **RANDOM** - Connections are randomly allocated.
-  **LOAD** - Host with least number of active connections is allocated to caller.    

### Query and Connection Retries
CoRB automatically retries the requests a given URI when it encounters `com.marklogic.xcc.exceptions.ServerConnectionException` from MarkLogic. If necessary, the number of retry attempts can be configured using **XCC-CONNECTION-RETRY-LIMIT**. If multiple hosts are specified, we can optionally configure retries per each host using **XCC-CONNECTION-HOST-RETRY-LIMIT**. CoRB waits at least **XCC-CONNECTION-RETRY-INTERVAL** seconds before a connection is retried on a failed host. 

CoRB also supports retries of requests failed due to query errors. This feature is only intended for sporadic query errors which are not specific to a particular URI. A good example may include occasional time out exceptions from MarkLogic when the ML is too busy and request time limit is low. We can configure which queries can be retried using **QUERY-RETRY-ERROR-CODES** or **QUERY-RETRY-ERROR-MESSAGE** (when error codes are not available). If necessary, the number of query retry attempts can be configured using **QUERY-RETRY-LIMIT**. CoRB waits at least **QUERY-RETRY-INTERVAL** seconds before retrying a query.

```properties
QUERY-RETRY-ERROR-CODES=XDMP-EXTIME,SVC-EXTIME
QUERY-RETRY-ERROR-MESSAGE=ErrorMsg1,ErrorMsg2
```

### [Usage Examples](https://github.com/marklogic-community/corb2/wiki/Running-CoRB#usage-examples)
Refer to the [wiki for examples](https://github.com/marklogic-community/corb2/wiki/Running-CoRB#usage-examples) of how to execute a CoRB job and various ways of configuring the job options.

### [ModuleExecutor Tool](https://github.com/marklogic-community/corb2/wiki/ModuleExecutor-Tool)

Sometimes, a two or more staged CoRB job with both a selector and transform isn't necessary to get the job done. Sometimes, only a single query needs to be executed and the output captured to file. 
Maybe even to execute only a single query with no output captured. 
In these cases, the [ModuleExecutor Tool](https://github.com/marklogic-community/corb2/wiki/ModuleExecutor-Tool) can be used to quickly and efficiently execute your XQuery or JavaScript files.
