# CoRB Metrics FAQ

* **How can I log the job metrics to the Marklogic error log?**
    * Logging can be enabled by setting METRICS_TO_ERROR_LOG property in options.
        + Ex: **METRICS-TO-ERROR_LOG=info**
        + Startup message is logged when CoRB job startsup and detailed metrics are logged when the job finishes.
* **What kind of details are logged?**
    * User provided options ( not the default options )
    * Host Name
    * Job run location 
    * Time taken for each of the stages of th CoRB job, i.e, INIT-MODULE, URIS-MODULE, PRE-BATCH-MODULE, PROCESS-MODULE, POST-BATCH-MODULE
    * Failed transactions ( first 1000 failures)
    * Slow transactions ( top 5 up to 1000, sorted by the slowest )
    * Start and end times
    * Average transaction time
    * Total tasks 
    * Number of failed tasks
 * **Are connection strings or passwords logged to the error log?**
    * No. 
* **How can I set the log level when logging metrics to the Marklogic error log?**
    * METRICS-TO-ERROR-LOG property has the following possible values:
      +  *none,emergency,alert,critical,error,warning,notice,info,config,debug,fine,finer,finest.*
      + If none then metrics will not be logged.
* **How can I save the metrics as a Document to the database?**
    * METRICS-DB-NAME property will be used to save the mertrics document to the database.
    * Default format for metrics document is XML.
* **How can I save metrics document in JSON format?**
   * When METRICS_PROCESS_MODULE option is set to an xquery or javascript module, that module is executed after the CoRB job completes to save the metrics document.
   * CoRB2 distribution comes with two sample modules that can be found in the resources folder.
    + save-metric-to-db.xqy 
        + This is the default and saves the merics document as XML Document.
    + saveMetrics.sjs
        + This will save the merics document as a JSON Document.
        + Ex:METRICS-PROCESS-MODULE=saveMetrics.sjs|ADHOC
* **Can I add the metrics document to a Collection?**
    * The metrics document is added to a Collection with the Job Name ( or the Job run location if name is not provided )
    * METRICS-DOC-COLLECTIONS option takes a comma seperated collection names.
* **Can I change the URI to which the metrics document is saved?**
    * By default the URI format is /METRICS_DOC_BASE_DIR/CoRB2/JOB-NAME/YEAR/MONTH/DATE/HOUR/MINUTE/RANDOM-NUMBER.(json or xml)
    * METRICS_DOC_BASE_DIR has a default value of /ServiceMetrics/
    * JOB-NAME defaults to the job run location
     
* **I want to have complete control over how the metrics document is saved. Is that possible?**
    * You can use the above mentioned sample modules (*saveMetrics.sjs and save-metric-to-db.xqy*) as an example and implement your own customizations.
    + Ex:METRICS-PROCESS-MODULE=/export/home/dev/saveMetricsCustom.sjs|ADHOC


