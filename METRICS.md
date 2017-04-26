* How to log metrics to the Marklogic error log?
    * Logging can be enabled by setting METRICS_TO_ERROR_LOG property in options.
        + Ex: METRICS-TO-ERROR_LOG=info*
        + info messages are logged when CoRB job startsup and detailed metrics are logged when the job finishes.
* What are the details that are logged?
    * User provided options
    * Host Name
    * Job run location 
    * Time taken for each of the stages of th CoRB job, i.e, INIT-MODULE, URIS-MODULE, PRE-BATCH-MODULE, PROCESS-MODULE, POST-BATCH-MODULE
    * Failed transactions
    * Slow transactions

* How can I set the log level when logging metrics to the Marklogic error log?
    * METRICS-TO-ERROR-LOG property has the following possible values:
      +  *none,emergency,alert,critical,error,warning,notice,info,config,debug,fine,finer,finest.*
      + If none then metrics will not be logged.
* How can I save the metrics as a Document in the database?
    * METRICS-DB-NAME property will be used to save the mertrics document to the database.
    * Default format for metrics document is XML.
* How can I save metrics document in JSON format?
   * When METRICS_PROCESS_MODULE is set to an xquery or javascript module, that module is executed after the CoRB job completes to save the metrics document.
   * CoRB2 distribution comes with two sample modules that can be found in the resources folder.
    + save-metric-to-db.xqy 
        + This is the default and saves the merics document as XML Document.
    + saveMetrics.sjs
        + This will save the merics document as a JSON Document.
        + Ex:METRICS-PROCESS-MODULE=saveMetrics.sjs|ADHOC
* How can I customize the document that gets saved to the Database?
    * You can use the above mentioned sample modules as an example and implement your own customizations.
    + Ex:METRICS-PROCESS-MODULE=/export/home/dev/saveMetricsCustom.sjs|ADHOC


