* How to log metrics to the Marklogic error log?
>>Logging can be enabled by setting METRICS_TO_ERROR_LOG property in options.
*Ex: METRICS-TO-ERROR_LOG=info*
* How to set the log level when logging metrics to the Marklogic error log?
>>METRICS-TO-ERROR-LOG property has the following possible values:
*none,emergency,alert,critical,error,warning,notice,info,config,debug,fine,finer,finest.*
If none then metrics will not be logged.
* How to save the metrics as a Document in the database?
>>METRICS-DB-NAME property will be used to save the mertrics document to the database.
Default format for metrics document is XML.
* How can I customize the document that gets saved to the Database?
>>CoRB2 distribution has two sample modules
    * save-metric-to-db.xqy 
    >>* This is the default and saves the merics document as XML Document.
    * saveMetrics2.sjs
    >>* This will save the merics document as a JSON Document.
>> When METRICS_PROCESS_MODULE is set to an xquery or javascript module, that module is executed after the CoRB completes to save the metrics document.
>> To save the document as JSON set the property to the sample sjs file. Ex:METRICS-PROCESS-MODULE=/saveMetrics2.sjs
