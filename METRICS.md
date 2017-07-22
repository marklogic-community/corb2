# CoRB Metrics FAQ

* **How can I log the job metrics to the Marklogic error log?**
    * Logging can be enabled by setting METRICS-TO-ERROR-LOG property in options.
        + Eg: **METRICS-TO-ERROR-LOG=info**
        + Startup message is logged when CoRB job starts up and detailed metrics are logged when the job has finished.
* **How can I set the log level when logging metrics to the Marklogic error log?**
    * METRICS-TO-ERROR-LOG property has the following possible values:
      +  *none,emergency,alert,critical,error,warning,notice,info,config,debug,fine,finer,finest.*
      + Default value is none ( which means the metrics will not be logged to the error log).
* **What kind of details are logged?**
    * User provided options ( not the default options )
    * Host Name
    * Job run location 
    * Time taken for each of the stages of the CoRB job, i.e, INIT-MODULE, URIS-MODULE, PRE-BATCH-MODULE, PROCESS-MODULE, POST-BATCH-MODULE
    * Failed transactions ( first 1000 failures)
    * Slow transactions ( top 5 up to 100, sorted by the slowest )
    * Start and end times
    * Average transaction time
    * Total tasks 
    * Total Number of failed tasks
    * Current and average transactions per second
    * Estimated time of completion
 * **Are connection strings or passwords logged to the error log?**
    * No. 
* **How can I save the metrics as a Document to the database?**
    * METRICS-DB-NAME property will be used to save the metrics document to the database.
    * Default format for metrics document is XML.
    * METRICS-DB-NAME is the only required option for the document to be saved to the database. If this option is not specified the document will not be saved to the Database.
* **Setting the METRICS-DB-NAME option also log it to the server log?**
    * No. METRICS-TO-ERROR-LOG option needs to be selected for CoRB to log to server log.
* **How can I save metrics document in JSON format?**
   * METRICS-PROCESS-MODULE option can be used to save metrics document in JSON format
   * When METRICS-PROCESS-MODULE option is set to an xquery or javascript module, that module is executed after the CoRB job completes to save the metrics document.
   * CoRB2 distribution comes with two sample modules that can be found in the resources folder.
    + [save-metric-to-db.xqy](corb2/src/main/resources/save-metric-to-db.xqy)
        + This is the default and saves the metrics document as XML.
    + [saveMetrics.sjs](corb2/src/main/resources/saveMetrics.sjs)
        + This will save the metrics document as a **JSON**.
        + Ex:METRICS-PROCESS-MODULE=saveMetrics.sjs|ADHOC
* **Can I add the metrics document to a Collection?**
    + A comma seperated collection names can be assigned to METRICS-DOC-COLLECTIONS option and the document is saved to those collections.
    + By default the metrics document is added to a Collection with the Job Name (or the Job run location if name is not provided).
    
* **Can I change the URI to which the metrics document is saved?**
    * By default the URI format is /METRICS-DOC-BASE-DIR/CoRB2/JOB-NAME/YEAR/MONTH/DATE/HOUR/MINUTE/RANDOM-NUMBER.(json or xml)
    * METRICS-DOC-BASE-DIR has a default value of /ServiceMetrics/
    * JOB-NAME defaults to the job run location
     
* **I want to have complete control over how the metrics document is saved. Is that possible?**
    * You can use the above mentioned sample modules (*[saveMetrics.sjs](corb2/src/main/resources/saveMetrics.sjs) and [save-metric-to-db.xqy](corb2/src/main/resources/save-metric-to-db.xqy)*) as an example and implement your own customizations.
    + Eg:METRICS-PROCESS-MODULE=/export/home/dev/saveMetricsCustom.sjs|ADHOC
* **I want to keep logging metrics document at regular intervals. Is that possible?**
    * You can use METRICS-SYNC-FREQUENCY option to specify the frequency at which the document should be saved to the database.
    * Corb logs the metrics by creating a new document in the database with a new timestamp as shown below.
    * Eg: 
         + /my-dir/CoRB2/job-name/2017/9/22/16/16/1446989213638048899.xml
         + /my-dir/CoRB2/job-name/2017/9/22/16/16/1446989213638048899.xml/10656720599806190856
         + /my-dir/CoRB2/job-name/2017/9/22/16/16/1446989213638048899.xml/10846647439302775656
    
### Sample Metrics Document
```
{ "job" : {
    "name" : "TestJob",
    "runLocation" : "/data/MarkLogic/corb2",
    "host" : "www.marklogic.com",
    "userProvidedOptions" :
    {
        "METRICS-PROCESS-MODULE" : "/saveMetrics2.sjs",
        "METRICS-TO-ERROR-LOG" : "info",
        "METRICS-DB-NAME" : "Documents",
        "METRICS-NUM-FAILED-TRANSACTIONS" : "2",
        "METRICS-NUM-SLOW-TRANSACTIONS" : "4",
        "METRICS-DOC-COLLECTIONS" : "COLLECTION",
        "MODULE-ROOT" : "/",
        "URIS-FILE" : "test-file-1.txt"
    }
    ,
    "StartTime" : "2017-04-23T19:01:10Z",
    "endTime" : "2017-04-23T19:01:10Z",
    "totalNumberOfTasks" : "10",
    "totalRunTimeInMillis": "52074",
    "numberOfFailedTasks" : "2",
    "averageTransactionTimeInMillis" : "117.0",
    "urisLoadTimeInMillis" : "2",
    "preBatchRunTimeInMillis" : "16",
    "postBatchRunTimeInMillis" : "5",
    "failedTransactions" : ["uri1.xml","uri2.xml"],
    "slowTransactions" :[{
    "uri" : "uri.xml",
    "rank" : "1",
    "timeInMillis ": "20"}]
  }
}
```

### Sample Server Log

```
2017-04-27 10:19:26.814 Info: marklogic-corb-xdbc: STARTED CORB JOB:
2017-04-27 10:19:26.814 Info: marklogic-corb-xdbc: {"job":{"runLocation":"/home/dev/workspace/corb2","name":"testCorbMetrics","host":"localhost","StartTime":"2017-04-27T10:19:26Z"}}
2017-04-27 10:19:28.947 Info: marklogic-corb-xdbc: END RUNNING CORB JOB:
2017-04-27 10:19:28.947 Info: marklogic-corb-xdbc: {"job":{"runLocation":"/home/dev/workspace/corb2","name":"testCorbMetrics","host":"localhost","userProvidedOptions":{"METRICS-PROCESS-MODULE":"
```
