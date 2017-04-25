* How to log metrics to the Marklogic error log?
>>Logging can be enabled by setting METRICS_TO_ERROR_LOG property in options.
*Ex: METRICS-TO-ERROR_LOG=info*
* How to set the log level when logging metrics to the Marklogic error log?
>>METRICS-TO-ERROR-LOG property has the following possible values:
*none,emergency,alert,critical,error,warning,notice,info,config,debug,fine,finer,finest.*
If none then metrics will not be logged.
* How to save the metrics as a Document in the database?
>>METRICS-DB-NAME property will be used to save the mertrics document to the database.
```javascript 
{
"job":
{
"runLocation": "/home/vsaradhi/workspace/corb2",
"userProvidedOptions":
{
PROCESS-MODULE": "src/test/resources/transform.xqy|ADHOC",
"METRICS-DB-NAME": "Documents",
"URIS-FILE": "src/test/resources/test-file-1.txt"
}
,
"StartTime": "2017-04-23T19:01:10Z",
"endTime": "2017-04-23T19:01:10Z",
"host": "localhost",
"totalNumberOfTasks": "1",
"averageTransactionTimeInMillis": "117.0",
"urisLoadTimeInMillis": "2",
"preBatchRunTimeInMillis": "16",
"postBatchRunTimeInMillis": "5"
}
}```
