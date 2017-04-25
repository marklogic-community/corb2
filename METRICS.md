1. How to log metrics to the Marklogic error log?
..<p>Logging can be enabled by setting METRICS_TO_ERROR_LOG property in options.</p>
..<p>Ex: METRICS-TO-ERROR_LOG=info</p>
1. How to set the log level when logging metrics to the Marklogic error log?
..METRICS-TO-ERROR-LOG property has the following possible values:
none,emergency,alert,critical,error,warning,notice,info,config,debug,fine,finer,finest. 
..If none then metrics will not be logged.
1. How to save the metrics as a Document in the database?
..METRICS-DB-NAME property will be used to save the mertrics document to the database.
