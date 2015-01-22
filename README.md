For more information see "http://marklogic.github.com/corb/":http://marklogic.github.com/corb/  
Version: 2.0.5

## Running Corb

The entry point is the main method in the com.marklogic.developer.corb.Manager class. 

Corb needs one or more of the following parameters as (If specified in more then one place command line argument takes precedence over VM argument which take precedence over corb.properties)

1. command-line arguments 
2. VM arguments ex: -DXCC-CONNECTION-URI=xcc://user:password@localhost:8202 or 
3. As properties file in the class path specified using -DOPTIONS-FILE=myjob.properties. Relative and full file paths are also supported. 

## Options  

* XCC-CONNECTION-URI
* COLLECTION-NAME (can be empty string)
* XQUERY-MODULE (provide file system path if not contained in the corb package)
* THREAD-COUNT (number of worker threads; default = 1)
* URIS-MODULE (alternate URI selection module, replacing provided Corb default).
* MODULE-ROOT (assumes '/' if not provided)
* MODULES-DATABASE (uses the XCC-CONNECTION-URI if not provided; use 0 for filesystem)
* INSTALL (default is true; set to 'false' or '0' to skip installation)
* PROCESS-TASK (Java Class that implements com.marklogic.developer.Task or extends com.marklogic.developer.AbstractTask. It can talk to XQUERY-MODULE and do additional processing locally. Ex: ExportToFileTask (included) will save the document to local file system)
* PRE-BATCH-MODULE (XQuery module, if specified, will be run before batch processing starts)
* PRE-BATCH-TASK (Java Class that implements com.marklogic.developer.Task or extends com.marklogic.developer.AbstractTask- can be used in place or in addition to PRE-BATCH-MODULE. Ex: PreBatchUpdateFileTask (included) which adds additional content to the export file)
* POST-BATCH-MODULE (XQuery module, if specified, will be run after batch processing is completed)
* POST-BATCH-TASK (Java Class that implements com.marklogic.developer.Task or extends com.marklogic.developer.AbstractTask- can be used in place or in addition to POST-BATCH-MODULE. Ex: PostBatchUpdateFileTask (included) which adds additional content to the export file created by ExportToFileTask)
* EXPORT-FILE-DIR (export directory for com.marklogic.developer.corb.ExportToFileTask or similar tasks. If not specified, it defaults to user.dir)
* EXPORT-FILE-NAME (shared file to export output of com.marklogic.developer.corb.ExportBatchToFileTask - Not full path)
* URIS-FILE (If defined instead of URIS-MODULE, URIS will be loaded from the file located on the client)
* INIT-MODULE (XQuery Module, if specified, will be invoked prior to URIS-MODULE)
* INIT-TASK (Java Task, if specified, will be called prior to URIS-MODULE, This can be used addition to INIT-MODULE)

## Additional options via properties file

* EXPORT-FILE-PART-EXT (if specified, PreBatchUpdateFileTask adds this extension to export file. It is expected that PostBatchUpdateFileTask will be specified, which removes the extension for the final export file)
* EXPORT-FILE-TOP-CONTENT (used by PreBatchUpdateFileTask to insert content at the top to EXPORT_FILE_NAME before batch process starts, if it finds the text @URIS_BATCH_REF it replaces it by batch reference sent by URIS-MODULE)
* EXPORT-FILE-BOTTOM-CONTENT (used by PostBatchUpdateFileTask to append content to EXPORT_FILE_NAME after batch process is complete)
* EXPORT_FILE_AS_ZIP (if true, PreBatchUpdateFileTask compression the output file as a zip file)
* URIS-REPLACE-PATTERN (one or more replace patterns for URIs - typically used to save memory. usage: URIS-REPLACE-PATTERN=pattern1,replace1,pattern2,replace2,...)
* XCC-CONNECTION-RETRY-LIMIT (Number attempts to connect to ML before giving up - default is 3)
* XCC-CONNECTION-RETRY-INTERVAL (in seconds - Time interval in seconds between retry attempts - default is 60)

## Custom Inputs to XQuery modules

Any property specified with prefix (with '.') URIS-MODULE,XQUERY-MODULE,PRE-BATCH-MODULE,POST-BATCH-MODULE,INIT-MODULE, will be set as external variables to the corresponding xquery module (if defined).  
ex:  
URIS-MODULE.filePath  
XQUERY-MODULE.outputFolder   

## Internal Properties

URIS_BATCH_REF (This is not a user specified property. URIS-MODULE can optionally send this a batch reference which can be used by post batch hooks)

## Usage

### Usage 1:
java com.marklogic.developer.corb.Manager XCC-CONNECTION-URI [COLLECTION-NAME [XQUERY-MODULE [ THREAD-COUNT [ URIS-MODULE [ MODULE-ROOT [ MODULES-DATABASE [ INSTALL [ PROCESS-TASK [ PRE-BATCH-MODULE  [ PRE-BATCH-TASK [ POST-XQUERY-MODULE  [ POST-BATCH-TASK [ EXPORT-FILE-DIR [ EXPORT-FILE-NAME [ URIS-FILE ] ] ] ] ] ] ] ] ] ] ] ] ] ] ]

### Usage 2:
java -DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ] -DXQUERY-MODULE=module-name.xqy -DTHREAD-COUNT=10 -DURIS-MODULE=get-uris.xqy -DPOST-BATCH-XQUERY-MODULE=post-batch.xqy -D... com.marklogic.developer.corb.Manager

### Usage 3:
java -DOPTIONS-FILE=myjob.properties com.marklogic.developer.corb.Manager (looks for myjob.properties file in classpath)

### Usage 4:
java -DOPTIONS-FILE=myjob.properties -DTHREAD-COUNT=10 com.marklogic.developer.corb.Manager XCC-CONNECTION-URI

##  Sample myjob.properties

XCC-CONNECTION-URI=xcc://user:password@localhost:8202/  
XQUERY-MODULE=SampleCorbJob.xqy  
THREAD-COUNT=10  
URIS-MODULE=get-uris.xqy  
POST-BATCH-MODULE=post-batch.xqy  
XQUERY-MODULE=get-document.xqy  
PROCESS-TASK=com.marklogic.developer.corb.ExportToFileTask  
EXPORT-FILE-DIR=/temp/export  
POST-BATCH-TASK=com.marklogic.developer.corb.PostBatchUpdateFileTask  
