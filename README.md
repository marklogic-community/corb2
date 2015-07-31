Version: 2.1.2

### User Guide
This document provides a comprehensive overview of CoRB2.  For additional information, please refer to CoRB2 online [Wiki](https://github.com/marklogic/corb2/wiki) or download [WhatIsCORB.doc](https://github.com/marklogic/corb2/blob/master/WhatIsCORB.doc).  This document also covers the less robust [RunXQuery Tool]](#runXQuery-readme) which can be used when only a single staged query is necessary.  The RunXQuery Tool is provided as part of the CoRB2 distribution.

### Downloads
Please download latest release from https://github.com/marklogic/corb2/releases.  

For backwards compatibility with marklogic server, Corb releases upto v2.1.* are built using marklogic xcc v6.0.2. However, a later version of corb can be used for running the Corb. Please note that xcc v8.0.* is required to communicate with Marklogic 8 server. Also, please use java 1.7 or later for running corb. 

To build corb using ant, please specify java.library.user folder in the build.properties file and place marklogic-xcc-6.0.2.jar in this folder. Please update build.xml for building corb with a later version of xcc jar.   

### Running Corb
The entry point is the main method in the com.marklogic.developer.corb.Manager class. Corb requires marklogic xcc jar in the classpath, preferably the version that corresponds to marklogic server version, which can be downloaded from https://developer.marklogic.com/products/xcc (corb releases are tested with xcc versions 6.0, 7.0 and 8.0). Requires java 1.7 or later.

Corb needs one or more of the following parameters as (If specified in more than one place command line parameter takes precedence over java system property which take precedence over myjob.properties)

1. command-line parameters 
2. Java system properties ex: -DXCC-CONNECTION-URI=xcc://user:password@localhost:8202. 
3. As properties file in the class path specified using -DOPTIONS-FILE=myjob.properties. Relative and full file paths are also supported. 

Note: Any or all of the properties can be specified as java system properties or key value pairs in properties file.

### Options  
* XCC-CONNECTION-URI (Connection string to MarkLogic XDBC Server)
* COLLECTION-NAME (Value of this parameter will be passed into the URIS-MODULE via external or global variable with the name URIS)
* XQUERY-MODULE (XQuery or java script to be executed in a batch for each URI from the URIS-MODULE or URIS-FILE. Module is expected to have at least one external or global variable with name URI. XQuery and java script modules need to have .xqy and .sjs extensions respectively. If returning multiple values from a java script module, values must be returned as ValueIterator.)
* THREAD-COUNT (number of worker threads; default = 1)
* MODULE-ROOT (default: '/' for root)
* MODULES-DATABASE (uses the XCC-CONNECTION-URI if not provided; use 0 for file system)
* INSTALL (default is false; set to 'true' or '1' for installation)
* URIS-MODULE (URI selector module written in XQuery or JavaScript. Expected to return a sequence containing the uris count followed by all the uris. Optionally, it can also return an arbitrary string as a first item in this sequence - refer to URIS\_BATCH\_REF section below. XQuery and JavaScript modules need to have .xqy and .sjs extensions respectively. JavaScript modules must return a ValueIterator.)
* URIS-FILE (If defined instead of URIS-MODULE, URIS will be loaded from the file located on the client. There should only be one URI per each line. This path may be relative or absolute. For example, a file containing a list of document identifiers can be used as a URIS-FILE and XQUERY-MODULE can query for the document based on this document identifier.)
* PROCESS-TASK (Java Class that implements com.marklogic.developer.corb.Task or extends com.marklogic.developer.corb.AbstractTask. Typically, it can talk to XQUERY-MODULE and the do additional processing locally such save a returned value.)    
  * com.marklogic.developer.corb.ExportBatchToFileTask (Generates _**a single file**_, typically used for reports. Writes the data returned by the XQUERY-MODULE to a single file specified by EXPORT-FILE-NAME. All returned values from entire CoRB will be streamed into the single file. If EXPORT-FILE-NAME is not specified, CoRB uses URIS\_BATCH\_REF returned by URIS-MODULE as the file name.)   
  * com.marklogic.developer.corb.ExportToFileTask (Generates _**multiple files**_. Saves the documents returned by each invocation of XQUERY-MODULE to a separate local file within EXPORT-FILE-DIR where the file name for each document will be the based on the URI.)   
* PRE-BATCH-MODULE (An XQuery or JavaScript module which, if specified, will be run before batch processing starts. XQuery and JavaScript modules need to have .xqy and .sjs extensions respectively.)
* PRE-BATCH-TASK (Java Class that implements com.marklogic.developer.corb.Task or extends com.marklogic.developer.corb.AbstractTask. If PRE-BATCH-MODULE is also specified, the implementation is expected to invoke the XQuery and process the result if any. It can also be specified without PRE-BATCH-MODULE and an example of this is to add a static header to a report.)   
  * com.marklogic.developer.corb.PreBatchUpdateFileTask (included - Writes the data returned by the PRE-BATCH-MODULE to EXPORT-FILE-NAME, which can particularly be used to to write dynamic headers for CSV output. Also, if EXPORT-FILE-TOP-CONTENT is specified, this task will write this value to the EXPORT-FILE-NAME - this option is especially useful for writing fixed headers to reports. If EXPORT-FILE-NAME is not specified, CoRB uses URIS\_BATCH\_REF returned by URIS-MODULE as the file name.) 
* POST-BATCH-MODULE (An XQuery or JavaScript module which, if specified, will be run after batch processing is completed. XQuery and JavaScript modules need to have .xqy and .sjs extensions respectively.)
* POST-BATCH-TASK (Java Class that implements com.marklogic.developer.corb.Task or extends com.marklogic.developer.corb.AbstractTask. If POST-BATCH-MODULE is also specified, the implementation is expected to invoke the XQuery and process the result if any. It can also be specified without POST-BATCH-MODULE and an example of this is to add static content to the bottom of the report.)   
  com.marklogic.developer.corb.PostBatchUpdateFileTask (included - Writes the data returned by the POST-BATCH-MODULE to EXPORT-FILE-NAME. Also, if EXPORT-FILE-BOTTOM-CONTENT is specified, this task will write this value to the EXPORT-FILE-NAME. If EXPORT-FILE-NAME is not specified, CoRB uses URIS\_BATCH\_REF returned by URIS-MODULE as the file name.)
* EXPORT-FILE-DIR (Export directory parameter is used by com.marklogic.developer.corb.ExportBatchToFileTask or similar custom task implementations. Optional: Alternatively, EXPORT-FILE-NAME can be specified with a full path)
* EXPORT-FILE-NAME (shared file to write output of com.marklogic.developer.corb.ExportBatchToFileTask - should be a file name with our without full path. EXPORT-FILE-DIR is not required if full path is used. If EXPORT-FILE-NAME is not specified, CoRB attempts to use URIS\_BATCH\_REF as the file name and this is especially useful in case of automated jobs where file name can only be determined by the URIS-MODULE - refer to URIS\_BATCH\_REF section below)
* INIT-MODULE (An XQuery or JavaScript module which, if specified, will be invoked prior to URIS-MODULE. XQuery and JavaScript modules need to have .xqy and .sjs extensions respectively.)
* INIT-TASK (Java Task which, if specified, will be called prior to URIS-MODULE - this can be used addition to INIT-MODULE for custom implementations)

### Additional options
* EXPORT-FILE-PART-EXT (ex: .tmp - if specified, com.marklogic.developer.corb.PreBatchUpdateFileTask adds this temporary extension to the export file name to indicate EXPORT-FILE-NAME is being actively modified. To remove this temporary extension after EXPORT-FILE-NAME is complete, com.marklogic.developer.corb.PostBatchUpdateFileTask must be specified as POST-BATCH-TASK.)
* EXPORT-FILE-TOP-CONTENT (used by com.marklogic.developer.corb.PreBatchUpdateFileTask to insert content at the top of EXPORT-FILE-NAME before batch process starts. If it includes the string @URIS\_BATCH\_REF, it is replaced by the batch reference returned by URIS-MODULE)
* EXPORT-FILE-BOTTOM-CONTENT (used by com.marklogic.developer.corb.PostBatchUpdateFileTask to append content to EXPORT-FILE-NAME after batch process is complete)
* EXPORT_FILE_AS_ZIP (if true, PostBatchUpdateFileTask compresses the output file as a zip file)
* URIS-REPLACE-PATTERN (one or more replace patterns for URIs - Used by java to truncate the length of URIs on the client side, typically to reduce java heap size in very large batch jobs, as the CoRB java client holds all the URIS in memory while processing is in progress. If truncated, XQUERY-MODULE needs to reconstruct the URI before trying to do fn:doc() to fetch the document. Usage: URIS-REPLACE-PATTERN=pattern1,replace1,pattern2,replace2,...)  
  *Example:*  
  URIS-REPLACE-PATTERN=/com/marklogic/sample/,,.xml,  (Replace /com/marklogic/sample/ and .xml with empty strings. So, Corb client only need to cache the id '1234' instead of the entire URI /com/marklogic/sample/1234.xml. In the transform XQUERY-MODULE, we need to do let $URI := fn:concat("/com/marklogic/sample/",$URI,".xml"))
* XCC-CONNECTION-RETRY-LIMIT (Number attempts to connect to ML before giving up - default is 3)
* XCC-CONNECTION-RETRY-INTERVAL (in seconds - Time interval in seconds between retry attempts - default is 60)

### Alternate XCC connection configuration
* XCC-USERNAME (Required if XCC-CONNECTION-URI is not specified)
* XCC-PASSWORD (Required if XCC-CONNECTION-URI is not specified)
* XCC-HOSTNAME (Required if XCC-CONNECTION-URI is not specified)
* XCC-PORT (Required if XCC-CONNECTION-URI is not specified)
* XCC-DBNAME (Optional)

### Custom Inputs to XQuery or JavaScript Modules
Any property specified with prefix (with '.') URIS-MODULE,XQUERY-MODULE,PRE-BATCH-MODULE,POST-BATCH-MODULE,INIT-MODULE, will be set as an external variable in the corresponding XQuery module (if that variable is defined as an external string variable in XQuery module). For JavaScript modules the variables need be defined as global variables.  

**Examples:**  
URIS-MODULE.maxLimit=1000 (Expects an external string variable  _maxLimit_ in URIS-MODULE XQuery or global variable for JavaScript)  
XQUERY-MODULE.startDate=2015-01-01 (Expects an external string variable _startDate_ in XQUERY-MODULE XQuery or global variable for JavaScript)  

### Adhoc Modules
Appending "|ADHOC" to the name or path of a XQuery module (with .xqy extension) or JavaScript (with .sjs or .js extension) module will cause the module to be read off the file system and executed in MarkLogic without being uploaded to Modules database. This simplifies running CoRB jobs by not requiring deployment of any code to MarkLogic, and makes set of CoRB2 files and configuration more self contained.   

INIT-MODULE, URIS-MODULE, XQUERY-MODULE, PRE-BATCH-MODULE and POST-BATCH-MODULE can be specified adhoc by adding prefix '|ADHOC' for XQuery or JavaScript (with .sjs or .js extension) at the end. Adhoc XQuery or JavaScript remains local to the CoRB and not deployed to MarkLogic. The XQuery or JavaScript module should be in its named file and that file should be available on the file system, including being on the java classpath for CoRB.  

**Examples:**  
PRE-BATCH-MODULE=adhoc-pre-batch.xqy|ADHOC (adhoc-pre-batch.xqy must be on the classpath or in the current directory)  
XQUERY-MODULE=/path/to/file/adhoc-transform-module.xqy|ADHOC (xquery module file with full path in the file system)  
URIS-MODULE=adhoc-uris.sjs|ADHOC (Adhoc JavaScript module in the classpath or current directory)

### JavaScript Modules
JavaScript modules are supported with Marklogic 8 and can be used in place of an xquery module. However, if returning multiple values (ex: URIS-MODULE), values must be returned as ValueIterator. MarkLogic JavaScript API has helper functions to convert Arrays into ValueIterator (xdmp.arrayValues()) and inserting values into another ValueIterator (fn.insertBefore()). 

JavaScritp module must have .sjs file extension when deployed to Modules database. However, adhoc JavaScript modules support both .sjs or .js file extensions. 

For example, a simple URIS-MODULE may look like this

var uris = cts.uris()  
fn.insertBefore(uris,0,uris.count)

To return URIS\_BATCH\_REF, we can do the following   
fn.insertBefore(fn.insertBefore(uris,0,uris.count),0,"batch\-ref") 

### Encryption
It is often required to protect the database connection string or password from unauthorized access. So, CoRB optionally supports encryption of entire XCC URL or any parts of the XCC URL (if individually specified) such as XCC-PASSWORD. 
 
* DECRYPTER (Must extend com.marklogic.developer.corb.AbstractDecrypter. Encryptable options include XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and XCC-DBNAME)   
  * com.marklogic.developer.corb.PrivateKeyDecrypter (Included, requires private key file)  
  * com.marklogic.developer.corb.JasyptDecrypter (Included, requires jasypt-*.jar in classpath)
* PRIVATE-KEY-FILE (Required property for PrivateKeyDecrypter, should be accessible in classpath or file system)
* PRIVATE-KEY-ALGORITHM (Optional - Default algorithm for PrivateKeyDecrypter is RSA. Default algorithm for JasyptDecrypter is PBEWithMD5AndTripleDES)
* JASYPT-PROPERTIES-FILE (Optional property for JasyptDecrypter. If not specified, it uses default jasypt.proeprties file, which should be accessible in the classpath or file system.)  

#### com.marklogic.developer.corb.PrivateKeyDecrypter
PrivateKeyDecrypter automatically detects if the text is encrypted. Unencrypted text or clear text is returned as-is. Though, not required, encrypted text can be optionally enclosed with "ENC" ex: ENC(xxxxxx) to clearly indicate that it is encrypted.  

Generate keys and encrypt XCC URL or password using one of the options below.   

**Java Crypt**  
* Use the PrivateKeyDecrypter class inside the corb jar with the gen-keys option to generate a key.  
  java -cp marklogic-corb-2.1.*.jar com.marklogic.developer.corb.PrivateKeyDecrypter gen-keys /path/to/private.key /path/to/public.key RSA 1024 (Note: if not specified, default algorithm: RSA, default key-length: 1024)
* Use the PrivateKeyDecrypter class inside the corb jar with the encrypt option to encrypt the clear text such as an xcc URL or password.  
  java -cp marklogic-corb-2.1.*.jar com.marklogic.developer.corb.PrivateKeyDecrypter encrypt /path/to/public.key clearText RSA (Note: if not specified, default algorithm: RSA)

**RSA keys**  
* openssl genrsa -out private.pem 1024 (generate private key in PEM format)
* openssl pkcs8 -topk8 -nocrypt -in private.pem -out private.pkcs8.key (create PRIVATE-KEY-FILE in PKCS8 standard for java)
* openssl rsa -in private.pem -pubout > public.key  (extract public key)
* echo "uri or password" | openssl rsautl -encrypt -pubin -inkey public.key | base64 (encrypt URI or password. Optionally, the encrypted text can be enclosed with "ENC" ex: ENC(xxxxxx))

**ssh-keygen**  
* ssh-keygen (ex:key as id_rsa after selecting a passphrase)
* openssl pkcs8 -topk8 -nocrypt -in id_rsa -out id_rsa.pkcs8.key (asks for passphrase)
* openssl rsa -in id_rsa -pubout > public.key (asks for passphrase)
* echo "password or uri" | openssl rsautl -encrypt -pubin -inkey public.key | base64

#### com.marklogic.developer.corb.JasyptDecrypter
JasyptDecrypter automatically detects if the text is encrypted. Unencrypted text or clear text is returned as-is. Though, not required, encrypted text can be optionally enclosed with "ENC" ex: ENC(xxxxxx) to clearly indicate that it is encrypted.    

Encrypt the URI or password as below. It is assumed that jasypt distribution is available on your machine.   
jasypt-1.9.2/bin/encrypt.sh input="uri or password" password="passphrase" algorithm="algorithm" (ex: PBEWithMD5AndTripleDES or PBEWithMD5AndDES)  

**jasypt.properties file**  
jasypt.algorithm=PBEWithMD5AndTripleDES (If not specified, default is PBEWithMD5AndTripleDES)  
jasypt.password=passphrase

#### URIS\_BATCH\_REF
If a module, including those specified by PRE-BATCH-MODULE, XQUERY-MODULE or POST-BATCH-MODULE have an external or global variable named URIS\_BATCH\_REF, the variable will be set to the first item in the sequence or ValueIterator returned by URIS-MODULE. This means that, when used, the URIS-MODULE must return a sequence or ValueIterator with the special string value first, then the URI count, then the sequence of URIs to process.  

As an example, a batch ref can be a link/id of a document that manages the status of the batch job, where pre-batch module updates the status to start and post-batch module can set it to complete. This example can be used to manage status and errors in automated batch jobs.   

ExportBatchToFileTask, PreBatchUpdateFileTask and PostBatchUpdateFileTask use URIS\_BATCH\_REF as the file name if EXPORT-FILE-NAME is not specified. This is useful for automated jobs where name of the output file name can be determined only by the URIS-MODULE.  


### Usage
#### Usage 1 (Command line options):
java com.marklogic.developer.corb.Manager XCC-CONNECTION-URI [COLLECTION-NAME [XQUERY-MODULE [ THREAD-COUNT [ URIS-MODULE [ MODULE-ROOT [ MODULES-DATABASE [ INSTALL [ PROCESS-TASK [ PRE-BATCH-MODULE  [ PRE-BATCH-TASK [ POST-XQUERY-MODULE  [ POST-BATCH-TASK [ EXPORT-FILE-DIR [ EXPORT-FILE-NAME [ URIS-FILE ] ] ] ] ] ] ] ] ] ] ] ] ] ] ]

#### Usage 2 (Java system properties specifying options):
java -DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ] -DXQUERY-MODULE=module-name.xqy -DTHREAD-COUNT=10 -DURIS-MODULE=get-uris.xqy -DPOST-BATCH-XQUERY-MODULE=post-batch.xqy -D... com.marklogic.developer.corb.Manager

#### Usage 3 (Properties file specifying options):
java -DOPTIONS-FILE=myjob.properties com.marklogic.developer.corb.Manager (looks for myjob.properties file in classpath)

#### Usage 4 (Combination of properties file with java system properties and command line options):
java -DOPTIONS-FILE=myjob.properties -DTHREAD-COUNT=10 com.marklogic.developer.corb.Manager XCC-CONNECTION-URI

###  Sample myjob.properties (Note: any of the properties below can be specified as java system property i.e. '-D' option)

##### sample 1 - simple batch
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy  
XQUERY-MODULE=transform.xqy  

##### sample 2 - Use username, password, host and port instead of connection URI
XCC-USERNAME=username   
XCC-PASSWORD=password   
XCC-HOSTNAME=localhost   
XCC-PORT=9999   
XCC-DBNAME=ML-database   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy  
XQUERY-MODULE=SampleCorbJob.xqy 

##### sample 3 - simple batch with URIS-FILE (in place of URIS-MODULE)
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-FILE=input-uris.csv  
XQUERY-MODULE=SampleCorbJob.xqy  

##### sample 4 - report, generates a single file with data from processing each URI
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB 
XQUERY-MODULE=get-data-from-document.xqy   
PROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask   
EXPORT-FILE-NAME=/local/path/to/exportmyfile.csv   

##### sample 5 - report with header, add following to sample 4. 
...    
PRE-BATCH-TASK=com.marklogic.developer.corb.PreBatchUpdateFileTask  
EXPORT-FILE-TOP-CONTENT=col1,col2,col3  

##### sample 6 - dynamic headers, assuming pre-batch-header.xqy module returns the header row, add following to sample 4.
...        
PRE-BATCH-MODULE=pre-batch-header.xqy  
PRE-BATCH-TASK=com.marklogic.developer.corb.PreBatchUpdateFileTask   

##### sample 7 - pre and post batch hooks
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy  
XQUERY-MODULE=transform.xqy  
PRE-BATCH-MODULE=pre-batch.xqy   
POST-BATCH-MODULE=post-batch.xqy   

##### sample 8 - adhoc tasks (xquery modules live local to filesystem where corb is located. Any xquery module can be adhoc)
XCC-CONNECTION-URI=xcc://user:password@localhost:8202/   
THREAD-COUNT=10  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB   
URIS-MODULE=get-uris.xqy|ADHOC   
XQUERY-MODULE=SampleCorbJob.xqy|ADHOC   
PRE-BATCH-MODULE=/local/path/to/adhoc-pre-batch.xqy|ADHOC

##### sample 9 - jasypt encryption (XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and/or XCC-DBNAME properties can be encrypted and optionally enclosed by ENC(). If JASYPT-PROPERTIES-FILE is not specified, it assumes default jasypt.properties)
XCC-CONNECTION-URI=ENC(encrypted_uri)   
...   
DECRYPTER=com.marklogic.developer.corb.JasyptDecrypter  

**sample jasypt.properties**   
jasypt.password=foo   
jasypt.algorithm=PBEWithMD5AndTripleDES  

##### sample 10 - private key encryption with java keys (XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and/or XCC-DBNAME properties can be encrypted and optionally enclosed by ENC())
XCC-CONNECTION-URI=encrypted_uri  
...   
DECRYPTER=com.marklogic.developer.corb.PrivateKeyDecrypter  
PRIVATE-KEY-FILE=/path/to/key/private.key  
PRIVATE-KEY-ALGORITHM=RSA  

##### sample 11 - private key encryption with unix keys (XCC-CONNECTION-URI, XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME, XCC-PORT and/or XCC-DBNAME properties can be encrypted and optionally enclosed by ENC())
XCC-CONNECTION-URI=encrypted_uri  
...   
DECRYPTER=com.marklogic.developer.corb.PrivateKeyDecrypter  
PRIVATE-KEY-FILE=/path/to/rsa/key/rivate.pkcs8.key  

##### sample 12 - JavaScript modules depolyed to modules database
...  
MODULE-ROOT=/temp/  
MODULES-DATABASE=MY-Modules-DB  
URIS-MODULE=get-uris.sjs  
XQUERY-MODULE=transform.sjs  

##### sample 13 - Adhoc JavaScript modules 
..  
URIS-MODULE=get-uris.sjs|ADHOC  
XQUERY-MODULE=extract.sjs|ADHOC  


### RunXQuery Tool <a id="runXQuery-readme"></a>
 
Sometimes, a two or more staged CoRB job isn't necessary to get the job done.  Sometimes, only a single query needs to be executed and the output captured to file.  Maybe only a single query with no output captured?  In these cases, the RunXQuery Tool can be used to quickly and efficiently execute your XQuery or JavaScript files.

Yes, that's right.  Like CoRB, with Version 8 or higher of the MarkLogic XCC Connection Jar in your classpath, the RunXQuery Tool can run JavaScript queries against a MarkLogic8 server.  We know, bad name for the tool but what can we say, old habits die hard!

So how does the RunXQuery Tool differ from CoRB?  The key differences are:

+ There is only one stage in the process so it can only run one query unlike CoRB which is multi staged (init, pre-batch, uri-module, process-module, post-batch)
+ It is a single threaded application

That's it.  Doesn't seem like a lot but it actually limits its functionality significantly.  So what does the RunXQuery Tool have in common with CoRB?  Quite a bit:

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
  
So how do you use it?   For convenience, it can be configured using the same techniques as CoRB provides and using the same parameter names. The big difference is that there are far fewer parameters needed and there is a different class used for its execution (com.marklogic.developer.corb.RunXQueryManager).

The following parameters are supported and can be used in the same ways as described above for CoRB:

+ XQUERY-MODULE
+ XQUERY-MODULE.customVariableNameExample
+ MODULES-DATABASE
+ MODULES-ROOT
+ DECRYPTER=com.marklogic.developer.corb.JasyptDecrypter
+ PROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask
+ EXPORT-FILE-NAME
+ XCC-CONNECTION-URI
+ XCC-USERNAME
+ XCC-PASSWORD
+ XCC-HOSTNAME
+ XCC-PORT
+ EXPORT-FILE-DIR
+ EXPORT-FILE-NAME

The following are example usages from a Windows console:

##### Usage 1

java -cp pathToXCC.jar:pathToCoRB.jar RunXQueryManager xcc://user:password@host:port/[ database ] ^
         xqueryOrJavascriptModuleName moduleRootName modulesDatabaseName com.marklogic.developer.corb.ExportBatchToFileTask ^
         c:\\myPath\\to\\file\\directory myFileName
         
##### Usage 2

java -cp pathToXCC.jar:pathToCoRB.jar -DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ] ^
	 -DXQUERY-MODULE=module-name.xqy -DPROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask ^
         -DXQUERY-MODULE.collectionName=myCollectionName RunXQueryManager
         
##### Usage 3

java -cp pathToXCC.jar:pathToCoRB.jar:pathToJasypt.jar -DOPTIONS-FILE=myJob.properties RunXQueryManager

Where myJob.properties has:

XQUERY-MODULE=/test/HelloWorld.xqy
XQUERY-MODULE.lastName=Smith
MODULES-DATABASE=My-Modules
DECRYPTER=com.marklogic.developer.corb.JasyptDecrypter
XQUERY-MODULE.collectionName=myCollectionName
PROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask
EXPORT-FILE-NAME=C:\\Users\\jon.smith\\Documents\\runXQueryOutput.log
XCC-CONNECTION-URI=ENC(fslfuoifsdofjjwfckmeflkjlj377239843u)

##### Usage 4

java -cp pathToXCC.jar:pathToCoRB.jar:pathToJasypt.jar -DOPTIONS-FILE=myJob.properties ^
         RunXQueryManager ENC(fslfuoifsdofjjwfckmeflkjlj377239843u)



    
