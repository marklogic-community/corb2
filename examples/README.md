# Examples

##Setup
In order to run any of the example jobs locally, run:

    ./gradlew setup

This will invoke ml-gradle task `mlDeploy` to setup a content and modules database, as well as an application server.

For convenience, the `setup` task will also execute the `createCorbShell` and `createCorbBatch` tasks to generate a `corb.sh` shell script and `corb.bat` Windows batch file. It sets the classpath and the XCC connection string dusing values specified in the gradle.properties.

##Running Example Jobs

Specify the **OPTIONS-FILE** for one of the example jobs 
and any other CoRB options that you would like to apply or override from the example jobs.

Each example directory also includes a brief `README.md` describing what feature it demonstrates.

When executing the `corb` task, set the options file as a project property with the `-P` switch:

    ./gradlew corb -PcorbOptionsFile=PostBatchUpdateFileTask-dedup/job.properties
    ./gradlew corb -PcorbOptionsFile=batch-size-delimited/job.properties
    ./gradlew corb -PcorbOptionsFile=feature-demo/job.properties
    ./gradlew corb -PcorbOptionsFile=inline-adhoc/job.properties
    ./gradlew corb -PcorbOptionsFile=javascript/job.properties
    ./gradlew corb -PcorbOptionsFile=loader-json/job.properties
    ./gradlew corb -PcorbOptionsFile=loader-json-streaming/job.properties
    ./gradlew corb -PcorbOptionsFile=loader-xml/job.properties
    ./gradlew corb -PcorbOptionsFile=loader-zip/job.properties
    ./gradlew corb -PcorbOptionsFile=schemaValidate/job.properties
    ./gradlew corb -PcorbOptionsFile=schemaValidateBatch/job.properties
    ./gradlew corb -PcorbOptionsFile=split-by-lines/job.properties
    ./gradlew corb -PcorbOptionsFile=split-by-size/job.properties
    ./gradlew corb -PcorbOptionsFile=test/job.properties
    ./gradlew corb -PcorbOptionsFile=uris-file-basic/job.properties
    ./gradlew corb -PcorbOptionsFile=zip-export/job.properties

You could also choose to use the generated corb.sh and set **OPTIONS-FILE** as a system property with the `-D` switch:

    ./corb.sh -DOPTIONS-FILE=inline-adhoc/job.properties 

or the corb.bat file:

    corb -DOPTIONS-FILE=inline-adhoc/job.properties 
