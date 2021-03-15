# Examples

##Setup
In order to run any of the example jobs locally, run:

    ./gradlew setup

This will invoke ml-gradle task `mlDeploy` to setup a content and modules database, as well as an application server.

For convenience, the `setup` task will also execute the `createCorbShell` and `createCorbBatch` tasks to generate a `corb.sh` shell script and `corb.bat` Windows batch file. It sets the classpath and the XCC connection string dusing values specified in the gradle.properties.

##Running Example Jobs

Specify the **OPTIONS-FILE** for one of the example jobs 
and any other CoRB options that you would like to apply or override from the example jobs.

When executing the `corb` task, set the options file as a project property with the `-P` switch:

    ./gradlew corb -PcorbOptionsFile=inline-adhoc/job.properties

You could also choose to use the generated corb.sh and set **OPTIONS-FILE** as a system property with the `-D` switch:

    ./corb.sh -DOPTIONS-FILE=inline-adhoc/job.properties 

or the corb.bat file:

    corb -DOPTIONS-FILE=inline-adhoc/job.properties 
