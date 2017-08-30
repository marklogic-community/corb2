/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.BATCH_SIZE;
import static com.marklogic.developer.corb.Options.COLLECTION_NAME;
import static com.marklogic.developer.corb.Options.COMMAND_FILE;
import static com.marklogic.developer.corb.Options.DISK_QUEUE;
import static com.marklogic.developer.corb.Options.DISK_QUEUE_TEMP_DIR;
import static com.marklogic.developer.corb.Options.DISK_QUEUE_MAX_IN_MEMORY_SIZE;
import static com.marklogic.developer.corb.Options.ERROR_FILE_NAME;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_DIR;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_PART_EXT;
import static com.marklogic.developer.corb.Options.FAIL_ON_ERROR;
import static com.marklogic.developer.corb.Options.INIT_MODULE;
import static com.marklogic.developer.corb.Options.INIT_TASK;
import static com.marklogic.developer.corb.Options.INSTALL;
import static com.marklogic.developer.corb.Options.MODULES_DATABASE;
import static com.marklogic.developer.corb.Options.MODULE_ROOT;
import static com.marklogic.developer.corb.Options.NUM_TPS_FOR_ETC;
import static com.marklogic.developer.corb.Options.OPTIONS_FILE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MINIMUM_COUNT;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_TASK;
import static com.marklogic.developer.corb.Options.POST_BATCH_XQUERY_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MINIMUM_COUNT;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_TASK;
import static com.marklogic.developer.corb.Options.PRE_BATCH_XQUERY_MODULE;
import static com.marklogic.developer.corb.Options.PRE_POST_BATCH_ALWAYS_EXECUTE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_TASK;
import static com.marklogic.developer.corb.Options.TEMP_DIR;
import static com.marklogic.developer.corb.Options.THREAD_COUNT;
import static com.marklogic.developer.corb.Options.URIS_FILE;
import static com.marklogic.developer.corb.Options.URIS_LOADER;
import static com.marklogic.developer.corb.Options.URIS_MODULE;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_URI;
import static com.marklogic.developer.corb.Options.XQUERY_MODULE;
import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.NumberUtils;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * @author Colleen Whitney, MarkLogic Corporation
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class Manager extends AbstractManager {

    private static final String END_RUNNING_JOB_MESSAGE = "END RUNNING CORB JOB:";
    private static final String START_RUNNING_JOB_MESSAGE = "STARTED CORB JOB:";

    protected static final String NAME = Manager.class.getName();

    public static final String URIS_BATCH_REF = com.marklogic.developer.corb.Options.URIS_BATCH_REF;
    public static final String DEFAULT_BATCH_URI_DELIM = ";";

    protected transient PausableThreadPoolExecutor pool;
    protected transient Monitor monitor;
    protected transient MetricsDocSyncJob metricsDocSyncJob;
    protected transient JobServer jobServer = null;
    protected JobStats jobStats = null;
    protected long startMillis;
    protected long transformStartMillis;
    protected long endMillis;

    protected transient Thread monitorThread;
    protected transient CompletionService<String[]> completionService;
    protected transient ScheduledExecutorService scheduledExecutor;

    protected boolean execError;
    protected boolean stopCommand;

    protected static int EXIT_CODE_NO_URIS = EXIT_CODE_SUCCESS;
    protected static final int EXIT_CODE_STOP_COMMAND = 3;

    private static final Logger LOG = Logger.getLogger(Manager.class.getName());
    private static final String TAB = "\t";

    /**
     * @param args
     */
    public static void main(String... args) {
        Manager manager = new Manager();
        try {
            manager.init(args);
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error initializing CORB " + exc.getMessage(), exc);
            manager.usage();
            System.exit(EXIT_CODE_INIT_ERROR);
        }
        //now we can start corb.
        try {
            int count = manager.run();
            if (manager.execError) {
                System.exit(EXIT_CODE_PROCESSING_ERROR);
            } else if (manager.stopCommand) {
                System.exit(EXIT_CODE_STOP_COMMAND);
            } else if (count == 0) {
                System.exit(EXIT_CODE_NO_URIS);
            } else {
                System.exit(EXIT_CODE_SUCCESS);
            }
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error while running CORB", exc);
            System.exit(EXIT_CODE_PROCESSING_ERROR);
        }
    }

    @Override
    public void init(String[] commandlineArgs, Properties props) throws CorbException {
        super.init(commandlineArgs, props);

        prepareModules();

        String[] args = commandlineArgs;
        if (args == null) {
            args = new String[0];
        }
        String collectionName = getOption(args, 1, COLLECTION_NAME);
        collection = collectionName == null ? "" : collectionName;

        EXIT_CODE_NO_URIS = NumberUtils.toInt(getOption(Options.EXIT_CODE_NO_URIS));

        scheduleCommandFileWatcher();
    }

    protected void scheduleCommandFileWatcher() {
        String commandFile = getOption(COMMAND_FILE);
        if (isNotBlank(commandFile)) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            CommandFileWatcher commandFileWatcher = new CommandFileWatcher(FileUtils.getFile(commandFile), this);
            int pollInterval = NumberUtils.toInt(getOption(Options.COMMAND_FILE_POLL_INTERVAL), 1);
            scheduledExecutor.scheduleWithFixedDelay(commandFileWatcher, pollInterval, pollInterval, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void initOptions(String... args) throws CorbException {
        super.initOptions(args);
        // gather inputs
        String processModule = getOption(args, 2, PROCESS_MODULE);
        String threadCount = getOption(args, 3, THREAD_COUNT);
        String urisModule = getOption(args, 4, URIS_MODULE);
        String moduleRoot = getOption(args, 5, MODULE_ROOT);
        String modulesDatabase = getOption(args, 6, MODULES_DATABASE);
        String install = getOption(args, 7, INSTALL);
        String processTask = getOption(args, 8, PROCESS_TASK);
        String preBatchModule = getOption(args, 9, PRE_BATCH_MODULE);
        String preBatchTask = getOption(args, 10, PRE_BATCH_TASK);
        String postBatchModule = getOption(args, 11, POST_BATCH_MODULE);
        String postBatchTask = getOption(args, 12, POST_BATCH_TASK);
        String exportFileDir = getOption(args, 13, EXPORT_FILE_DIR);
        String exportFileName = getOption(args, 14, EXPORT_FILE_NAME);
        String urisFile = getOption(args, 15, URIS_FILE);

        String urisLoader = getOption(URIS_LOADER);
        if (urisLoader != null) {
            try {
                options.setUrisLoaderClass(getUrisLoaderCls(urisLoader));
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
                throw new CorbException("Unable to instantiate UrisLoader Class: " + urisLoader, ex);
            }
        }

        String initModule = getOption(INIT_MODULE);
        String initTask = getOption(INIT_TASK);

        String batchSize = getOption(BATCH_SIZE);
        String failOnError = getOption(FAIL_ON_ERROR);
        String errorFileName = getOption(ERROR_FILE_NAME);

        options.setUseDiskQueue(stringToBoolean(getOption(DISK_QUEUE)));
        String diskQueueMaxInMemorySize = getOption(DISK_QUEUE_MAX_IN_MEMORY_SIZE);
        String diskQueueTempDir = getOption(DISK_QUEUE_TEMP_DIR);
        String tempDir = getOption(TEMP_DIR);
        if (isBlank(diskQueueTempDir) && isNotBlank(tempDir)) {
            diskQueueTempDir = tempDir;
        }
        String numTpsForETC = getOption(NUM_TPS_FOR_ETC);

        //Check legacy properties keys, for backwards compatibility
        if (processModule == null) {
            processModule = getOption(XQUERY_MODULE);
        }
        if (preBatchModule == null) {
            preBatchModule = getOption(PRE_BATCH_XQUERY_MODULE);
        }
        if (postBatchModule == null) {
            postBatchModule = getOption(POST_BATCH_XQUERY_MODULE);
        }
        if (moduleRoot != null) {
            options.setModuleRoot(moduleRoot);
        }
        if (processModule != null) {
            options.setProcessModule(processModule);
        }
        if (threadCount != null) {
            options.setThreadCount(Integer.parseInt(threadCount));
        }
        if (urisModule != null) {
            options.setUrisModule(urisModule);
        }
        if (modulesDatabase != null) {
            options.setModulesDatabase(modulesDatabase);
        }
        if (install != null && ("true".equalsIgnoreCase(install) || "1".equals(install))) {
            options.setDoInstall(true);
        }
        if (urisFile != null) {
            File f = new File(urisFile);
            if (!f.exists()) {
                throw new IllegalArgumentException("Uris file " + urisFile + " not found");
            }
            options.setUrisFile(urisFile);
        }
        if (batchSize != null) {
            options.setBatchSize(Integer.parseInt(batchSize));
        }
        if (failOnError != null && "false".equalsIgnoreCase(failOnError)) {
            options.setFailOnError(false);
        }
        if (diskQueueMaxInMemorySize != null) {
            options.setDiskQueueMaxInMemorySize(Integer.parseInt(diskQueueMaxInMemorySize));
        }
        if (numTpsForETC != null) {
            options.setNumTpsForETC(Integer.parseInt(numTpsForETC));
        }

        options.setPrePostBatchAlwaysExecute(stringToBoolean(getOption(PRE_POST_BATCH_ALWAYS_EXECUTE)));

        String postBatchMinimumCount = getOption(POST_BATCH_MINIMUM_COUNT);
        if (StringUtils.isNotEmpty(postBatchMinimumCount)) {
            options.setPostBatchMinimumCount(Integer.parseInt(postBatchMinimumCount));
        }

        String preBatchMinimumCount = getOption(PRE_BATCH_MINIMUM_COUNT);
        if (StringUtils.isNotEmpty(preBatchMinimumCount)) {
            options.setPreBatchMinimumCount(Integer.parseInt(preBatchMinimumCount));
        }

        if (!properties.containsKey(EXPORT_FILE_DIR) && exportFileDir != null) {
            properties.put(EXPORT_FILE_DIR, exportFileDir);
        }
        if (!properties.containsKey(EXPORT_FILE_NAME) && exportFileName != null) {
            properties.put(EXPORT_FILE_NAME, exportFileName);
        }
        if (!properties.containsKey(ERROR_FILE_NAME) && errorFileName != null) {
            properties.put(ERROR_FILE_NAME, errorFileName);
        }

        if (initModule != null) {
            options.setInitModule(initModule);
        }
        if (preBatchModule != null) {
            options.setPreBatchModule(preBatchModule);
        }
        if (postBatchModule != null) {
            options.setPostBatchModule(postBatchModule);
        }

        // java class for processing individual tasks.
        // If specified, it is used instead of xquery module, but xquery module is
        // still required.
        try {
            if (initTask != null) {
                options.setInitTaskClass(getTaskCls(INIT_TASK, initTask));
            }
            if (processTask != null) {
                options.setProcessTaskClass(getTaskCls(PROCESS_TASK, processTask));
            }
            if (preBatchTask != null) {
                options.setPreBatchTaskClass(getTaskCls(PRE_BATCH_TASK, preBatchTask));
            }
            if (postBatchTask != null) {
                options.setPostBatchTaskClass(getTaskCls(POST_BATCH_TASK, postBatchTask));
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            throw new CorbException("Unable to instantiate class", ex);
        }

        if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
            throw new NullPointerException(PROCESS_TASK + " or " + PROCESS_MODULE + " must be specified");
        }

        if (options.getPostBatchTaskClass() == null) {
            if (properties.containsKey(EXPORT_FILE_PART_EXT)) {
                properties.remove(EXPORT_FILE_PART_EXT);
            }
            if (System.getProperty(EXPORT_FILE_PART_EXT) != null) {
                System.clearProperty(EXPORT_FILE_PART_EXT);
            }
        }

        if (exportFileDir != null) {
            File dirFile = new File(exportFileDir);
            if (dirFile.exists() && dirFile.canWrite()) {
                options.setExportFileDir(exportFileDir);
            } else {
                throw new IllegalArgumentException("Cannot write to export folder " + exportFileDir);
            }
        }

        if (diskQueueTempDir != null) {
            File dirFile = new File(diskQueueTempDir);
            if (dirFile.exists() && dirFile.canWrite()) {
                options.setDiskQueueTempDir(dirFile);
            } else {
                throw new IllegalArgumentException("Cannot write to queue temp directory " + diskQueueTempDir);
            }
        }
        /**/
        String logMetricsToServerLog = getOption(Options.METRICS_TO_ERROR_LOG);
        if (logMetricsToServerLog != null) {
            if (logMetricsToServerLog.toLowerCase().matches(Options.ML_LOG_LEVELS)) {
                options.setLogMetricsToServerLog(logMetricsToServerLog.toLowerCase());
            } else {
                throw new IllegalArgumentException("INVALID VALUE for METRICS-TO-ERROR-LOG: " + logMetricsToServerLog + ". Supported LOG LEVELS are " + Options.ML_LOG_LEVELS);

            }
        }
        String logMetricsToServerCollections = getOption(Options.METRICS_DOC_COLLECTIONS);
        if (logMetricsToServerCollections != null) {
            options.setLogMetricsToServerDBCollections(logMetricsToServerCollections);
        }
        String logMetricsToServerDBName = getOption(Options.METRICS_DB_NAME);
        if (logMetricsToServerDBName != null) {
            options.setLogMetricsToServerDBName(logMetricsToServerDBName);
        }
        String logMetricsToServerModule = getOption(Options.METRICS_PROCESS_MODULE);
        if (logMetricsToServerModule != null) {
            options.setLogMetricsToServerDBTransformModule(logMetricsToServerModule);
        }
        String logMetricsToServerURIRoot = getOption(Options.METRICS_DOC_BASE_DIR);
        if (logMetricsToServerURIRoot != null) {
            options.setLogMetricsToServerDBURIRoot(logMetricsToServerURIRoot);
        }
        String jobName = getOption(Options.JOB_NAME);
        if (jobName != null) {
            options.setJobName(jobName);
        }
        String numberOfLongRunningUris = getOption(Options.METRICS_NUM_SLOW_TRANSACTIONS);
        if (numberOfLongRunningUris != null) {
            try {
                int intNumberOfLongRunningUris = Integer.parseInt(numberOfLongRunningUris);
                if (intNumberOfLongRunningUris > TransformOptions.MAX_NUM_SLOW_TRANSACTIONS) {
                    intNumberOfLongRunningUris = TransformOptions.MAX_NUM_SLOW_TRANSACTIONS;
                }
                options.setNumberOfLongRunningUris(intNumberOfLongRunningUris);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Options.METRICS_NUM_SLOW_TRANSACTIONS + " = " + numberOfLongRunningUris + " is invalid. please use a valid integer.");
            }
        }
        String numberOfFailedUris = getOption(Options.METRICS_NUM_FAILED_TRANSACTIONS);
        if (numberOfFailedUris != null) {
            int intNumFaileTransactions = Integer.parseInt(numberOfFailedUris);
            if (intNumFaileTransactions > TransformOptions.MAX_NUM_FAILED_TRANSACTIONS) {
                intNumFaileTransactions = TransformOptions.MAX_NUM_FAILED_TRANSACTIONS;
            }
            options.setNumberOfFailedUris(intNumFaileTransactions);
        }
        String metricsSyncFrequencyInSeconds = getOption(Options.METRICS_SYNC_FREQUENCY);
        if ((logMetricsToServerDBName != null || options.isMetricsToServerLogEnabled(logMetricsToServerLog)) && metricsSyncFrequencyInSeconds != null) {
            //periodically update db only if db name is set or logging enabled and sync frequency is selected
            //no defaults for this function
            try {
                int intMetricsSyncFrequencyInSeconds = Integer.parseInt(metricsSyncFrequencyInSeconds);
                options.setMetricsSyncFrequencyInMillis(intMetricsSyncFrequencyInSeconds * 1000);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Options.METRICS_SYNC_FREQUENCY + " = " + metricsSyncFrequencyInSeconds + " is invalid. please use a valid integer.");
            }
        }
        String jobServerPort = getOption(Options.JOB_SERVER_PORT);
        if (jobServerPort != null) {
            //no defaults for this function
            try {
                if (jobServerPort.indexOf('-') > -1 || jobServerPort.indexOf(',') > -1) {
                    List<Integer> jobServerPorts = StringUtils.parsePortRanges(jobServerPort);
                    if (!jobServerPorts.isEmpty()) {
                        options.setJobServerPortsToChoose(jobServerPorts);
                    }

                } else {
                    int intJobServerPort = Integer.parseInt(jobServerPort);
                    options.setJobServerPort(intJobServerPort);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Options.JOB_SERVER_PORT + " must be a valid port(s) or a valid range of ports. Ex: 9080 Ex: 9080,9083,9087 Ex: 9080-9090 Ex: 9080-9083,9085-9090");
            }
        }
        // delete the export file if it exists
        deleteFileIfExists(exportFileDir, exportFileName);
        deleteFileIfExists(exportFileDir, errorFileName);

        normalizeLegacyProperties();
    }

    protected void normalizeLegacyProperties() {
        //fix map keys for backward compatibility
        if (properties != null) {
            properties.putAll(getNormalizedProperties(properties));
        }
        //System properties override properties file properties
        Properties props = getNormalizedProperties(System.getProperties());
        for (final String name : props.stringPropertyNames()) {
            System.setProperty(name, props.getProperty(name));
        }
    }

    private Properties getNormalizedProperties(Properties properties) {
        Properties normalizedProperties = new Properties();
        if (properties == null) {
            return normalizedProperties;
        }

        //key=Current Property, value=Legacy Property
        Map<String, String> legacyProperties = new HashMap<>(3);
        legacyProperties.put(PROCESS_MODULE, XQUERY_MODULE);
        legacyProperties.put(PRE_BATCH_MODULE, PRE_BATCH_XQUERY_MODULE);
        legacyProperties.put(POST_BATCH_MODULE, POST_BATCH_XQUERY_MODULE);

        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            for (Map.Entry<String, String> entry : legacyProperties.entrySet()) {
                String legacyKey = entry.getValue();
                String legacyKeyPrefix = legacyKey + '.';
                String normalizedKey = entry.getKey();
                String normalizedKeyPrefix = normalizedKey + '.';
                String normalizedCustomInputKey = key.replace(legacyKeyPrefix, normalizedKeyPrefix);

                //First check for an exact match of the keys
                if (!properties.containsKey(normalizedKey) && key.equals(legacyKey)) {
                    normalizedProperties.setProperty(normalizedKey, value);
                    //Then look for custom inputs with the base property as a prefix
                } else if (!properties.containsKey(normalizedCustomInputKey)
                        && key.startsWith(legacyKeyPrefix) && value != null) {
                    normalizedProperties.setProperty(normalizedCustomInputKey, value);
                }
            }
        }

        return normalizedProperties;
    }

    protected Class<? extends Task> getTaskCls(String type, String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> cls = Class.forName(className);
        if (Task.class.isAssignableFrom(cls)) {
            cls.newInstance(); // sanity check
            return cls.asSubclass(Task.class);
        } else {
            throw new IllegalArgumentException(type + " must be of type com.marklogic.developer.corb.Task");
        }
    }

    protected Class<? extends UrisLoader> getUrisLoaderCls(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> cls = Class.forName(className);
        if (UrisLoader.class.isAssignableFrom(cls)) {
            cls.newInstance(); // sanity check
            return cls.asSubclass(UrisLoader.class);
        } else {
            throw new IllegalArgumentException("Uris Loader must be of type com.marklogic.developer.corb.UrisLoader");
        }
    }

    @Override
    protected void usage() {
        super.usage();

        List<String> args = new ArrayList<>(7);
        String xccConnectionUri = "xcc://user:password@host:port/[ database ]";
        String threadCount = "10";
        String optionsFile = "myjob.properties";
        PrintStream err = System.err; // NOPMD

        err.println("usage 1:"); // NOPMD
        err.println(TAB + NAME + ' ' + xccConnectionUri + " input-selector module-name.xqy"
                + " [ thread-count [ uris-module [ module-root" + " [ modules-database [ install [ process-task"
                + " [ pre-batch-module [ pre-batch-task" + " [ post-batch-module  [ post-batch-task"
                + " [ export-file-dir [ export-file-name" + " [ uris-file ] ] ] ] ] ] ] ] ] ] ] ] ]"); // NOPMD

        err.println("\nusage 2:");
        args.add(buildSystemPropertyArg(XCC_CONNECTION_URI, xccConnectionUri));
        args.add(buildSystemPropertyArg(PROCESS_MODULE, "module-name.xqy"));
        args.add(buildSystemPropertyArg(THREAD_COUNT, threadCount));
        args.add(buildSystemPropertyArg(URIS_MODULE, "get-uris.xqy"));
        args.add(buildSystemPropertyArg(POST_BATCH_MODULE, "post-batch.xqy"));
        args.add(buildSystemPropertyArg("... ", null));
        args.add(NAME);
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD

        err.println("\nusage 3:"); // NOPMD
        args.clear();
        args.add(buildSystemPropertyArg(OPTIONS_FILE, optionsFile));
        args.add(NAME);
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD

        err.println("\nusage 4:"); // NOPMD
        args.clear();
        args.add(buildSystemPropertyArg(OPTIONS_FILE, optionsFile));
        args.add(buildSystemPropertyArg(THREAD_COUNT, threadCount));
        args.add(NAME);
        args.add(xccConnectionUri);
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD
    }

    public int run() throws Exception {
        startMillis = System.currentTimeMillis();
        if (jobStats == null) {
            jobStats = new JobStats(this);
        }
        jobStats.logJobStatsToServer(START_RUNNING_JOB_MESSAGE, false);
        startMetricsSyncJob();
        startJobServer();
        LOG.log(INFO, () -> MessageFormat.format("{0} starting: {1}", NAME, VERSION_MSG));
        long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        LOG.log(INFO, () -> MessageFormat.format("maximum heap size = {0} MiB", maxMemory));

        execError = false; //reset execution error flag for a new run
        monitorThread = preparePool();

        try {
            int count = populateQueue();

            while (monitorThread.isAlive()) {
                try {
                    monitorThread.join();
                } catch (InterruptedException e) {
                    // reset interrupt status and continue
                    Thread.interrupted();
                    LOG.log(SEVERE, "interrupted while waiting for monitor", e);
                }
            }
            if (shouldRunPostBatch(count)) {
                runPostBatchTask(); // post batch tasks
                cleanupJobStats();
                LOG.info("all done");
            }
            return count;
        } catch (Exception e) {
            LOG.log(SEVERE, e.getMessage());
            stop();
            throw e;
        } finally {
            if (null != jobStats) {
                cleanupJobStats();
            }
        }
    }

    private void startJobServer() throws IOException {
        int port = options.getJobServerPort();
        if ((port > 0 ||
            options.getJobServerPortsToChoose() != null &&
            !options.getJobServerPortsToChoose().isEmpty()) &&
            jobServer == null) {

            Set<Integer> portCandidates = new TreeSet<>();
            if (port > 0) {
                portCandidates.add(port);
            }
            portCandidates.addAll(options.getJobServerPortsToChoose());

            jobServer = JobServer.create(portCandidates, this);
            jobServer.start();
            options.setJobServerPort(jobServer.getAddress().getPort());
        }
    }

    private void stopJobServer() {
        if (jobServer != null) {
            jobServer.stop(0);
            jobServer = null;
        }
    }

    private void startMetricsSyncJob() {
        if (metricsDocSyncJob == null && options.getMetricsSyncFrequencyInMillis() != null && options.getMetricsSyncFrequencyInMillis() > 0) {
            metricsDocSyncJob = new MetricsDocSyncJob(jobStats, options.getMetricsSyncFrequencyInMillis());
            Thread metricSyncThread = new Thread(metricsDocSyncJob, "metricsDocSyncJob");
            metricSyncThread.setDaemon(true);
            metricSyncThread.start();
        }
    }

    private void shutDownMetricsSyncJob() {
        if (metricsDocSyncJob != null) {
            metricsDocSyncJob.shutdownNow();
            metricsDocSyncJob = null;
        }
    }

    private void pauseMetricsSyncJob() {
        if (metricsDocSyncJob != null) {
            metricsDocSyncJob.setPaused(true);//this will also log full metrics
        }
    }

    private void resumeMetricsSyncJob() {
        if (metricsDocSyncJob != null) {
            metricsDocSyncJob.setPaused(false);
        }
    }

    protected boolean shouldRunPostBatch(int count) {
        return !execError && options.shouldPrePostBatchAlwaysExecute() || count >= options.getPostBatchMinimumCount();
    }

    protected boolean shouldRunPreBatch(int count) {
        return options.shouldPrePostBatchAlwaysExecute() || count >= options.getPreBatchMinimumCount();
    }

    /**
     * @return
     */
    private Thread preparePool() {
        RejectedExecutionHandler policy = new CallerBlocksPolicy();
        int threads = options.getThreadCount();
        // an array queue should be somewhat lighter-weight
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(options.getQueueSize());
        pool = new PausableThreadPoolExecutor(threads, threads, 16, TimeUnit.SECONDS, workQueue, policy, options);
        pool.prestartAllCoreThreads();
        completionService = new ExecutorCompletionService<>(pool);
        monitor = new Monitor(pool, completionService, this);
        return new Thread(monitor, "monitor");
    }

    /**
     * @throws CorbException
     *
     */
    private void prepareModules() throws CorbException {
        String[] resourceModules = new String[]{options.getInitModule(), options.getUrisModule(),
            options.getProcessModule(), options.getPreBatchModule(), options.getPostBatchModule()};
        String modulesDatabase = options.getModulesDatabase();
        LOG.log(INFO, () -> MessageFormat.format("checking modules, database: {0}", modulesDatabase));

        try (Session session = contentSource.newSession(modulesDatabase)) {
            for (String resourceModule : resourceModules) {
                insertModule(session, resourceModule);
            }
        }
    }

    protected void insertModule(Session session, String resourceModule) throws CorbException {
        if (resourceModule == null || isInlineOrAdhoc(resourceModule)) {
            return;
        }
        try {
            // Start by checking install flag.
            if (!options.isDoInstall()) {
                LOG.log(INFO, () -> MessageFormat.format("Skipping module installation: {0}", resourceModule));
            } // Next check: if XCC is configured for the filesystem, warn user
            else if (options.getModulesDatabase().isEmpty()) {
                LOG.warning("XCC configured for the filesystem: please install modules manually");
            } // Finally, if it's configured for a database, install.
            else {
                ContentCreateOptions contentCreateOptions = ContentCreateOptions.newTextInstance();
                File file = new File(resourceModule);
                Content content;
                // If not installed, are the specified files on the filesystem?
                if (file.exists()) {
                    String moduleUri = options.getModuleRoot() + file.getName();
                    content = ContentFactory.newContent(moduleUri, file, contentCreateOptions);
                } // finally, check package
                else {
                    LOG.log(WARNING, () -> MessageFormat.format("looking for {0} as resource", resourceModule));
                    String moduleUri = options.getModuleRoot() + resourceModule;
                    try (InputStream is = this.getClass().getResourceAsStream('/' + resourceModule)) {
                        if (null == is) {
                            throw new NullPointerException(resourceModule + " could not be found on the filesystem," + " or in package resources");
                        }
                        content = ContentFactory.newContent(moduleUri, is, contentCreateOptions);
                    }
                }
                session.insertContent(content);
            }
        } catch (IOException | RequestException e) {
            throw new CorbException(MessageFormat.format("error while reading module {0}", resourceModule), e);
        }
    }

    @Override
    protected void logOptions() {
        LOG.log(INFO, () -> MessageFormat.format("Configured modules db: {0}", options.getModulesDatabase()));
        LOG.log(INFO, () -> MessageFormat.format("Configured modules xdbc root: {0}", options.getXDBC_ROOT()));
        LOG.log(INFO, () -> MessageFormat.format("Configured modules root: {0}", options.getModuleRoot()));
        LOG.log(INFO, () -> MessageFormat.format("Configured uri module: {0}", options.getUrisModule()));
        LOG.log(INFO, () -> MessageFormat.format("Configured uri file: {0}", options.getUrisFile()));
        LOG.log(INFO, () -> MessageFormat.format("Configured uri loader: {0}", options.getUrisLoaderClass()));
        LOG.log(INFO, () -> MessageFormat.format("Configured process module: {0}", options.getProcessModule()));
        LOG.log(INFO, () -> MessageFormat.format("Configured process task: {0}", options.getProcessTaskClass()));
        LOG.log(INFO, () -> MessageFormat.format("Configured pre batch module: {0}", options.getPreBatchModule()));
        LOG.log(INFO, () -> MessageFormat.format("Configured pre batch task: {0}", options.getPreBatchTaskClass()));
        LOG.log(INFO, () -> MessageFormat.format("Configured post batch module: {0}", options.getPostBatchModule()));
        LOG.log(INFO, () -> MessageFormat.format("Configured post batch task: {0}", options.getPostBatchTaskClass()));
        LOG.log(INFO, () -> MessageFormat.format("Configured init module: {0}", options.getInitModule()));
        LOG.log(INFO, () -> MessageFormat.format("Configured init task: {0}", options.getInitTaskClass()));
        LOG.log(INFO, () -> MessageFormat.format("Configured thread count: {0}", options.getThreadCount()));
        LOG.log(INFO, () -> MessageFormat.format("Configured batch size: {0}", options.getBatchSize()));
        LOG.log(INFO, () -> MessageFormat.format("Configured failonError: {0}", options.isFailOnError()));
        LOG.log(INFO, () -> MessageFormat.format("Configured URIs queue max in-memory size: {0}", options.getDiskQueueMaxInMemorySize()));
        LOG.log(INFO, () -> MessageFormat.format("Configured URIs queue temp dir: {0}", options.getDiskQueueTempDir()));
    }

    private void runInitTask(TaskFactory tf) throws Exception {
        Task initTask = tf.newInitTask();
        if (initTask != null) {
            Long startTime = System.nanoTime();
            LOG.info("Running init Task");
            initTask.call();
            long endTime = System.nanoTime();
            jobStats.setInitTaskRunTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
        }
    }

    private void runPreBatchTask(TaskFactory tf) throws Exception {
        Task preTask = tf.newPreBatchTask();
        if (preTask != null) {
            long startTime = System.nanoTime();
            LOG.info("Running pre batch Task");
            preTask.call();
            long endTime = System.nanoTime();
            jobStats.setPreBatchRunTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
        }
    }

    private void runPostBatchTask() throws Exception {
        TaskFactory tf = new TaskFactory(this);
        Task postTask = tf.newPostBatchTask();
        if (postTask != null) {
            long startTime = System.nanoTime();
            LOG.info("Running post batch Task");
            postTask.call();
            long endTime = System.nanoTime();
            jobStats.setPostBatchRunTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
        }
    }

    private UrisLoader getUriLoader() throws InstantiationException, IllegalAccessException {
        UrisLoader loader;
        if (isNotBlank(options.getUrisModule())) {
            loader = new QueryUrisLoader();
        } else if (isNotBlank(options.getUrisFile())) {
            loader = new FileUrisLoader();
        } else if (options.getUrisLoaderClass() != null) {
            loader = options.getUrisLoaderClass().newInstance();
        } else {
            throw new IllegalArgumentException("Cannot find " + URIS_MODULE + ", " + URIS_FILE + " or " + URIS_LOADER);
        }

        loader.setOptions(options);
        loader.setContentSource(contentSource);
        loader.setCollection(collection);
        loader.setProperties(properties);
        return loader;
    }

    private int populateQueue() throws Exception {
        LOG.info("populating queue");
        TaskFactory taskFactory = new TaskFactory(this);
        Long startTime;
        int expectedTotalCount = -1;
        int urisCount = 0;
        try (UrisLoader urisLoader = getUriLoader()) {
            // run init task
            runInitTask(taskFactory);
            startTime = System.nanoTime();
            urisLoader.open();
            if (urisLoader.getBatchRef() != null) {
                properties.put(URIS_BATCH_REF, urisLoader.getBatchRef());
                LOG.log(INFO, () -> MessageFormat.format("{0}: {1}", URIS_BATCH_REF, urisLoader.getBatchRef()));
            }

            expectedTotalCount = urisLoader.getTotalCount();
            Long endTime = System.nanoTime();
            jobStats.setUrisLoadTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
            LOG.log(INFO, MessageFormat.format("expecting total {0,number}", expectedTotalCount));

            if (shouldRunPreBatch(expectedTotalCount)) {
                // run pre-batch task, if present.
                runPreBatchTask(taskFactory);
            }

            if (expectedTotalCount <= 0) {
                LOG.info("nothing to process");
                stop();
                return 0;
            }

            // now start process tasks
            monitor.setTaskCount(expectedTotalCount);
            monitorThread.start();

            urisCount = submitUriTasks(urisLoader, taskFactory, expectedTotalCount);

            if (urisCount == expectedTotalCount) {
                LOG.log(INFO, MessageFormat.format("queue is populated with {0,number} tasks", urisCount));
            } else {
                LOG.log(WARNING, MessageFormat.format("queue is expected to be populated with {0,number} tasks, but got {1,number} tasks.", expectedTotalCount, urisCount));
                monitor.setTaskCount(urisCount);
            }

            pool.shutdown();

        } catch (Exception exc) {
            stop();
            throw exc;
        }

        return urisCount;
    }

    /**
     * Submit batches of the URIs to be processed. Filter out blank entries and
     * return the total number of URIs.
     *
     * @param urisLoader
     * @param taskFactory
     * @param expectedTotalCount
     * @return
     * @throws CorbException
     */
    protected int submitUriTasks(UrisLoader urisLoader, TaskFactory taskFactory, int expectedTotalCount) throws CorbException {
        int urisCount = 0;
        long lastMessageMillis = System.currentTimeMillis();
        final long totalMemory = Runtime.getRuntime().totalMemory();
        String uri;
        List<String> uriBatch = new ArrayList<>(options.getBatchSize());

        while (urisLoader.hasNext()) {
            // check pool occasionally, for fast-fail
            if (null == pool) {
                break;
            }

            uri = urisLoader.next();
            if (isBlank(uri)) {
                continue;
            }
            uriBatch.add(uri);

            if (uriBatch.size() >= options.getBatchSize() || urisCount >= expectedTotalCount || !urisLoader.hasNext()) {
                String[] uris = uriBatch.toArray(new String[uriBatch.size()]);
                uriBatch.clear();
                completionService.submit(taskFactory.newProcessTask(uris, options.isFailOnError()));
            }

            urisCount++;

            if (0 == urisCount % 25000) {
                LOG.log(INFO, MessageFormat.format("received {0,number}/{1,number}: {2}", urisCount, expectedTotalCount, uri));
                logIfSlowReceive(lastMessageMillis, totalMemory);
                lastMessageMillis = System.currentTimeMillis();
            }
        }
        return urisCount;
    }

    protected void logIfSlowReceive(long lastMessageMillis, long totalMemory) {
        if (System.currentTimeMillis() - lastMessageMillis > (1000 * 4)) {
            LOG.warning("Slow receive! Consider increasing max heap size and using -XX:+UseConcMarkSweepGC");
            long freeMemory = Runtime.getRuntime().freeMemory();
            Level memoryLogLevel = freeMemory < totalMemory * 0.2d ? WARNING : INFO;
            final int megabytes = 1024 * 1024;
            LOG.log(memoryLogLevel, () -> MessageFormat.format("free memory: {0,number} MiB of {1,number}", freeMemory / megabytes, totalMemory / megabytes));
        }
    }

    public void setThreadCount(int threadCount) {
        if (threadCount > 0) {
            if (threadCount != options.getThreadCount()) {
                options.setThreadCount(threadCount);
                setPoolSize(pool, threadCount);
            }
        } else {
            LOG.log(WARNING, () -> THREAD_COUNT + " must be a positive integer value");
        }
    }

    protected void setPoolSize(ThreadPoolExecutor threadPool, int threadCount) {
        if (threadPool != null) {
            int currentMaxPoolSize = threadPool.getMaximumPoolSize();
            try {
                if (threadCount < currentMaxPoolSize) {
                    //shrink the core first then max
                    threadPool.setCorePoolSize(threadCount);
                    threadPool.setMaximumPoolSize(threadCount);
                } else {
                    //grow max first, then core
                    threadPool.setMaximumPoolSize(threadCount);
                    threadPool.setCorePoolSize(threadCount);
                }
                LOG.log(INFO, () -> MessageFormat.format("Changed {0} to {1}", THREAD_COUNT, threadCount));
            } catch (IllegalArgumentException ex) {
                LOG.log(WARNING, "Unable to change thread count", ex);
            }
        }
    }

    /**
     * Pause execution of pool tasks
     */
    public void pause() {
        if (pool != null && pool.isRunning()) {
            LOG.info("pausing");
            pool.pause();
            pauseMetricsSyncJob();
        }
    }

    public boolean isPaused() {
        return pool != null && pool.isPaused();
    }

    /**
     * Resume pool execution (if paused).
     */
    public void resume() {
        if (isPaused()) {
            LOG.info("resuming");
            pool.resume();
            resumeMetricsSyncJob();
        }
    }

    /**
     * Stop the thread pool
     */
    public void stop() {
        LOG.info("cleaning up");
        if (null != pool) {
            if (pool.isPaused()) {
                pool.resume();
            }
            List<Runnable> remaining = pool.shutdownNow();
            if (!remaining.isEmpty()) {
                LOG.log(WARNING, () -> MessageFormat.format("thread pool was shut down with {0,number} pending tasks", remaining.size()));
            }
            pool = null;
        }
        if (null != monitor) {
            monitor.shutdownNow();
        }
        if (null != monitorThread) {
            monitorThread.interrupt();
        }
    }

    private void cleanupJobStats() {
        endMillis = System.currentTimeMillis();
        shutDownMetricsSyncJob();
        jobStats.logJobStatsToServer(END_RUNNING_JOB_MESSAGE, false);
        stopJobServer();
    }

    /**
     * Log a fatal error for the provided exception and then stop the thread
     * pool
     *
     * @param e
     */
    public void stop(ExecutionException e) {
        execError = true;
        LOG.log(SEVERE, "fatal error", e.getCause());
        LOG.warning("exiting due to fatal error");
        stop();
    }

    /**
     * @return the startMillis
     */
    public long getStartMillis() {
        return startMillis;
    }

    /**
     * @return the transformStartMillis
     */
    public long getTransformStartMillis() {
        return transformStartMillis;
    }

    /**
     * @return the endMillis
     */
    public long getEndMillis() {
        return endMillis;
    }

    public static class CommandFileWatcher implements Runnable {

        private long timeStamp;
        private final File file;
        private final Manager manager;

        public CommandFileWatcher(File file, Manager manager) {
            this.file = file;
            timeStamp = -1;
            this.manager = manager;
        }

        @Override
        public void run() {
            if (file.exists()) {
                long lastModified = file.lastModified();
                if (timeStamp != lastModified) {
                    timeStamp = lastModified;
                    onChange(file);
                }
            }
        }

        public void onChange(File file) {

            try (InputStream in = new FileInputStream(file)) {

                Properties commandFile = new Properties();
                commandFile.load(in);

                String command = commandFile.getProperty(Options.COMMAND);
                if ("PAUSE".equalsIgnoreCase(command)) {
                    manager.pause();
                } else if ("STOP".equalsIgnoreCase(command)) {
                    manager.stopCommand = true;
                    manager.stop();
                } else {
                    manager.resume();
                }

                if (commandFile.containsKey(THREAD_COUNT)) {
                    int threadCount = NumberUtils.toInt(commandFile.getProperty(THREAD_COUNT));
                    if (threadCount > 0) {
                        manager.setThreadCount(threadCount);
                    }
                }

            } catch (IOException e) {
                LOG.log(WARNING, "Unable to load " + COMMAND_FILE, e);
            }
        }
    }

    public static class CallerBlocksPolicy implements RejectedExecutionHandler {

        private BlockingQueue<Runnable> queue;

        private boolean warning;

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (null == queue) {
                queue = executor.getQueue();
            }
            try {
                // block until space becomes available
                if (!warning) {
                    LOG.log(INFO, () -> MessageFormat.format("queue is full: size = {0} (will only appear once)", queue.size()));
                    warning = true;
                }
                queue.put(r);
            } catch (InterruptedException e) {
                // reset interrupt status and exit
                Thread.interrupted();
                // someone is trying to interrupt us
                throw new RejectedExecutionException(e);
            }
        }
    }

}
