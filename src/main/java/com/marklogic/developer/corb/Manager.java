/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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
import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.NumberUtils;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * @author Colleen Whitney, MarkLogic Corporation
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class Manager extends AbstractManager implements Closeable {

    protected static final String NAME = Manager.class.getName();

    public static final String URIS_BATCH_REF = com.marklogic.developer.corb.Options.URIS_BATCH_REF;
    public static final String DEFAULT_BATCH_URI_DELIM = ";";

    protected PausableThreadPoolExecutor pool;
    protected Monitor monitor;
    protected JobServer jobServer = null;
    protected String jobId = null;
    protected JobStats jobStats = null;
    protected long startMillis;
    protected long transformStartMillis;
    protected long endMillis;
    protected long urisCount;
    protected boolean execError;
    protected Integer exitCode;

    protected boolean stopCommand;

    protected Thread monitorThread;
    protected CompletionService<String[]> completionService;
    protected ScheduledExecutorService scheduledExecutor;

    protected int EXIT_CODE_NO_URIS = EXIT_CODE_SUCCESS;
    protected int EXIT_CODE_IGNORED_ERRORS = EXIT_CODE_SUCCESS;
    protected static final int EXIT_CODE_STOP_COMMAND = 3;

    private static final Logger LOG = Logger.getLogger(Manager.class.getName());
    private static final String TAB = "\t";

    private static final String RUNNING_JOB_MESSAGE = "RUNNING CORB JOB:";
    private static final String START_RUNNING_JOB_MESSAGE = "STARTED " + RUNNING_JOB_MESSAGE;
    private static final String PAUSING_JOB_MESSAGE = "PAUSING CORB JOB:";
    private static final String RESUMING_JOB_MESSAGE = "RESUMING CORB JOB:";
    private static final String END_RUNNING_JOB_MESSAGE = "END " + RUNNING_JOB_MESSAGE;

    /**
     * @param args
     */
    public static void main(String... args) {
        try (Manager manager = new Manager()) {
            try {
                manager.init(args);
            } catch (Exception exc) {
                LOG.log(SEVERE, "Error initializing CoRB " + exc.getMessage(), exc);
                manager.usage();
                LOG.log(INFO, () -> "init error - exiting with code " + EXIT_CODE_INIT_ERROR);
                System.exit(EXIT_CODE_INIT_ERROR);
            }
            //now we can start CoRB.
            try {
                manager.run();
                manager.close();
            } catch (Exception exc) {
                LOG.log(SEVERE, "Error while running CoRB", exc);
                manager.setExitCode(EXIT_CODE_PROCESSING_ERROR);
            } finally {
                System.exit(manager.getExitCode());
            }
        }
    }

    protected int determineExitCode() {
        if (this.exitCode == null) {
            if (execError) {
                LOG.log(INFO, () -> "processing error - exiting with code " + EXIT_CODE_PROCESSING_ERROR);
                setExitCode(EXIT_CODE_PROCESSING_ERROR);
            } else if (stopCommand) {
                LOG.log(INFO, () -> "stop command - exiting with code " + EXIT_CODE_STOP_COMMAND);
                setExitCode(EXIT_CODE_STOP_COMMAND);
            } else if (urisCount == 0) {
                LOG.log(INFO, () -> "no uris found - exiting with code " + EXIT_CODE_NO_URIS);
                setExitCode(EXIT_CODE_NO_URIS);
            } else if (pool.getNumFailedUris() > 0) {
                LOG.log(INFO, () -> "completed with ignored errors - exiting with code " + EXIT_CODE_IGNORED_ERRORS);
                setExitCode(EXIT_CODE_IGNORED_ERRORS);
            } else {
                LOG.log(INFO, () -> "success - exiting with code " + EXIT_CODE_SUCCESS);
                setExitCode(EXIT_CODE_SUCCESS);
            }
        }
        return exitCode;
    }

    protected void setExitCode(int code) {
        exitCode = code;
    }

    protected int getExitCode() {
        return determineExitCode();
    }

    @Override
    public void close() {
        if (scheduledExecutor != null) {
            //This will shutdown the scheduled executors for the command file watcher and logging JobStats
            scheduledExecutor.shutdown();
        }
        IOUtils.closeQuietly(csp);
        stopJobServer();
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
        EXIT_CODE_IGNORED_ERRORS = NumberUtils.toInt(getOption(Options.EXIT_CODE_IGNORED_ERRORS));
        scheduledExecutor = Executors.newScheduledThreadPool(2);
    }

    protected void scheduleCommandFileWatcher() {
        String commandFile = getOption(COMMAND_FILE);
        if (isNotBlank(commandFile)) {
            Runnable commandFileWatcher = new CommandFileWatcher(FileUtils.getFile(commandFile), this);
            int pollInterval = NumberUtils.toInt(getOption(Options.COMMAND_FILE_POLL_INTERVAL), 1);
            //execute immediately, in order to read command file before the job starts
            scheduledExecutor.execute(commandFileWatcher);
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

        options.setShouldRedactUris(stringToBoolean(getOption(Options.URIS_REDACTED)));
        options.setPrePostBatchAlwaysExecute(stringToBoolean(getOption(PRE_POST_BATCH_ALWAYS_EXECUTE)));

        String postBatchMinimumCount = getOption(POST_BATCH_MINIMUM_COUNT);
        if (isNotEmpty(postBatchMinimumCount)) {
            options.setPostBatchMinimumCount(Integer.parseInt(postBatchMinimumCount));
        }

        String preBatchMinimumCount = getOption(PRE_BATCH_MINIMUM_COUNT);
        if (isNotEmpty(preBatchMinimumCount)) {
            options.setPreBatchMinimumCount(Integer.parseInt(preBatchMinimumCount));
        }

        if (!properties.containsKey(EXPORT_FILE_DIR) && exportFileDir != null) {
            properties.put(EXPORT_FILE_DIR, exportFileDir);
        }
        if (!properties.containsKey(EXPORT_FILE_NAME) && exportFileName != null) {
            properties.put(EXPORT_FILE_NAME, exportFileName);
        }
        if (properties.containsKey(EXPORT_FILE_NAME) && processTask == null) {
            LOG.info("configuring ExportBatchToFileTask since EXPORT-FILE-NAME was set and without PROCESS-TASK");
            processTask = ExportBatchToFileTask.class.getName();
            properties.put(PROCESS_TASK, processTask);
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
            throw new CorbException(PROCESS_TASK + " or " + PROCESS_MODULE + " must be specified");
        }

        if (options.getPostBatchTaskClass() == null) {
            properties.remove(EXPORT_FILE_PART_EXT);
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

        String metricsLogLevel = getOption(Options.METRICS_LOG_LEVEL);
        if (metricsLogLevel != null) {
            if (metricsLogLevel.toLowerCase().matches(Options.ML_LOG_LEVELS)) {
                options.setLogMetricsToServerLog(metricsLogLevel.toLowerCase());
            } else {
                throw new IllegalArgumentException("INVALID VALUE for METRICS-TO-ERROR-LOG: " + metricsLogLevel + ". Supported LOG LEVELS are one of: " + Options.ML_LOG_LEVELS);
            }
        }
        String metricsCollections = getOption(Options.METRICS_COLLECTIONS);
        if (metricsCollections != null) {
            options.setMetricsCollections(metricsCollections);
        }
        String metricsDatabase = getOption(Options.METRICS_DATABASE);
        if (metricsDatabase != null) {
            options.setMetricsDatabase(metricsDatabase);
        }
        String metricsModule = getOption(Options.METRICS_MODULE);
        if (metricsModule != null) {
            options.setMetricsModule(metricsModule);
        }
        String metricsRoot = getOption(Options.METRICS_ROOT);
        if (metricsRoot != null) {
            options.setMetricsRoot(metricsRoot);
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
                throw new IllegalArgumentException(Options.METRICS_NUM_SLOW_TRANSACTIONS + " = " + numberOfLongRunningUris + " is invalid. Value must be a valid integer.");
            }
        }
        String numberOfFailedUris = getOption(Options.METRICS_NUM_FAILED_TRANSACTIONS);
        if (numberOfFailedUris != null) {
            int intNumFailedTransactions = Integer.parseInt(numberOfFailedUris);
            if (intNumFailedTransactions > TransformOptions.MAX_NUM_FAILED_TRANSACTIONS) {
                intNumFailedTransactions = TransformOptions.MAX_NUM_FAILED_TRANSACTIONS;
            }
            options.setNumberOfFailedUris(intNumFailedTransactions);
        }
        String metricsSyncFrequencyInSeconds = getOption(Options.METRICS_SYNC_FREQUENCY);
        if ((metricsDatabase != null || options.isMetricsLoggingEnabled(metricsLogLevel)) && metricsSyncFrequencyInSeconds != null) {
            //periodically update db only if db name is set or logging enabled and sync frequency is selected
            //no defaults for this function
            try {
                int intMetricsSyncFrequencyInSeconds = Integer.parseInt(metricsSyncFrequencyInSeconds);
                options.setMetricsSyncFrequencyInMillis(intMetricsSyncFrequencyInSeconds * 1000);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(Options.METRICS_SYNC_FREQUENCY + " = " + metricsSyncFrequencyInSeconds + " is invalid. Value must be a valid integer.");
            }
        }

        String jobServerPort = getOption(Options.JOB_SERVER_PORT);
        //no defaults for this function
        try {
            Set<Integer> jobServerPorts = new LinkedHashSet<>(StringUtils.parsePortRanges(jobServerPort));
            options.setJobServerPortsToChoose(jobServerPorts);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(Options.JOB_SERVER_PORT + " must be a valid port(s) or a valid range of ports. Ex: 9080 Ex: 9080,9083,9087 Ex: 9080-9090 Ex: 9080-9083,9085-9090");
        }

        // delete the export file if it exists
        FileUtils.deleteFileQuietly(exportFileDir, exportFileName);
        FileUtils.deleteFileQuietly(exportFileDir, errorFileName);

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
            throw new IllegalArgumentException(type + " must be of type " + Task.class.getName());
        }
    }

    protected Class<? extends UrisLoader> getUrisLoaderCls(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> cls = Class.forName(className);
        if (UrisLoader.class.isAssignableFrom(cls)) {
            cls.newInstance(); // sanity check
            return cls.asSubclass(UrisLoader.class);
        } else {
            throw new IllegalArgumentException("Uris Loader must be of type " + UrisLoader.class.getName());
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

    public long run() throws Exception {
        if (jobId == null) {
            jobId = UUID.randomUUID().toString();
        }
        scheduleCommandFileWatcher();
        startJobServer();
        jobStats = new JobStats(this);
        scheduleJobMetrics();

        startMillis = System.currentTimeMillis();
        jobStats.logMetrics(START_RUNNING_JOB_MESSAGE, false, false);
        LOG.log(INFO, () -> MessageFormat.format("{0} starting: {1}", NAME, VERSION_MSG));
        long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        LOG.log(INFO, () -> MessageFormat.format("maximum heap size = {0} MiB", maxMemory));

        execError = false; //reset execution error flag for a new run
        exitCode = null;
        monitorThread = preparePool();

        try {
            urisCount = populateQueue();
            while (monitorThread.isAlive()) {
                try {
                    monitorThread.join();
                } catch (InterruptedException e) {
                    // reset interrupt status and continue
                    Thread.interrupted();
                    LOG.log(SEVERE, "interrupted while waiting for monitor", e);
                }
            }
            if (shouldRunPostBatch(urisCount)) {
                TaskFactory taskFactory = new TaskFactory(this);
                runPostBatchTask(taskFactory);
            }
            endMillis = System.currentTimeMillis();
            jobStats.logMetrics(END_RUNNING_JOB_MESSAGE, false, true);
            LOG.info("all done");
            return urisCount;
        } catch (Exception e) {
            LOG.log(SEVERE, e.getMessage());
            stop();
            throw e;
        } finally {
            determineExitCode();
        }
    }

    private void startJobServer() throws IOException {
        if (!options.getJobServerPortsToChoose().isEmpty() && jobServer == null) {
            setJobServer(JobServer.create(options.getJobServerPortsToChoose(), this));
            jobServer.start();
        }
    }

    public JobServer getJobServer() {
        return jobServer;
    }

    protected void setJobServer(JobServer jobServer) {
        this.jobServer = jobServer;
        options.setJobServerPort(jobServer.getAddress().getPort());
        if (jobStats == null) {
            jobStats = new JobStats(this);
        }
    }

    private void stopJobServer() {
        if (jobServer != null) {
            // UI polls on interval of 2 seconds, so delay just a bit longer before shutting down
            jobServer.stop(4000);
            jobServer = null;
        }
    }

    protected void scheduleJobMetrics() {
        Integer interval = options.getMetricsSyncFrequencyInMillis();
        if (interval != null && interval > 0) {
            Runnable jobMetricsLogger = () -> {
                if (!isPaused()){
                    jobStats.logMetrics(RUNNING_JOB_MESSAGE, true, false);
                }
            };
            scheduledExecutor.scheduleWithFixedDelay(jobMetricsLogger, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    protected boolean shouldRunPostBatch(long count) {
        return !execError && (options.shouldPrePostBatchAlwaysExecute() || count >= options.getPostBatchMinimumCount());
    }

    protected boolean shouldRunPreBatch(long count) {
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

        ContentSource contentSource = csp.get();
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
            LOG.info("Running init Task");

            long startTime = System.nanoTime();
            initTask.call();
            long endTime = System.nanoTime();

            jobStats.setInitTaskRunTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
        }
    }

    private void runPreBatchTask(TaskFactory tf) throws Exception {
        Task preTask = tf.newPreBatchTask();
        if (preTask != null) {
            LOG.info("Running pre batch Task");

            long startTime = System.nanoTime();
            preTask.call();
            long endTime = System.nanoTime();

            jobStats.setPreBatchRunTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
        }
    }

    private void runPostBatchTask(TaskFactory tf) throws Exception {
        Task postTask = tf.newPostBatchTask();
        if (postTask != null) {
            LOG.info("Running post batch Task");

            long startTime = System.nanoTime();
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
        loader.setContentSourcePool(csp);
        loader.setCollection(collection);
        loader.setProperties(properties);
        return loader;
    }

    private void runUrisLoader(UrisLoader urisLoader) throws CorbException {
        long startTime = System.nanoTime();
        urisLoader.open();
        if (urisLoader.getBatchRef() != null) {
            properties.put(URIS_BATCH_REF, urisLoader.getBatchRef());
            LOG.log(INFO, () -> MessageFormat.format("{0}: {1}", URIS_BATCH_REF, urisLoader.getBatchRef()));
        }
        long endTime = System.nanoTime();

        jobStats.setUrisLoadTime(TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
    }

    private long populateQueue() throws Exception {
        long expectedTotalCount = -1;
        if (!this.stopCommand) {
            try (UrisLoader urisLoader = getUriLoader()) {
                LOG.info("populating queue");
                TaskFactory taskFactory = new TaskFactory(this);
                // run init task
                runInitTask(taskFactory);
                // Invoke URIs Module, read text file, etc.
                runUrisLoader(urisLoader);

                expectedTotalCount = urisLoader.getTotalCount();
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

                transformStartMillis = System.currentTimeMillis();
                urisCount = submitUriTasks(urisLoader, taskFactory, expectedTotalCount);

                if (urisCount == expectedTotalCount) {
                    LOG.log(INFO, MessageFormat.format("queue is populated with {0,number} tasks", urisCount));
                } else {
                    LOG.log(WARNING, MessageFormat.format("queue is expected to be populated with {0,number} tasks, but got {1,number} tasks.", expectedTotalCount, urisCount));
                    monitor.setTaskCount(urisCount);
                }

                if (pool != null) {
                    LOG.info("Invoking graceful shutdown of the thread pool and wait for remaining tasks in the queue to complete.");
                    pool.shutdown();
                } else {
                    LOG.warning("Thread pool is set null - closed already?");
                }
            } catch (Exception exc) {
                stop();
                throw exc;
            }
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
    protected long submitUriTasks(UrisLoader urisLoader, TaskFactory taskFactory, long expectedTotalCount) throws CorbException {
        urisCount = 0;
        String uri;
        List<String> uriBatch = new ArrayList<>(options.getBatchSize());
        boolean redactUris = options.shouldRedactUris();

        while (urisLoader.hasNext()) {
            // check pool occasionally, for fast-fail
            if (null == pool) {
                LOG.warning("Thread pool is set to null. Exiting out of the task submission loop prematurely.");
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

            if (0 == urisCount % 50000) {
                LOG.log(INFO, MessageFormat.format("received {0,number}/{1,number}{2}", urisCount, expectedTotalCount, redactUris ? "" : ": " + uri));
            }
            if (0 == urisCount % 25000) {
                long totalMemory = Runtime.getRuntime().totalMemory(); //according to java doc this value may vary over time
                logIfLowMemory(totalMemory);
            }
        }
        return urisCount;
    }

    protected void logIfLowMemory(long totalMemory) {
        long freeMemory = Runtime.getRuntime().freeMemory();
        if (freeMemory < totalMemory * 0.2d) { //less than 20% of total memory
            final int megabytes = 1024 * 1024;
            LOG.log(WARNING, () -> MessageFormat.format("free memory: {0,number} MiB of {1,number}", freeMemory / megabytes, totalMemory / megabytes));
            LOG.warning("Consider increasing max heap size and using -XX:+UseConcMarkSweepGC");
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
            jobStats.logMetrics(PAUSING_JOB_MESSAGE, false, true);
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
            jobStats.logMetrics(RESUMING_JOB_MESSAGE, true, false);
            pool.resume();
        }
    }

    /**
     * Stop the thread pool
     */
    public void stop() {
        LOG.info("cleaning up");
        if (null != pool) {
            LOG.info("Shutting down the thread pool");
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

    public String getJobId() {
        return jobId;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public JobStats getJobStats() {
        return jobStats;
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
