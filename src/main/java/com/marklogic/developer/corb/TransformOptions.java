/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Configuration container for CoRB job execution options.
 * <p>
 * TransformOptions holds all configuration settings for a CoRB job, including:
 * </p>
 * <ul>
 * <li>Module paths (URIS-MODULE, PROCESS-MODULE, PRE-BATCH-MODULE, POST-BATCH-MODULE, INIT-MODULE)</li>
 * <li>Task classes (custom Java implementations)</li>
 * <li>Execution parameters (thread count, batch size, fail-on-error)</li>
 * <li>URI loading configuration (file, loader class)</li>
 * <li>Queue settings (disk queue, memory limits)</li>
 * <li>Metrics collection (database, logging, sync frequency)</li>
 * <li>Job server settings (port, monitoring)</li>
 * <li>Export file configuration (directory)</li>
 * </ul>
 * <p>
 * This class is populated by {@link Manager} during initialization from:
 * </p>
 * <ul>
 * <li>Command-line arguments</li>
 * <li>System properties</li>
 * <li>Properties file (OPTIONS-FILE)</li>
 * </ul>

 * <p>
 * TransformOptions is used by:
 * </p>
 * <ul>
 * <li>{@link Manager} - Overall job coordination</li>
 * <li>{@link TaskFactory} - Task creation and configuration</li>
 * <li>{@link Monitor} - Progress tracking</li>
 * <li>{@link JobStats} - Metrics collection</li>
 * <li>{@link PausableThreadPoolExecutor} - Thread pool configuration</li>
 * </ul>

 *
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Colleen Whitney, colleen.whitney@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see Manager
 * @see TaskFactory
 * @see Options
 */
public class TransformOptions {

    /**
     * Default sleep time in milliseconds between progress checks.
     */
    public static final int SLEEP_TIME_MS = 500;

    /**
     * Default progress reporting interval in milliseconds (30 seconds).
     */
    public static final long PROGRESS_INTERVAL_MS = 60L * SLEEP_TIME_MS;

    /**
     * Fully qualified class name of TransformOptions.
     */
    public static final String NAME = TransformOptions.class.getName();

    /**
     * Forward slash path separator.
     */
    private static final String SLASH = "/";

    /**
     * Identifier for collection-based URI selection.
     */
    public static final String COLLECTION_TYPE = "COLLECTION";

    /**
     * Identifier for directory-based URI selection.
     */
    public static final String DIRECTORY_TYPE = "DIRECTORY";

    /**
     * Identifier for query-based URI selection.
     */
    public static final String QUERY_TYPE = "QUERY";

    /**
     * Token prefix for marking failed URIs.
     */
    public static final String FAILED_URI_TOKEN = "FAILED#";

    /**
     * Maximum number of failed transactions to track in metrics.
     */
    public static final int MAX_NUM_FAILED_TRANSACTIONS = 1000;

    /**
     * Maximum number of slow transactions to track in metrics.
     */
    public static final int MAX_NUM_SLOW_TRANSACTIONS = 100;


    /** Path to the PROCESS-MODULE for URI processing. */
    private String processModule;

    /** Custom Java Task class for URI processing. */
    private Class<? extends Task> processTaskCls;

    /** Path to the PRE-BATCH-MODULE executed before batch processing. */
    private String preBatchModule;

    /** Custom Java Task class for pre-batch operations. */
    private Class<? extends Task> preBatchTaskCls;

    /** Path to the POST-BATCH-MODULE executed after batch processing. */
    private String postBatchModule;

    /** Custom Java Task class for post-batch operations. */
    private Class<? extends Task> postBatchTaskCls;

    /** Path to the INIT-MODULE executed at job initialization. */
    private String initModule;

    /** Custom Java Task class for initialization operations. */
    private Class<? extends Task> initTaskCls;

    /** Directory path for exported files. */
    private String exportFileDir;

    /** Root path prefix for all modules. */
    private String moduleRoot = SLASH;

    /** Path to the URIS-MODULE for URI loading. */
    private String urisModule;

    /** Path to the URIS-FILE for loading URIs from disk. */
    private String urisFile;

    /** Custom UrisLoader class for URI loading. */
    private Class<? extends UrisLoader> urisLoaderCls;

    /** Number of worker threads for parallel processing. */
    private int threadCount = 1;

    /** Number of URIs to process in each batch. */
    private int batchSize = 1;

    /** Whether to use disk-backed queue for URI storage. */
    private boolean useDiskQueue;

    /** Maximum number of URIs to hold in memory before spilling to disk. */
    private int diskQueueMaxInMemorySize = 1000;

    /** Temporary directory for disk queue storage. */
    private File diskQueueTempDir;

    /** Whether restart state should be persisted for best-effort resume. */
    private boolean restartable;

    /** Directory used to store restart state files. */
    private File restartStateDir;

    /** Whether to install modules to the modules database. */
    private boolean doInstall;

    /** Number of recent TPS values to use for ETC calculation. */
    private int numTpsForETC = 10;

    /** Whether pre-batch and post-batch tasks should always execute regardless of URI count. */
    private boolean prePostBatchAlwaysExecute;

    /** Minimum URI count required for pre-batch task execution. */
    private int preBatchMinimumCount = 1;

    /** Minimum URI count required for post-batch task execution. */
    private int postBatchMinimumCount = 1;

    /** Whether the job should fail immediately on task errors. */
    private boolean failOnError = true;

    /** Whether URIs should be redacted from logs and metrics. */
    private boolean redactUris = false;

    /** Name of the modules database. */
    private String modulesDatabase = "Modules";

    /** Log level for metrics logging to server log. */
    private String logMetricsToServerLog = "NONE";

    /** Whether metrics should be logged to server database (deprecated, not currently used). */
    private Boolean logMetricsToServerDB = false;

    /** Name of the database for saving metrics documents. */
    private String metricsDatabase = null;

    /** Root directory for metrics documents. */
    private String metricsRoot = "/ServiceMetrics/";

    /** Module for producing and saving metrics. */
    private String metricsModule = "save-metrics.xqy|ADHOC";

    /** Collections to add to metrics documents when saving. */
    private String metricsCollections = null;

    /** Number of long-running URIs to track in metrics. */
    private Integer numberOfLongRunningUris = 0;

    /** Number of failed URIs to track in metrics. */
    private Integer numberOfFailedUris = 0;

    /** Frequency for syncing metrics to the database in milliseconds. */
    private Integer metricsSyncFrequencyInMillis = -1;

    /** Port number for the job server. */
    private Integer jobServerPort = -1;

    /** Set of port candidates for the job server. */
    private Set<Integer> jobServerPortsToChoose = new LinkedHashSet<>();

    /** Name of the job. */
    private String jobName = null;

    /** XDBC root path, set during status check. */
    private String xdbcRoot = SLASH;

    /**
     * Sets whether pre-batch and post-batch tasks should always execute,
     * regardless of URI count.
     *
     * @param shouldAlwaysExecute true to always execute, false to check minimum counts
     * @see #setPreBatchMinimumCount(int)
     * @see #setPostBatchMinimumCount(int)
     */
    public void setPrePostBatchAlwaysExecute(boolean shouldAlwaysExecute) {
        prePostBatchAlwaysExecute = shouldAlwaysExecute;
    }

    /**
     * Checks whether pre-batch and post-batch tasks should always execute.
     *
     * @return true if tasks should always execute, false otherwise
     */
    public boolean shouldPrePostBatchAlwaysExecute() {
        return prePostBatchAlwaysExecute;
    }

    /**
     * Sets the minimum URI count required for post-batch task execution.
     *
     * @param count the minimum count
     * @see #setPrePostBatchAlwaysExecute(boolean)
     */
    public void setPostBatchMinimumCount(int count) {
        postBatchMinimumCount = count;
    }

    /**
     * Gets the minimum URI count required for post-batch task execution.
     *
     * @return the minimum count
     */
    public int getPostBatchMinimumCount() {
        return postBatchMinimumCount;
    }

    /**
     * Sets the minimum URI count required for pre-batch task execution.
     *
     * @param count the minimum count
     * @see #setPrePostBatchAlwaysExecute(boolean)
     */
    public void setPreBatchMinimumCount(int count) {
        preBatchMinimumCount = count;
    }

    /**
     * Gets the minimum URI count required for pre-batch task execution.
     *
     * @return the minimum count
     */
    public int getPreBatchMinimumCount() {
        return preBatchMinimumCount;
    }

    /**
     * Gets the XDBC root path.
     *
     * @return the XDBC root path
     */
    public String getXDBC_ROOT() {
        return xdbcRoot;
    }

    /**
     * Sets the XDBC root path.
     *
     * @param xdbc_root the XDBC root path to set
     */
    public void setXDBC_ROOT(String xdbc_root) {
        this.xdbcRoot = xdbc_root;
    }

    /**
     * Gets the number of worker threads for parallel processing.
     *
     * @return the thread count
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Sets the number of worker threads for parallel processing.
     *
     * @param count the thread count to set
     */
    public void setThreadCount(int count) {
        this.threadCount = count;
    }

    /**
     * Gets the number of URIs to process in a single batch.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of URIs to process in a single batch.
     *
     * @param batchSize the batch size to set
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Gets the log level.
     * TODO: Make this configurable.
     *
     * @return the log level (currently always "INFO")
     */
    public String getLogLevel() {
        // TODO LogLevel make configurable
        return "INFO";
    }

    /**
     * Gets the log handler type.
     * TODO: Make this configurable.
     *
     * @return the log handler (currently always "CONSOLE")
     */
    public String getLogHandler() {
        // TODO LogHandler make configurable
        return "CONSOLE";
    }

    /**
     * Gets the name of the modules database.
     *
     * @return the modules database name
     * @see Options#MODULES_DATABASE
     */
    public String getModulesDatabase() {
        return this.modulesDatabase;
    }

    /**
     * Sets the name of the modules database.
     *
     * @param modulesDatabase the modules database name
     * @see Options#MODULES_DATABASE
     */
    public void setModulesDatabase(String modulesDatabase) {
        this.modulesDatabase = modulesDatabase;
    }

    /**
     * Gets the URIS-MODULE path for loading URIs.
     *
     * @return the URIS-MODULE path
     * @see Options#URIS_MODULE
     */
    public String getUrisModule() {
        return urisModule;
    }

    /**
     * Sets the URIS-MODULE path for loading URIs.
     *
     * @param urisModule the URIS-MODULE path
     * @see Options#URIS_MODULE
     */
    public void setUrisModule(String urisModule) {
        this.urisModule = urisModule;
    }

    /**
     * Gets the URIS-FILE path for loading URIs from a file.
     *
     * @return the URIS-FILE path
     * @see Options#URIS_FILE
     */
    public String getUrisFile() {
        return this.urisFile;
    }

    /**
     * Sets the URIS-FILE path for loading URIs from a file.
     *
     * @param urisFile the URIS-FILE path
     * @see Options#URIS_FILE
     */
    public void setUrisFile(String urisFile) {
        this.urisFile = urisFile;
    }

    /**
     * Gets the custom UrisLoader class.
     *
     * @return the UrisLoader class, or null if not configured
     * @see Options#URIS_LOADER
     */
    public Class<? extends UrisLoader> getUrisLoaderClass() {
        return this.urisLoaderCls;
    }

    /**
     * Sets the custom UrisLoader class.
     *
     * @param urisLoaderCls the UrisLoader class
     * @see Options#URIS_LOADER
     */
    public void setUrisLoaderClass(Class<? extends UrisLoader> urisLoaderCls) {
        this.urisLoaderCls = urisLoaderCls;
    }

    /**
     * Gets the PROCESS-MODULE path.
     *
     * @return the PROCESS-MODULE path
     * @see Options#PROCESS_MODULE
     */
    public String getProcessModule() {
        return processModule;
    }

    /**
     * Sets the PROCESS-MODULE path.
     *
     * @param processModule the PROCESS-MODULE path
     * @see Options#PROCESS_MODULE
     */
    public void setProcessModule(String processModule) {
        this.processModule = processModule;
    }

    /**
     * Sets the custom PROCESS-TASK Java class.
     *
     * @param processTaskCls the Task class for process operations
     * @see Options#PROCESS_TASK
     */
    public void setProcessTaskClass(Class<? extends Task> processTaskCls) {
        this.processTaskCls = processTaskCls;
    }

    /**
     * Gets the custom PROCESS-TASK Java class.
     *
     * @return the Task class for process operations, or null if not configured
     * @see Options#PROCESS_TASK
     */
    public Class<? extends Task> getProcessTaskClass() {
        return this.processTaskCls;
    }

    /**
     * Gets the module root path prefix.
     *
     * @return the module root path
     * @see Options#MODULE_ROOT
     */
    public String getModuleRoot() {
        return moduleRoot;
    }

    /**
     * Sets the module root path prefix.
     *
     * @param moduleRoot the module root path
     * @see Options#MODULE_ROOT
     */
    public void setModuleRoot(String moduleRoot) {
        this.moduleRoot = moduleRoot;
    }

    /**
     * Checks whether modules should be installed to the modules database.
     *
     * @return true if modules should be installed, false otherwise
     * @see Options#INSTALL
     */
    public boolean isDoInstall() {
        return doInstall;
    }

    /**
     * Sets whether modules should be installed to the modules database.
     *
     * @param doInstall true to install modules, false otherwise
     * @see Options#INSTALL
     */
    public void setDoInstall(boolean doInstall) {
        this.doInstall = doInstall;
    }

    /**
     * Sets the PRE-BATCH-MODULE path.
     *
     * @param preBatchModule the PRE-BATCH-MODULE path
     * @see Options#PRE_BATCH_MODULE
     */
    public void setPreBatchModule(String preBatchModule) {
        this.preBatchModule = preBatchModule;
    }

    /**
     * Gets the PRE-BATCH-MODULE path.
     *
     * @return the PRE-BATCH-MODULE path
     * @see Options#PRE_BATCH_MODULE
     */
    public String getPreBatchModule() {
        return this.preBatchModule;
    }

    /**
     * Sets the custom PRE-BATCH-TASK Java class.
     *
     * @param preBatchTaskCls the Task class for pre-batch operations
     * @see Options#PRE_BATCH_TASK
     */
    public void setPreBatchTaskClass(Class<? extends Task> preBatchTaskCls) {
        this.preBatchTaskCls = preBatchTaskCls;
    }

    /**
     * Gets the custom PRE-BATCH-TASK Java class.
     *
     * @return the Task class for pre-batch operations, or null if not configured
     * @see Options#PRE_BATCH_TASK
     */
    public Class<? extends Task> getPreBatchTaskClass() {
        return this.preBatchTaskCls;
    }

    /**
     * Sets the POST-BATCH-MODULE path.
     *
     * @param postBatchModule the POST-BATCH-MODULE path
     * @see Options#POST_BATCH_MODULE
     */
    public void setPostBatchModule(String postBatchModule) {
        this.postBatchModule = postBatchModule;
    }

    /**
     * Gets the POST-BATCH-MODULE path.
     *
     * @return the POST-BATCH-MODULE path
     * @see Options#POST_BATCH_MODULE
     */
    public String getPostBatchModule() {
        return this.postBatchModule;
    }

    /**
     * Sets the custom POST-BATCH-TASK Java class.
     *
     * @param postBatchTaskCls the Task class for post-batch operations
     * @see Options#POST_BATCH_TASK
     */
    public void setPostBatchTaskClass(Class<? extends Task> postBatchTaskCls) {
        this.postBatchTaskCls = postBatchTaskCls;
    }

    /**
     * Gets the custom POST-BATCH-TASK Java class.
     *
     * @return the Task class for post-batch operations, or null if not configured
     * @see Options#POST_BATCH_TASK
     */
    public Class<? extends Task> getPostBatchTaskClass() {
        return this.postBatchTaskCls;
    }

    /**
     * Gets the export file directory path.
     *
     * @return the export file directory path
     * @see Options#EXPORT_FILE_DIR
     */
    public String getExportFileDir() {
        return this.exportFileDir;
    }

    /**
     * Sets the export file directory path.
     *
     * @param exportFileDir the export file directory path
     * @see Options#EXPORT_FILE_DIR
     */
    public void setExportFileDir(String exportFileDir) {
        this.exportFileDir = exportFileDir;
    }

    /**
     * Sets the INIT-MODULE path.
     *
     * @param initModule the INIT-MODULE path
     * @see Options#INIT_MODULE
     */
    public void setInitModule(String initModule) {
        this.initModule = initModule;
    }

    /**
     * Gets the INIT-MODULE path.
     *
     * @return the INIT-MODULE path
     * @see Options#INIT_MODULE
     */
    public String getInitModule() {
        return this.initModule;
    }

    /**
     * Sets the custom INIT-TASK Java class.
     *
     * @param initTaskCls the Task class for initialization operations
     * @see Options#INIT_TASK
     */
    public void setInitTaskClass(Class<? extends Task> initTaskCls) {
        this.initTaskCls = initTaskCls;
    }

    /**
     * Gets the custom INIT-TASK Java class.
     *
     * @return the Task class for initialization operations, or null if not configured
     * @see Options#INIT_TASK
     */
    public Class<? extends Task> getInitTaskClass() {
        return this.initTaskCls;
    }

    /**
     * Gets the size of the thread pool work queue.
     *
     * @return the queue size (100,000)
     */
    public int getQueueSize() {
        return 100 * 1000;
    }

    /**
     * Sets whether the job should fail immediately on task errors.
     *
     * @param failOnError true to fail immediately, false to continue and log errors
     * @see Options#FAIL_ON_ERROR
     */
    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    /**
     * Checks whether the job should fail immediately on task errors.
     *
     * @return true if job fails on errors, false if it continues
     */
    public boolean isFailOnError() {
        return this.failOnError;
    }

    /**
     * Sets whether to use a disk-backed queue for URIs.
     *
     * @param useDiskQueue true to use disk queue, false for in-memory queue
     * @see Options#DISK_QUEUE
     */
    public void setUseDiskQueue(boolean useDiskQueue) {
        this.useDiskQueue = useDiskQueue;
    }

    /**
     * Checks whether to use a disk-backed queue for URIs.
     *
     * @return true if disk queue is enabled, false otherwise
     */
    public boolean shouldUseDiskQueue() {
        return this.useDiskQueue;
    }

    /**
     * Sets the maximum number of URIs to hold in memory before spilling to disk.
     *
     * @param size the maximum in-memory size
     * @see Options#DISK_QUEUE_MAX_IN_MEMORY_SIZE
     */
    public void setDiskQueueMaxInMemorySize(int size) {
        this.diskQueueMaxInMemorySize = size;
    }

    /**
     * Gets the maximum number of URIs to hold in memory before spilling to disk.
     *
     * @return the maximum in-memory size
     * @see Options#DISK_QUEUE_MAX_IN_MEMORY_SIZE
     */
    public int getDiskQueueMaxInMemorySize() {
        return this.diskQueueMaxInMemorySize;
    }

    /**
     * Sets the temporary directory for disk queue storage.
     *
     * @param directory the temporary directory
     * @see Options#DISK_QUEUE_TEMP_DIR
     */
    public void setDiskQueueTempDir(File directory) {
        this.diskQueueTempDir = directory;
    }

    /**
     * Gets the temporary directory for disk queue storage.
     *
     * @return the temporary directory
     * @see Options#DISK_QUEUE_TEMP_DIR
     */
    public File getDiskQueueTempDir() {
        return this.diskQueueTempDir;
    }

    /**
     * Sets whether CoRB should persist restart state for best-effort resume.
     *
     * @param restartable true to enable restartability, false otherwise
     * @see Options#RESTARTABLE
     */
    public void setRestartable(boolean restartable) {
        this.restartable = restartable;
    }

    /**
     * Checks whether restartability is enabled.
     *
     * @return true if restartability is enabled, false otherwise
     * @see Options#RESTARTABLE
     */
    public boolean isRestartable() {
        return this.restartable;
    }

    /**
     * Sets the directory used for restart state files.
     *
     * @param restartStateDir the restart state directory
     * @see Options#RESTART_STATE_DIR
     */
    public void setRestartStateDir(File restartStateDir) {
        this.restartStateDir = restartStateDir;
    }

    /**
     * Gets the directory used for restart state files.
     *
     * @return the restart state directory
     * @see Options#RESTART_STATE_DIR
     */
    public File getRestartStateDir() {
        return this.restartStateDir;
    }

    /**
     * Sets the number of recent TPS values to use for ETC calculation.
     *
     * @param numTpsForETC the number of TPS values (must be positive)
     * @see Options#NUM_TPS_FOR_ETC
     */
    public void setNumTpsForETC(int numTpsForETC) {
        if (numTpsForETC > 0) {
            this.numTpsForETC = numTpsForETC;
        }
    }

    /**
     * Gets the number of recent TPS values to use for ETC calculation.
     *
     * @return the number of TPS values
     * @see Options#NUM_TPS_FOR_ETC
     */
    public int getNumTpsForETC() {
        return this.numTpsForETC;
    }

	/**
	 * Gets the log level for metrics logging to server log.
	 *
	 * @return the log level (e.g., "INFO", "DEBUG", "NONE")
	 * @see Options#METRICS_LOG_LEVEL
	 */
	public String getLogMetricsToServerLog() {
        return logMetricsToServerLog;
	}

	/**
	 * Sets the log level for metrics logging to server log.
	 *
	 * @param logMetricsToServerLog the log level to set
	 * @see Options#METRICS_LOG_LEVEL
	 */
	public void setLogMetricsToServerLog(String logMetricsToServerLog) {
		this.logMetricsToServerLog = logMetricsToServerLog;
	}

	/**
	 * Gets whether metrics should be logged to server database.
	 *
	 * @return true if metrics should be logged to database, false otherwise
	 * @deprecated This property is not currently used
	 */
	@Deprecated
	public Boolean getLogMetricsToServerDB() {
        return logMetricsToServerDB;
	}

	/**
	 * Sets whether metrics should be logged to server database.
	 *
	 * @param logMetricsToServerDB true to log to database, false otherwise
	 * @deprecated This property is not currently used
	 */
	@Deprecated
	public void setLogMetricsToServerDB(Boolean logMetricsToServerDB) {
        this.logMetricsToServerDB = logMetricsToServerDB;
	}

	/**
	 * Gets the name of the database for saving metrics documents.
	 *
	 * @return the metrics database name
	 * @see Options#METRICS_DATABASE
	 */
	public String getMetricsDatabase() {
        return metricsDatabase;
	}

	/**
	 * Sets the name of the database for saving metrics documents.
	 *
	 * @param metricsDatabase the name of the database to save metrics documents
	 * @see Options#METRICS_DATABASE
	 */
	public void setMetricsDatabase(String metricsDatabase) {
		this.metricsDatabase = metricsDatabase;
	}

	/**
	 * Gets the root directory for metrics documents.
	 *
	 * @return the root directory for metrics documents
	 * @see Options#METRICS_ROOT
	 */
	public String getMetricsRoot() {
        return metricsRoot;
	}

	/**
	 * Sets the root directory for metrics documents.
	 *
	 * @param metricsRoot the root directory for metrics documents
	 * @see Options#METRICS_ROOT
	 */
	public void setMetricsRoot(String metricsRoot) {
		this.metricsRoot = metricsRoot;
	}

	/**
	 * Gets the module for producing and saving metrics.
	 *
	 * @return the metrics module path
	 * @see Options#METRICS_MODULE
	 */
	public String getMetricsModule() {
		return metricsModule;
	}

	/**
	 * Sets the module for producing and saving metrics.
	 *
	 * @param metricsModule module to produce and save metrics
	 * @see Options#METRICS_MODULE
	 */
	public void setMetricsModule(String metricsModule) {
		this.metricsModule = metricsModule;
	}

	/**
	 * Gets the collections to add to metrics documents when saving.
	 *
	 * @return the metrics collections (comma-separated)
	 * @see Options#METRICS_COLLECTIONS
	 */
	public String getMetricsCollections() {
		return metricsCollections;
	}

	/**
	 * Sets the collections to add to metrics documents when saving.
	 *
	 * @param metricsCollections the collections to add metrics documents when saving
	 * @see Options#METRICS_COLLECTIONS
	 */
	public void setMetricsCollections(String metricsCollections) {
		this.metricsCollections = metricsCollections;
	}

	/**
	 * Gets the job name.
	 *
	 * @return the job name
	 * @see Options#JOB_NAME
	 */
	public String getJobName() {
		return jobName;
	}

	/**
	 * Sets the job name.
	 *
	 * @param jobName the job name to set
	 * @see Options#JOB_NAME
	 */
	public void setJobName(String jobName) {
        this.jobName = jobName;
	}

	/**
	 * Gets the number of long-running URIs to track.
	 *
	 * @return the number of long-running URIs to track
	 * @see Options#METRICS_NUM_SLOW_TRANSACTIONS
	 */
	public Integer getNumberOfLongRunningUris() {
        return numberOfLongRunningUris;
	}

	/**
	 * Sets the number of long-running URIs to track.
	 *
	 * @param numberOfLongRunningUris the number of long-running URIs to track
	 * @see Options#METRICS_NUM_SLOW_TRANSACTIONS
	 */
	public void setNumberOfLongRunningUris(Integer numberOfLongRunningUris) {
		this.numberOfLongRunningUris = numberOfLongRunningUris;
	}

	/**
	 * Gets the number of failed URIs to track.
	 *
	 * @return the number of failed URIs to track
	 * @see Options#METRICS_NUM_FAILED_TRANSACTIONS
	 */
	public Integer getNumberOfFailedUris() {
        return numberOfFailedUris;
	}

	/**
	 * Sets the number of failed URIs to track.
	 *
	 * @param numberOfFailedUris the number of failed URIs to track
	 * @see Options#METRICS_NUM_FAILED_TRANSACTIONS
	 */
	public void setNumberOfFailedUris(Integer numberOfFailedUris) {
        this.numberOfFailedUris = numberOfFailedUris;
	}

	/**
	 * Gets the frequency for syncing metrics to the database.
	 *
	 * @return the sync frequency in milliseconds
	 * @see Options#METRICS_SYNC_FREQUENCY
	 */
	protected Integer getMetricsSyncFrequencyInMillis() {
        return metricsSyncFrequencyInMillis;
	}

	/**
	 * Sets the frequency for syncing metrics to the database.
	 *
	 * @param metricsSyncFrequencyInMillis the sync frequency in milliseconds
	 * @see Options#METRICS_SYNC_FREQUENCY
	 */
	protected void setMetricsSyncFrequencyInMillis(Integer metricsSyncFrequencyInMillis) {
		this.metricsSyncFrequencyInMillis = metricsSyncFrequencyInMillis;
	}

	/**
	 * Gets the port number for the job server.
	 *
	 * @return the job server port number
	 * @see Options#JOB_SERVER_PORT
	 */
	protected Integer getJobServerPort() {
        return jobServerPort;
	}

	/**
	 * Sets the port number for the job server.
	 *
	 * @param metricsOnDemandPort the job server port number
	 * @see Options#JOB_SERVER_PORT
	 */
	protected void setJobServerPort(Integer metricsOnDemandPort) {
        this.jobServerPort = metricsOnDemandPort;
	}

	/**
	 * Checks whether metrics logging to the server log is enabled.
	 *
	 * @param logMetricsToServerLog the log level to check, or null to use configured value
	 * @return true if metrics logging is enabled (not "NONE"), false otherwise
	 */
	protected boolean isMetricsLoggingEnabled(String logMetricsToServerLog){
		logMetricsToServerLog = logMetricsToServerLog == null ? getLogMetricsToServerLog() : logMetricsToServerLog;
		return logMetricsToServerLog != null && !logMetricsToServerLog.equalsIgnoreCase("NONE");
	}

	/**
	 * Gets the set of port candidates for the job server.
	 *
	 * @return the set of job server port candidates
	 * @see Options#JOB_SERVER_PORT
	 */
	public Set<Integer> getJobServerPortsToChoose() {
        return jobServerPortsToChoose;
	}

	/**
	 * Sets the set of port candidates for the job server.
	 *
	 * @param jobServerPortToChoose the set of job server port candidates
	 * @see Options#JOB_SERVER_PORT
	 */
	public void setJobServerPortsToChoose(Set<Integer> jobServerPortToChoose) {
		this.jobServerPortsToChoose = jobServerPortToChoose;
	}

	    /**
     * Sets whether URIs should be redacted from logs and metrics.
     *
     * @param redact true to redact URIs, false to include them
     * @see Options#URIS_REDACTED
     */
    public void setShouldRedactUris(boolean redact) {
	    redactUris = redact;
    }

    /**
     * Checks whether URIs should be redacted from logs and metrics.
     *
     * @return true if URIs should be redacted, false otherwise
     */
    protected boolean shouldRedactUris() {
        return redactUris;
    }
}
