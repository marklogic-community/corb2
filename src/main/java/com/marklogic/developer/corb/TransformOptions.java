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

import java.io.File;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Colleen Whitney, colleen.whitney@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public class TransformOptions {

    public static final int SLEEP_TIME_MS = 500;
    public static final long PROGRESS_INTERVAL_MS = 60L * SLEEP_TIME_MS;
    public static final String NAME = TransformOptions.class.getName();
    private static final String SLASH = "/";
    public static final String COLLECTION_TYPE = "COLLECTION";
    public static final String DIRECTORY_TYPE = "DIRECTORY";
    public static final String QUERY_TYPE = "QUERY";

    private String processModule;
    private Class<? extends Task> processTaskCls;

    private String preBatchModule;
    private Class<? extends Task> preBatchTaskCls;

    private String postBatchModule;
    private Class<? extends Task> postBatchTaskCls;

    private String initModule;
    private Class<? extends Task> initTaskCls;

    private String exportFileDir;

    // Defaults for optional arguments
    private String moduleRoot = SLASH;

    private String urisModule;
    private String urisFile;
    private Class<? extends UrisLoader> urisLoaderCls;

    private int threadCount = 1;
    private int batchSize = 1;
    private boolean useDiskQueue;
    private int diskQueueMaxInMemorySize = 1000;
    private File diskQueueTempDir;
    private boolean doInstall;

    private int numTpsForETC = 10;

    private boolean failOnError = true;

    // We could get rid of this now that we check status...
    private String modulesDatabase = "Modules";

    // Set on status check
    private String xdbcRoot = SLASH;

    /**
     * @return
     */
    public String getXDBC_ROOT() {
        return xdbcRoot;
    }

    /**
     * @param xdbc_root
     */
    public void setXDBC_ROOT(String xdbc_root) {
        this.xdbcRoot = xdbc_root;
    }

    /**
     * @return
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * @param count
     */
    public void setThreadCount(int count) {
        this.threadCount = count;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * @return
     */
    public String getLogLevel() {
        // TODO LogLevel make configurable
        return "INFO";
    }

    /**
     * @return
     */
    public String getLogHandler() {
        // TODO LogHandler make configurable
        return "CONSOLE";
    }

    /**
     * @return
     */
    public String getModulesDatabase() {
        return this.modulesDatabase;
    }

    /**
     * @param modulesDatabase
     */
    public void setModulesDatabase(String modulesDatabase) {
        this.modulesDatabase = modulesDatabase;
    }

    /**
     * @return
     */
    public String getUrisModule() {
        return urisModule;
    }

    /**
     * @param urisModule
     */
    public void setUrisModule(String urisModule) {
        this.urisModule = urisModule;
    }

    public String getUrisFile() {
        return this.urisFile;
    }

    public void setUrisFile(String urisFile) {
        this.urisFile = urisFile;
    }

    public Class<? extends UrisLoader> getUrisLoaderClass() {
        return this.urisLoaderCls;
    }

    public void setUrisLoaderClass(Class<? extends UrisLoader> urisLoaderCls) {
        this.urisLoaderCls = urisLoaderCls;
    }

    /**
     * @return
     */
    public String getProcessModule() {
        return processModule;
    }

    /**
     * @param processModule
     */
    public void setProcessModule(String processModule) {
        this.processModule = processModule;
    }

    /**
     * Java class
     *
     * @param processTaskCls
     */
    public void setProcessTaskClass(Class<? extends Task> processTaskCls) {
        this.processTaskCls = processTaskCls;
    }

    /**
     * Java Class
     *
     * @return
     */
    public Class<? extends Task> getProcessTaskClass() {
        return this.processTaskCls;
    }

    /**
     * @return
     */
    public String getModuleRoot() {
        return moduleRoot;
    }

    /**
     * @param moduleRoot
     */
    public void setModuleRoot(String moduleRoot) {
        this.moduleRoot = moduleRoot;
    }

    /**
     * @return
     */
    public boolean isDoInstall() {
        return doInstall;
    }

    /**
     * @param doInstall
     */
    public void setDoInstall(boolean doInstall) {
        this.doInstall = doInstall;
    }

    public void setPreBatchModule(String preBatchModule) {
        this.preBatchModule = preBatchModule;
    }

    public String getPreBatchModule() {
        return this.preBatchModule;
    }

    /**
     * Java Class
     *
     * @param preBatchTaskCls
     */
    public void setPreBatchTaskClass(Class<? extends Task> preBatchTaskCls) {
        this.preBatchTaskCls = preBatchTaskCls;
    }

    /**
     * Java Class
     *
     * @return
     */
    public Class<? extends Task> getPreBatchTaskClass() {
        return this.preBatchTaskCls;
    }

    public void setPostBatchModule(String postBatchModule) {
        this.postBatchModule = postBatchModule;
    }

    public String getPostBatchModule() {
        return this.postBatchModule;
    }

    /**
     * Java Class
     *
     * @param postBatchTaskCls
     */
    public void setPostBatchTaskClass(Class<? extends Task> postBatchTaskCls) {
        this.postBatchTaskCls = postBatchTaskCls;
    }

    /**
     * Java Class
     *
     * @return
     */
    public Class<? extends Task> getPostBatchTaskClass() {
        return this.postBatchTaskCls;
    }

    public String getExportFileDir() {
        return this.exportFileDir;
    }

    public void setExportFileDir(String exportFileDir) {
        this.exportFileDir = exportFileDir;
    }

    public void setInitModule(String initModule) {
        this.initModule = initModule;
    }

    public String getInitModule() {
        return this.initModule;
    }

    /**
     * Java Class
     *
     * @param initTaskCls
     */
    public void setInitTaskClass(Class<? extends Task> initTaskCls) {
        this.initTaskCls = initTaskCls;
    }

    /**
     * Java Class
     *
     * @return
     */
    public Class<? extends Task> getInitTaskClass() {
        return this.initTaskCls;
    }

    /**
     * The size of the ThreadPool work queue
     *
     * @return
     */
    public int getQueueSize() {
        return 100 * 1000;
    }

    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    public boolean isFailOnError() {
        return this.failOnError;
    }

    public void setUseDiskQueue(boolean useDiskQueue) {
        this.useDiskQueue = useDiskQueue;
    }

    public boolean shouldUseDiskQueue() {
        return this.useDiskQueue;
    }

    public void setDiskQueueMaxInMemorySize(int size) {
        this.diskQueueMaxInMemorySize = size;
    }

    public int getDiskQueueMaxInMemorySize() {
        return this.diskQueueMaxInMemorySize;
    }

    public void setDiskQueueTempDir(File directory) {
        this.diskQueueTempDir = directory;
    }

    public File getDiskQueueTempDir() {
        return this.diskQueueTempDir;
    }

    public void setNumTpsForETC(int numTpsForETC) {
        if (numTpsForETC > 0) {
            this.numTpsForETC = numTpsForETC;
        }
    }

    public int getNumTpsForETC() {
        return this.numTpsForETC;
    }
}
