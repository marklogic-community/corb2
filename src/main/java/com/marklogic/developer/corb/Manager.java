/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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
import static com.marklogic.developer.corb.Options.OPTIONS_FILE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_TASK;
import static com.marklogic.developer.corb.Options.POST_BATCH_XQUERY_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_TASK;
import static com.marklogic.developer.corb.Options.PRE_BATCH_XQUERY_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_TASK;
import static com.marklogic.developer.corb.Options.THREAD_COUNT;
import static com.marklogic.developer.corb.Options.URIS_FILE;
import static com.marklogic.developer.corb.Options.URIS_LOADER;
import static com.marklogic.developer.corb.Options.URIS_MODULE;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_URI;
import static com.marklogic.developer.corb.Options.XQUERY_MODULE;
import com.marklogic.developer.corb.util.FileUtils;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import com.marklogic.developer.corb.util.NumberUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmItem;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    protected static final String NAME = Manager.class.getName();

    public static final String URIS_BATCH_REF = com.marklogic.developer.corb.Options.URIS_BATCH_REF;
    public static final String DEFAULT_BATCH_URI_DELIM = ";";

    protected transient PausableThreadPoolExecutor pool;
    protected transient Monitor monitor;
    protected transient Thread monitorThread;
    protected transient CompletionService<String[]> completionService;
    protected transient ScheduledExecutorService scheduledExecutor;

    protected boolean execError;
    protected boolean stopCommand;

    static int EXIT_CODE_NO_URIS = EXIT_CODE_SUCCESS;
    protected static final int EXIT_CODE_STOP_COMMAND = 3;

    private static final Logger LOG = Logger.getLogger(Manager.class.getName());

    public Manager() {

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Manager manager = new Manager();
        try {
            manager.init(args);
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error initializing CORB", exc);
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
    public void init(String[] args, Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
        if (args == null) {
            args = new String[0];
        }
        if (props == null || props.isEmpty()) {
            initPropertiesFromOptionsFile();
        } else {
            this.properties = props;
        }

        initDecrypter();
        initSSLConfig();

        initURI(args.length > 0 ? args[0] : null);

        String collectionName = getOption(args.length > 1 ? args[1] : null, COLLECTION_NAME);
        this.collection = collectionName == null ? "" : collectionName;

        initOptions(args);

        logRuntimeArgs();

        prepareContentSource();
        registerStatusInfo();
        prepareModules();

        //This is relavant for unit tests only. clear the static map so it gets re-initialized for fresh run
        if (AbstractTask.MODULE_PROPS != null) {
            AbstractTask.MODULE_PROPS.clear();
        }

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

    protected void initOptions(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        // gather inputs		
        String processModule = getOption(args.length > 2 ? args[2] : null, PROCESS_MODULE);
        String threadCount = getOption(args.length > 3 ? args[3] : null, THREAD_COUNT);
        String urisModule = getOption(args.length > 4 ? args[4] : null, URIS_MODULE);
        String moduleRoot = getOption(args.length > 5 ? args[5] : null, MODULE_ROOT);
        String modulesDatabase = getOption(args.length > 6 ? args[6] : null, MODULES_DATABASE);
        String install = getOption(args.length > 7 ? args[7] : null, INSTALL);
        String processTask = getOption(args.length > 8 ? args[8] : null, PROCESS_TASK);
        String preBatchModule = getOption(args.length > 9 ? args[9] : null, PRE_BATCH_MODULE);
        String preBatchTask = getOption(args.length > 10 ? args[10] : null, PRE_BATCH_TASK);
        String postBatchModule = getOption(args.length > 11 ? args[11] : null, POST_BATCH_MODULE);
        String postBatchTask = getOption(args.length > 12 ? args[12] : null, POST_BATCH_TASK);
        String exportFileDir = getOption(args.length > 13 ? args[13] : null, EXPORT_FILE_DIR);
        String exportFileName = getOption(args.length > 14 ? args[14] : null, EXPORT_FILE_NAME);
        String urisFile = getOption(args.length > 15 ? args[15] : null, URIS_FILE);

        String urisLoader = getOption(URIS_LOADER);
        if (urisLoader != null) {
            options.setUrisLoaderClass(getUrisLoaderCls(urisLoader));
        }

        String initModule = getOption(INIT_MODULE);
        String initTask = getOption(INIT_TASK);

        String batchSize = getOption(BATCH_SIZE);
        String failOnError = getOption(FAIL_ON_ERROR);
        String errorFileName = getOption(ERROR_FILE_NAME);

        options.setUseDiskQueue(stringToBoolean(getOption(DISK_QUEUE)));
        String diskQueueMaxInMemorySize = getOption(DISK_QUEUE_MAX_IN_MEMORY_SIZE);
        String diskQueueTempDir = getOption(DISK_QUEUE_TEMP_DIR);

        //Check legacy properties keys, for backwards compatability
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
        if (install != null && (install.equalsIgnoreCase("true") || install.equals("1"))) {
            options.setDoInstall(true);
        }
        if (urisFile != null) {
            options.setUrisFile(urisFile);
        }
        if (batchSize != null) {
            options.setBatchSize(Integer.parseInt(batchSize));
        }
        if (failOnError != null && (failOnError.equalsIgnoreCase("false"))) {
            options.setFailOnError(false);
        }
        if (diskQueueMaxInMemorySize != null) {
            options.setDiskQueueMaxInMemorySize(Integer.parseInt(diskQueueMaxInMemorySize));
        }
        if (!this.properties.containsKey(EXPORT_FILE_DIR) && exportFileDir != null) {
            this.properties.put(EXPORT_FILE_DIR, exportFileDir);
        }
        if (!this.properties.containsKey(EXPORT_FILE_NAME) && exportFileName != null) {
            this.properties.put(EXPORT_FILE_NAME, exportFileName);
        }
        if (!this.properties.containsKey(ERROR_FILE_NAME) && errorFileName != null) {
            this.properties.put(ERROR_FILE_NAME, errorFileName);
        }

        if (urisFile != null) {
            File f = new File(options.getUrisFile());
            if (!f.exists()) {
                throw new IllegalArgumentException("Uris file " + urisFile + " not found");
            }
        }

        if (initModule != null) {
            options.setInitModule(initModule);
        }
        if (initTask != null) {
            options.setInitTaskClass(getTaskCls(INIT_TASK, initTask));
        }

        // java class for processing individual tasks.
        // If specified, it is used instead of xquery module, but xquery module is
        // still required.
        if (processTask != null) {
            options.setProcessTaskClass(getTaskCls(PROCESS_TASK, processTask));
        }
        if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
            throw new NullPointerException(PROCESS_TASK + " or " + PROCESS_MODULE + " must be specified");
        }

        if (preBatchModule != null) {
            options.setPreBatchModule(preBatchModule);
        }
        if (preBatchTask != null) {
            options.setPreBatchTaskClass(getTaskCls(PRE_BATCH_TASK, preBatchTask));
        }

        if (postBatchModule != null) {
            options.setPostBatchModule(postBatchModule);
        }
        if (postBatchTask != null) {
            options.setPostBatchTaskClass(getTaskCls(POST_BATCH_TASK, postBatchTask));
        }

        if (options.getPostBatchTaskClass() == null) {
            if (this.properties.containsKey(EXPORT_FILE_PART_EXT)) {
                this.properties.remove(EXPORT_FILE_PART_EXT);
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

        // delete the export file if it exists
        deleteFileIfExists(exportFileDir, exportFileName);
        deleteFileIfExists(exportFileDir, errorFileName);

        normalizeLegacyProperties();
    }

    protected boolean deleteFileIfExists(String directory, String filename) {
        if (filename != null) {
            File file = new File(directory, filename);
            if (file.exists()) {
                return file.delete();
            }
        }
        return false;
    }

    protected void normalizeLegacyProperties() {
        //fix map keys for backward compatibility
        if (this.properties != null) {
            this.properties.putAll(getNormalizedProperties(this.properties));
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
        Map<String, String> legacyProperties = new HashMap<String, String>(3);
        legacyProperties.put(PROCESS_MODULE, XQUERY_MODULE);
        legacyProperties.put(PRE_BATCH_MODULE, PRE_BATCH_XQUERY_MODULE);
        legacyProperties.put(POST_BATCH_MODULE, POST_BATCH_XQUERY_MODULE);

        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            for (Map.Entry<String, String> entry : legacyProperties.entrySet()) {
                String legacyKey = entry.getValue();
                String legacyKeyPrefix = legacyKey + ".";
                String normalizedKey = entry.getKey();
                String normalizedKeyPrefix = normalizedKey + ".";
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
        PrintStream err = System.err;
        err.println("usage 1:");
        err.println("\t" + NAME + " xcc://user:password@host:port/[ database ]" + " input-selector module-name.xqy"
                + " [ thread-count [ uris-module [ module-root" + " [ modules-database [ install [ process-task"
                + " [ pre-batch-module [ pre-batch-task" + " [ post-batch-module  [ post-batch-task"
                + " [ export-file-dir [ export-file-name" + " [ uris-file ] ] ] ] ] ] ] ] ] ] ] ] ]");
        err.println("\nusage 2:");
        err.println("\t-D" + XCC_CONNECTION_URI + "=xcc://user:password@host:port/[ database ]"
                + " -D" + XQUERY_MODULE + "=module-name.xqy" + " -D" + THREAD_COUNT + "=10"
                + " -D" + URIS_MODULE + "=get-uris.xqy"
                + " -D" + POST_BATCH_XQUERY_MODULE + "=post-batch.xqy"
                + " -D... " + NAME);
        err.println("\nusage 3:");
        err.println("\t" + "-D" + OPTIONS_FILE + "=myjob.properties " + NAME);
        err.println("\nusage 4:");
        err.println("\t" + "-D" + OPTIONS_FILE + "=myjob.properties"
                + " -D" + THREAD_COUNT + "=10 " + NAME
                + " xcc://user:password@host:port/[ database ]");
    }

    public int run() throws Exception {
        LOG.log(INFO, "{0} starting: {1}", new Object[]{NAME, VERSION_MSG});
        long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        LOG.log(INFO, "maximum heap size = {0} MiB", maxMemory);

        this.execError = false; //reset execution error flag for a new run
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
            if (!execError && count > 0) {
                runPostBatchTask(); // post batch tasks
                LOG.info("all done");
            }
            return count;
        } catch (Exception e) {
            LOG.log(SEVERE, e.getMessage());
            stop();
            throw e;
        }
    }

    /**
     * @return
     */
    private Thread preparePool() {
        RejectedExecutionHandler policy = new CallerBlocksPolicy();
        int threads = options.getThreadCount();
        // an array queue should be somewhat lighter-weight
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(options.getQueueSize());
        pool = new PausableThreadPoolExecutor(threads, threads, 16, TimeUnit.SECONDS, workQueue, policy);
        pool.prestartAllCoreThreads();
        completionService = new ExecutorCompletionService<String[]>(pool);
        monitor = new Monitor(pool, completionService, this);
        Thread thread = new Thread(monitor, "monitor");
        return thread;
    }

    /**
     * @throws IOException,RequestException
     *
     */
    private void prepareModules() throws IOException, RequestException {
        String[] resourceModules = new String[]{options.getInitModule(), options.getUrisModule(),
            options.getProcessModule(), options.getPreBatchModule(), options.getPostBatchModule()};
        String modulesDatabase = options.getModulesDatabase();
        LOG.log(INFO, "checking modules, database: {0}", modulesDatabase);
        Session session = contentSource.newSession(modulesDatabase);
        InputStream is = null;
        Content c = null;
        ContentCreateOptions opts = ContentCreateOptions.newTextInstance();
        try {
            for (String resourceModule : resourceModules) {
                if (resourceModule == null || isInlineOrAdhoc(resourceModule)) {
                    continue;
                }

                // Start by checking install flag.
                if (!options.isDoInstall()) {
                    LOG.log(INFO, "Skipping module installation: {0}", resourceModule);
                    continue;
                } // Next check: if XCC is configured for the filesystem, warn
                // user
                else if (options.getModulesDatabase().equals("")) {
                    LOG.warning("XCC configured for the filesystem: please install modules manually");
                    return;
                } // Finally, if it's configured for a database, install.
                else {
                    File f = new File(resourceModule);
                    // If not installed, are the specified files on the
                    // filesystem?
                    if (f.exists()) {
                        String moduleUri = options.getModuleRoot() + f.getName();
                        c = ContentFactory.newContent(moduleUri, f, opts);
                    } // finally, check package
                    else {
                        LOG.log(WARNING, "looking for {0} as resource", resourceModule);
                        String moduleUri = options.getModuleRoot() + resourceModule;
                        is = this.getClass().getResourceAsStream(resourceModule);
                        if (null == is) {
                            throw new NullPointerException(resourceModule + " could not be found on the filesystem," + " or in package resources");
                        }
                        c = ContentFactory.newContent(moduleUri, is, opts);
                    }
                    session.insertContent(c);
                }
            }
        } catch (IOException e) {
            LOG.log(SEVERE, "error while reading modules {0}", e.getMessage());
            throw e;
        } catch (RequestException e) {
            LOG.log(SEVERE, "error while loading modules {0}", e.getMessage());
            throw e;
        } finally {
            session.close();
            if (null != is) {
                try {
                    is.close();
                } catch (IOException ioe) {
                    LOG.log(SEVERE, "Couldn't close the stream", ioe);
                }
            }
        }
    }

    protected void registerStatusInfo() {
        Session session = contentSource.newSession();
        AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_ML + DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
                + "let $status := \n" + " xdmp:server-status(xdmp:host(), xdmp:server())\n"
                + "let $modules := $status/mlss:modules\n"
                + "let $root := $status/mlss:root\n"
                + "return (data($modules), data($root))");
        ResultSequence rs = null;
        try {
            rs = session.submitRequest(q);
        } catch (RequestException e) {
            e.printStackTrace();
        } finally {
            session.close();
        }
        while ((null != rs) && rs.hasNext()) {
            ResultItem rsItem = rs.next();
            XdmItem item = rsItem.getItem();
            if (rsItem.getIndex() == 0 && item.asString().equals("0")) {
                options.setModulesDatabase("");
            }
            if (rsItem.getIndex() == 1) {
                options.setXDBC_ROOT(item.asString());
            }
        }

        LOG.log(INFO, "Configured modules db: {0}", options.getModulesDatabase());
        LOG.log(INFO, "Configured modules xdbc root: {0}", options.getXDBC_ROOT());
        LOG.log(INFO, "Configured modules root: {0}", options.getModuleRoot());
        LOG.log(INFO, "Configured uri module: {0}", options.getUrisModule());
        LOG.log(INFO, "Configured uri file: {0}", options.getUrisFile());
        LOG.log(INFO, "Configured uri loader: {0}", options.getUrisLoaderClass());
        LOG.log(INFO, "Configured process module: {0}", options.getProcessModule());
        LOG.log(INFO, "Configured process task: {0}", options.getProcessTaskClass());
        LOG.log(INFO, "Configured pre batch module: {0}", options.getPreBatchModule());
        LOG.log(INFO, "Configured pre batch task: {0}", options.getPreBatchTaskClass());
        LOG.log(INFO, "Configured post batch module: {0}", options.getPostBatchModule());
        LOG.log(INFO, "Configured post batch task: {0}", options.getPostBatchTaskClass());
        LOG.log(INFO, "Configured init module: {0}", options.getInitModule());
        LOG.log(INFO, "Configured init task: {0}", options.getInitTaskClass());
        LOG.log(INFO, "Configured thread count: {0}", options.getThreadCount());
        LOG.log(INFO, "Configured batch size: {0}", options.getBatchSize());
        LOG.log(INFO, "Configured failonError: {0}", options.isFailOnError());
        LOG.log(INFO, "Configured URIs queue max in-memory size: {0}", options.getDiskQueueMaxInMemorySize());
        LOG.log(INFO, "Configured URIs queue temp dir: {0}", options.getDiskQueueTempDir());
        logProperties();
    }

    protected void logProperties() {
        for (Entry<Object, Object> e : properties.entrySet()) {
            if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
                LOG.log(INFO, "Loaded property {0}={1}", new Object[]{e.getKey(), e.getValue()});
            }
        }
    }

    private void runInitTask(TaskFactory tf) throws Exception {
        Task initTask = tf.newInitTask();
        if (initTask != null) {
            LOG.info("Running init Task");
            initTask.call();
        }
    }

    private void runPreBatchTask(TaskFactory tf) throws Exception {
        Task preTask = tf.newPreBatchTask();
        if (preTask != null) {
            LOG.info("Running pre batch Task");
            preTask.call();
        }
    }

    private void runPostBatchTask() throws Exception {
        TaskFactory tf = new TaskFactory(this);
        Task postTask = tf.newPostBatchTask();
        if (postTask != null) {
            LOG.info("Running post batch Task");
            postTask.call();
        }
    }

    private UrisLoader getUriLoader() throws InstantiationException, IllegalAccessException {
        UrisLoader loader = null;
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
        UrisLoader urisLoader = getUriLoader();
        int expectedTotalCount = -1;
        int urisCount = 0;
        try {
            // run init task
            runInitTask(taskFactory);

            urisLoader.open();
            if (urisLoader.getBatchRef() != null) {
                properties.put(URIS_BATCH_REF, urisLoader.getBatchRef());
                LOG.log(INFO, "{0}: {1}", new Object[]{URIS_BATCH_REF, urisLoader.getBatchRef()});
            }

            expectedTotalCount = urisLoader.getTotalCount();
            LOG.log(INFO, "expecting total {0}", expectedTotalCount);
            if (expectedTotalCount <= 0) {
                LOG.info("nothing to process");
                stop();
                return 0;
            }

            // run pre-batch task, if present.
            runPreBatchTask(taskFactory);

            // now start process tasks
            monitor.setTaskCount(expectedTotalCount);
            monitorThread.start();

            String uri;
            List<String> uriBatch = new ArrayList<String>(options.getBatchSize());

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
            }

            if (urisCount == expectedTotalCount) {
                LOG.log(INFO, "queue is populated with {0} tasks", urisCount);
            } else {
                LOG.log(WARNING, "queue is expected to be populated with {0} tasks, but got {1} tasks.", new Object[]{expectedTotalCount, urisCount});
                monitor.setTaskCount(urisCount);
            }

            pool.shutdown();

        } catch (Exception exc) {
            stop();
            throw exc;
        } finally {
            closeQuietly(urisLoader);
        }

        return urisCount;
    }

    public void setThreadCount(int threadCount) {
        if (threadCount > 0) {
            if (threadCount != options.getThreadCount()) {
                options.setThreadCount(threadCount);
                if (pool != null) {
                    int currentMaxPoolSize = pool.getMaximumPoolSize();
                    try {
                        if (threadCount < currentMaxPoolSize) {
                            //shrink the core first then max
                            pool.setCorePoolSize(threadCount);
                            pool.setMaximumPoolSize(threadCount);
                        } else {
                            //grow max first, then core
                            pool.setMaximumPoolSize(threadCount);
                            pool.setCorePoolSize(threadCount);
                        }
                        LOG.log(INFO, "Changed {0} to {1}", new Object[]{Options.THREAD_COUNT, threadCount});
                    } catch (IllegalArgumentException ex) {
                        LOG.log(WARNING, "Unable to change thread count", ex);
                    }
                }
            }
        } else {
            LOG.log(WARNING, Options.THREAD_COUNT + " must be a positive integer value");
        }
    }

    /**
     * Pause execution of pool tasks
     */
    public void pause() {
        if (pool != null && pool.isRunning()) {
            LOG.info("pausing");
            pool.pause();
        }
    }

    /**
     * Resume pool execution (if paused).
     */
    public void resume() {
        if (pool != null && pool.isPaused()) {
            LOG.info("resuming");
            pool.resume();
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
            if (remaining.size() > 0) {
                LOG.log(WARNING, "thread pool was shut down with {0} pending tasks", remaining.size());
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
        this.execError = true;
        LOG.log(SEVERE, "fatal error", e.getCause());
        LOG.warning("exiting due to fatal error");
        stop();
    }

    public static class CommandFileWatcher implements Runnable {

        private long timeStamp;
        private final File file;
        private final Manager manager;

        public CommandFileWatcher(File file, Manager manager) {
            this.file = file;
            this.timeStamp = -1;
            this.manager = manager;
        }

        @Override
        public final void run() {
            if (file.exists()) {
                long lastModified = file.lastModified();
                if (this.timeStamp != lastModified) {
                    this.timeStamp = lastModified;
                    onChange(file);
                }
            }
        }

        public void onChange(File file) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
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
                LOG.log(WARNING, MessageFormat.format("Unable to load {0}", COMMAND_FILE), e);
            } finally {
                closeQuietly(in);
            }
        }
    }

    public static class CallerBlocksPolicy implements RejectedExecutionHandler {

        private transient BlockingQueue<Runnable> queue;

        private boolean warning;

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (null == queue) {
                queue = executor.getQueue();
            }
            try {
                // block until space becomes available
                if (!warning) {
                    LOG.log(INFO, "queue is full: size = {0} (will only appear once)", queue.size());
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
