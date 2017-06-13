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

import static com.marklogic.developer.corb.Constants.COLLECTION_NAME;
import static com.marklogic.developer.corb.Constants.COMMAND_FILE;
import static com.marklogic.developer.corb.Constants.COMMAND_FILE_POLL_INTERVAL;
import static com.marklogic.developer.corb.Constants.EXIT_CODE_NO_URIS;
import static com.marklogic.developer.corb.Constants.EXPORT_FILE_DIR;
import static com.marklogic.developer.corb.Constants.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.Constants.INSTALL;
import static com.marklogic.developer.corb.Constants.MODULES_DATABASE;
import static com.marklogic.developer.corb.Constants.MODULE_ROOT;
import static com.marklogic.developer.corb.Constants.OPTIONS_FILE;
import static com.marklogic.developer.corb.Constants.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Constants.POST_BATCH_TASK;
import static com.marklogic.developer.corb.Constants.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Constants.PRE_BATCH_TASK;
import static com.marklogic.developer.corb.Constants.PROCESS_MODULE;
import static com.marklogic.developer.corb.Constants.PROCESS_TASK;
import static com.marklogic.developer.corb.Constants.THREAD_COUNT;
import static com.marklogic.developer.corb.Constants.URIS_FILE;
import static com.marklogic.developer.corb.Constants.URIS_LOADER;
import static com.marklogic.developer.corb.Constants.URIS_MODULE;
import static com.marklogic.developer.corb.Constants.XCC_CONNECTION_URI;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.CollectionUtils;
import com.marklogic.developer.corb.util.NumberUtils;
import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

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

    protected static int exitCodeNoUris = Options.DEFAULT_EXIT_CODE_SUCCESS;

    private static final Logger LOG = Logger.getLogger(Manager.class.getName());
    private static final String TAB = "\t";
    
    protected static final String[] argNames = {
        XCC_CONNECTION_URI,COLLECTION_NAME,PROCESS_MODULE,THREAD_COUNT,
        URIS_MODULE,MODULE_ROOT,MODULES_DATABASE,INSTALL,PROCESS_TASK,
        PRE_BATCH_MODULE,PRE_BATCH_TASK,POST_BATCH_MODULE,POST_BATCH_TASK,
        EXPORT_FILE_DIR,EXPORT_FILE_NAME,URIS_FILE
    };

    /**
     * @param args
     */
    public static void main(String... args) {
        Manager manager = new Manager();
        try {
            manager.init(getArgsAsMap(argNames,args));
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error initializing CORB " + exc.getMessage(), exc);
            manager.usage();
            System.exit(Options.DEFAULT_EXIT_CODE_INIT_ERROR);
        }
        //now we can start corb.
        try {
            int count = manager.run();
            if (manager.execError) {
                System.exit(Options.DEFAULT_EXIT_CODE_PROCESSING_ERROR);
            } else if (manager.stopCommand) {
                System.exit(Options.DEFAULT_EXIT_CODE_STOP_COMMAND);
            } else if (count == 0) {
                System.exit(exitCodeNoUris);
            } else {
                System.exit(Options.DEFAULT_EXIT_CODE_SUCCESS);
            }
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error while running CORB", exc);
            System.exit(Options.DEFAULT_EXIT_CODE_PROCESSING_ERROR);
        }
    }
    
    @Override
    public void init(Map<String,String> argsAsMap, Map<String,String> props) throws CorbException {
        super.init(argsAsMap, props);
        
        prepareModules();

        exitCodeNoUris = NumberUtils.toInt(options.getProperty(EXIT_CODE_NO_URIS),Options.DEFAULT_EXIT_CODE_NO_URIS);

        scheduleCommandFileWatcher();
        
        // delete the export file if it exists
        deleteFileIfExists(options.getExportFileDir(), options.getExportFileName());
        deleteFileIfExists(options.getExportFileDir(), options.getErrorFileName());
    }

    protected void scheduleCommandFileWatcher() {
        String commandFile = options.getProperty(COMMAND_FILE);
        if (isNotBlank(commandFile)) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            CommandFileWatcher commandFileWatcher = new CommandFileWatcher(FileUtils.getFile(commandFile), this);
            int pollInterval = NumberUtils.toInt(options.getProperty(COMMAND_FILE_POLL_INTERVAL), Options.DEFAULT_COMMAND_FILE_POLL_INTERVAL);
            scheduledExecutor.scheduleWithFixedDelay(commandFileWatcher, pollInterval, pollInterval, TimeUnit.SECONDS);
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
        LOG.log(INFO, () -> MessageFormat.format("{0} starting: {1}", NAME, VERSION_MSG));
        long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        LOG.log(INFO, () -> MessageFormat.format("maximum heap size = {0} MiB", maxMemory));

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
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(options.getQueueSize());
        pool = new PausableThreadPoolExecutor(threads, threads, 16, TimeUnit.SECONDS, workQueue, policy);
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
        loader.setCollection(options.getCollection());
        return loader;
    }

    private int populateQueue() throws Exception {
        LOG.info("populating queue");
        TaskFactory taskFactory = new TaskFactory(this);

        int expectedTotalCount = -1;
        int urisCount = 0;
        try (UrisLoader urisLoader = getUriLoader()) {
            // run init task
            runInitTask(taskFactory);

            urisLoader.open();
            if (urisLoader.getBatchRef() != null) {
                options.setProperty(URIS_BATCH_REF, urisLoader.getBatchRef());
                LOG.log(INFO, () -> MessageFormat.format("{0}: {1}", URIS_BATCH_REF, urisLoader.getBatchRef()));
            }

            expectedTotalCount = urisLoader.getTotalCount();
            LOG.log(INFO, "expecting total " + expectedTotalCount);
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

            urisCount = submitUriTasks(urisLoader, taskFactory, expectedTotalCount);

            if (urisCount == expectedTotalCount) {
                LOG.log(INFO, MessageFormat.format("queue is populated with {0} tasks", urisCount));
            } else {
                LOG.log(WARNING, MessageFormat.format("queue is expected to be populated with {0} tasks, but got {1} tasks.", expectedTotalCount, urisCount));
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
                LOG.log(INFO, MessageFormat.format("received {0}/{1}: {2}", urisCount, expectedTotalCount, uri));
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
            LOG.log(memoryLogLevel, () -> MessageFormat.format("free memory: {0} MiB of {1}", freeMemory / megabytes, totalMemory / megabytes));
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
        }
    }

    public boolean isPaused() {
        return pool != null && pool.isPaused();
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
            if (!remaining.isEmpty()) {
                LOG.log(WARNING, () -> MessageFormat.format("thread pool was shut down with {0} pending tasks", remaining.size()));
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
