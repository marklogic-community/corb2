/*
 * Copyright (c)2005-2012 Mark Logic Corporation
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
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * @author Colleen Whitney, MarkLogic Corporation
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class Manager extends AbstractManager{	
	protected static final String NAME = Manager.class.getName();
	
	public static final String URIS_BATCH_REF = "URIS_BATCH_REF";
	public static final String DEFAULT_BATCH_URI_DELIM = ";";
	
	private ThreadPoolExecutor pool;
	private Monitor monitor;
	private Thread monitorThread;
	private CompletionService<String[]> completionService;
	private boolean execError=false;

	private static final Logger LOG = Logger.getLogger(Manager.class.getSimpleName());
		
	public class CallerBlocksPolicy implements RejectedExecutionHandler {

		private BlockingQueue<Runnable> queue;

		private boolean warning = false;

		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			if (null == queue) {
				queue = executor.getQueue();
			}
			try {
				// block until space becomes available
				if (!warning) {
					LOG.log(Level.INFO, "queue is full: size = {0} (will only appear once)", queue.size());
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
	
	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static void main(String[] args){
		Manager tm = new Manager();
		try {
			tm.init(args);
		} catch(Exception exc){
			LOG.log(Level.SEVERE, "Error initializing CORB",exc);
			System.exit(1);
		}
		//now we can start corb. 
		try {
			int count = tm.run();
			if (tm.execError){
				System.exit(2);
			} else if (count == 0) {
				System.exit(9);
			} else {
				System.exit(0);
			}
		} catch(Exception exc){
			LOG.log(Level.SEVERE, "Error while running CORB",exc);
			System.exit(2);
		}
	}

	public Manager(){
		
	}
	
    @Override
	public void init(String[] args, Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException{			
		if (args == null) {
            args = new String[0];
        }
        if (props == null || props.isEmpty()){
			initPropertiesFromOptionsFile();
		} else {
			this.properties = props;
		}
		
		initDecrypter();
		initSSLConfig();
		
		initURI(args.length > 0 ? args[0] : null);
		
		String collection = getOption(args.length > 1 ? args[1] : null, "COLLECTION-NAME");
		this.collection = collection != null ? collection : "";
        
		initOptions(args);
		
		logRuntimeArgs();
		
		prepareContentSource();
		registerStatusInfo();
		prepareModules();
		
		//This is relavant for unit tests only. clear the static map so it gets re-initialized for fresh run
		if (AbstractTask.MODULE_PROPS != null) { AbstractTask.MODULE_PROPS.clear(); }
	}
			
	protected void initOptions(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		// gather inputs		
		String processModule = getOption(args.length > 2 ? args[2] : null, "PROCESS-MODULE");
		String threadCount = getOption(args.length > 3 ? args[3] : null, "THREAD-COUNT");
		String urisModule = getOption(args.length > 4 ? args[4] : null, "URIS-MODULE");
		String moduleRoot = getOption(args.length > 5 ? args[5] : null, "MODULE-ROOT");
		String modulesDatabase = getOption(args.length > 6 ? args[6] : null, "MODULES-DATABASE");
		String install = getOption(args.length > 7 ? args[7] : null, "INSTALL");
		String processTask = getOption(args.length > 8 ? args[8] : null, "PROCESS-TASK");
		String preBatchModule = getOption(args.length > 9 ? args[9] : null, "PRE-BATCH-MODULE");
		String preBatchTask = getOption(args.length > 10 ? args[10] : null, "PRE-BATCH-TASK");
		String postBatchModule = getOption(args.length > 11 ? args[11] : null, "POST-BATCH-MODULE");
		String postBatchTask = getOption(args.length > 12 ? args[12] : null, "POST-BATCH-TASK");
		String exportFileDir = getOption(args.length > 13 ? args[13] : null, "EXPORT-FILE-DIR");
		String exportFileName = getOption(args.length > 14 ? args[14] : null, "EXPORT-FILE-NAME");
		String urisFile = getOption(args.length > 15 ? args[15] : null, "URIS-FILE");
		
		String urisLoader = getOption(null, "URIS-LOADER");
		if (urisLoader != null) options.setUrisLoaderClass(getUrisLoaderCls(urisLoader));
		
		String initModule = getOption(null, "INIT-MODULE");
		String initTask = getOption(null, "INIT-TASK");
		
		String batchSize = getOption(null, "BATCH-SIZE");
		String failOnError = getOption(null, "FAIL-ON-ERROR");
		String errorFileName = getOption(null, "ERROR-FILE-NAME");
		
		if (processModule == null) processModule = getOption(null, "XQUERY-MODULE");
		if (preBatchModule == null) preBatchModule = getOption(null,"PRE-BATCH-XQUERY-MODULE");
        if (postBatchModule == null) postBatchModule = getOption(null,"POST-BATCH-XQUERY-MODULE");
		
		if (moduleRoot != null) options.setModuleRoot(moduleRoot);
		if (processModule != null) options.setProcessModule(processModule);
		if (threadCount != null) options.setThreadCount(Integer.parseInt(threadCount));
		if (urisModule != null) options.setUrisModule(urisModule);
		if (modulesDatabase != null) options.setModulesDatabase(modulesDatabase);
		if (install != null && (install.equalsIgnoreCase("true") || install.equals("1"))) options.setDoInstall(true);
		if (urisFile != null) options.setUrisFile(urisFile);
		if (batchSize != null) options.setBatchSize(Integer.parseInt(batchSize));
		if (failOnError != null && (failOnError.equalsIgnoreCase("false"))) options.setFailOnError(false);
		
		if (!this.properties.containsKey("EXPORT-FILE-DIR") && exportFileDir != null) {
			this.properties.put("EXPORT-FILE-DIR", exportFileDir);
		}
		if (!this.properties.containsKey("EXPORT-FILE-NAME") && exportFileName != null) {
			this.properties.put("EXPORT-FILE-NAME", exportFileName);
		}
		if (!this.properties.containsKey("ERROR-FILE-NAME") && errorFileName != null) {
			this.properties.put("ERROR-FILE-NAME", errorFileName);
		}
		
		if (urisFile != null && urisFile.trim().length() > 0) {
			File f = new File(options.getUrisFile());
			if (!f.exists()) {
				throw new IllegalArgumentException("Uris file " + urisFile + " not found");
			}
		}
		
		if (initModule != null) options.setInitModule(initModule);
		if (initTask != null) options.setInitTaskClass(getTaskCls("INIT-TASK",initTask));
				
		// java class for processing individual tasks.
		// If specified, it is used instead of xquery module, but xquery module is
		// still required.
		if (processTask != null) options.setProcessTaskClass(getTaskCls("PROCESS-TASK",processTask));
		if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
			throw new NullPointerException("PROCESS-TASK or PROCESS-MODULE must be specified");
		}
		
		if (preBatchModule != null) options.setPreBatchModule(preBatchModule);
		if (preBatchTask != null) options.setPreBatchTaskClass(getTaskCls("PRE-BATCH-TASK",preBatchTask));
		
		if (postBatchModule != null) options.setPostBatchModule(postBatchModule);
		if (postBatchTask != null) options.setPostBatchTaskClass(getTaskCls("POST-BATCH-TASK",postBatchTask));
		
		if (options.getPostBatchTaskClass() == null) {
			if(this.properties.containsKey("EXPORT-FILE-PART-EXT")) this.properties.remove("EXPORT-FILE-PART-EXT");
			if(System.getProperty("EXPORT-FILE-PART-EXT") != null) System.clearProperty("EXPORT-FILE-PART-EXT");
		}

		if (exportFileDir != null) {
			File dirFile = new File(exportFileDir);
			if (dirFile.exists() && dirFile.canWrite()) {
				options.setExportFileDir(exportFileDir);
			} else {
				throw new IllegalArgumentException("Cannot write to export folder " + exportFileDir);
			}
		}

		// delete the export file if it exists
		if (exportFileName != null) {
			File exportFile = new File(exportFileDir, exportFileName);
			if (exportFile.exists()) {
				exportFile.delete();
			}
		}
		
		if (errorFileName != null) {
			File errorFile = new File(exportFileDir, errorFileName);
			if (errorFile.exists()) {
				errorFile.delete();
			}
		}
		
		//fix map keys for backward compatibility
		for(String key : this.properties.stringPropertyNames()) {
		  String value = this.properties.getProperty(key);
		  if (key.startsWith("XQUERY-MODULE.") && value != null){
		  	properties.setProperty(key.replace("XQUERY-MODULE.", "PROCESS-MODULE."), value);
		  }
		}
		
		for(String key : System.getProperties().stringPropertyNames()) {
		  String value = System.getProperty(key);
		  if (key.startsWith("XQUERY-MODULE.") && value != null){
		  	System.setProperty(key.replace("XQUERY-MODULE.", "PROCESS-MODULE."), value);
		  }
		}
	}
	
	protected Class<? extends Task> getTaskCls(String type, String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		Class<?> cls = Class.forName(className);
		if (Task.class.isAssignableFrom(cls)) {
			cls.newInstance(); // sanity check
			return cls.asSubclass(Task.class);
		} else {
			throw new IllegalArgumentException(type+" must be of type com.marklogic.developer.corb.Task");
		}
	}
			
	protected Class<? extends UrisLoader> getUrisLoaderCls(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
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
		err.println("\t" + "-DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ]"
				+ " -DXQUERY-MODULE=module-name.xqy" + " -DTHREAD-COUNT=10" + " -DURIS-MODULE=get-uris.xqy"
				+ " -DPOST-BATCH-XQUERY-MODULE=post-batch.xqy" + " -D... " + NAME);
		err.println("\nusage 3:");
		err.println("\t" + "-DOPTIONS-FILE=myjob.properties " + NAME);
		err.println("\nusage 4:");
		err.println("\t" + "-DOPTIONS-FILE=myjob.properties" + " -DTHREAD-COUNT=10 " + NAME
				+ " xcc://user:password@host:port/[ database ]");
	}

	public int run() throws Exception {
		LOG.log(Level.INFO, "{0} starting: {1}", new Object[] { NAME, VERSION_MSG });
		long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
		LOG.log(Level.INFO, "maximum heap size = {0} MiB", maxMemory);
		this.execError=false; //reset execution error flag for a new run
		monitorThread = preparePool();
		
		try {
			int count = populateQueue();

			while (monitorThread.isAlive()) {
				try {
					monitorThread.join();
				} catch (InterruptedException e) {
					// reset interrupt status and continue
					Thread.interrupted();
					LOG.log(Level.SEVERE, "interrupted while waiting for monitor", e);
				}
			}
			if (!execError) {
				runPostBatchTask(); // post batch tasks
				LOG.info("all done");
			}
			return count;
		} catch (Exception e) {
			LOG.log(Level.SEVERE, e.getMessage());
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
		pool = new ThreadPoolExecutor(threads, threads, 16, TimeUnit.SECONDS, workQueue, policy);
		pool.prestartAllCoreThreads();
		completionService = new ExecutorCompletionService<String[]>(pool);
		monitor = new Monitor(pool, completionService, this);
		Thread thread = new Thread(monitor);
		return thread;
	}

	/**
	 * @throws IOException,RequestException 
	 * 
	 */
	private void prepareModules() throws IOException,RequestException {
		String[] resourceModules = new String[] { options.getInitModule(), options.getUrisModule(),
				options.getProcessModule(), options.getPreBatchModule(), options.getPostBatchModule() };
		String modulesDatabase = options.getModulesDatabase();
		LOG.log(Level.INFO, "checking modules, database: {0}", modulesDatabase);
		Session session = contentSource.newSession(modulesDatabase);
		InputStream is = null;
		Content c = null;
		ContentCreateOptions opts = ContentCreateOptions.newTextInstance();
		try {
			for (String resourceModule : resourceModules) {
				if (resourceModule == null || resourceModule.toUpperCase().endsWith("|ADHOC")) {
					continue;
				}

				// Start by checking install flag.
				if (!options.isDoInstall()) {
					LOG.log(Level.INFO, "Skipping module installation: {0}", resourceModule);
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
						LOG.log(Level.WARNING, "looking for {0} as resource", resourceModule);
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
			LOG.log(Level.SEVERE, "error while reading modules {0}", e.getMessage());
			throw e;
		} catch (RequestException e) {
			LOG.log(Level.SEVERE, "error while loading modules {0}", e.getMessage());
			throw e;
		} finally {
			session.close();
			if (null != is) {
				try {
					is.close();
				} catch (IOException ioe) {
					LOG.log(Level.SEVERE, "Couldn't close the stream", ioe);
				}
			}
		}
	}

	protected void registerStatusInfo() {
		Session session = contentSource.newSession();
		AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_0_9_ML + DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER + 
                "let $status := \n" + " xdmp:server-status(xdmp:host(), xdmp:server())\n" + 
                "let $modules := $status/mlss:modules\n" + 
                "let $root := $status/mlss:root\n" + 
                "return (data($modules), data($root))");
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

		LOG.log(Level.INFO, "Configured modules db: {0}", options.getModulesDatabase());
		LOG.log(Level.INFO, "Configured modules xdbc root: {0}", options.getXDBC_ROOT());
		LOG.log(Level.INFO, "Configured modules root: {0}", options.getModuleRoot());
		LOG.log(Level.INFO, "Configured uri module: {0}", options.getUrisModule());
		LOG.log(Level.INFO, "Configured uri file: {0}", options.getUrisFile());
		LOG.log(Level.INFO, "Configured uri loader: {0}", options.getUrisLoaderClass());
		LOG.log(Level.INFO, "Configured process module: {0}", options.getProcessModule());
		LOG.log(Level.INFO, "Configured process task: {0}", options.getProcessTaskClass());
		LOG.log(Level.INFO, "Configured pre batch module: {0}", options.getPreBatchModule());
		LOG.log(Level.INFO, "Configured pre batch task: {0}", options.getPreBatchTaskClass());
		LOG.log(Level.INFO, "Configured post batch module: {0}", options.getPostBatchModule());
		LOG.log(Level.INFO, "Configured post batch task: {0}", options.getPostBatchTaskClass());
		LOG.log(Level.INFO, "Configured init module: {0}", options.getInitModule());
		LOG.log(Level.INFO, "Configured init task: {0}", options.getInitTaskClass());
		LOG.log(Level.INFO, "Configured batch size: {0}", options.getBatchSize());
		LOG.log(Level.INFO, "Configured failonError: {0}", options.isFailOnError());

		logProperties();
	}
	
	protected void logProperties(){
		for (Entry<Object, Object> e : properties.entrySet()) {
			if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
				LOG.log(Level.INFO, "Loaded property {0}={1}", new Object[] { e.getKey(), e.getValue() });
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
		if (options.getUrisModule() != null && options.getUrisModule().trim().length() > 0) {
			loader = new QueryUrisLoader();
		} else if (options.getUrisFile() != null && options.getUrisFile().trim().length() > 0) {
			loader = new FileUrisLoader();
		} else if (options.getUrisLoaderClass() != null) {
			loader = options.getUrisLoaderClass().newInstance();
		} else {
			throw new IllegalArgumentException("Cannot find URIS-MODULE, URIS-FILE or URIS-LOADER");
		}

		loader.setOptions(options);
		loader.setContentSource(contentSource);
		loader.setCollection(collection);
		loader.setProperties(properties);
		return loader;
	}

	private int populateQueue() throws Exception {
		LOG.info("populating queue");
		TaskFactory tf = new TaskFactory(this);
		UrisLoader urisLoader = getUriLoader();
		int total = -1;
		int count = 0;
		try {
			// run init task
			runInitTask(tf);

			urisLoader.open();
			if (urisLoader.getBatchRef() != null) {
				properties.put(Manager.URIS_BATCH_REF, urisLoader.getBatchRef());
				LOG.log(Level.INFO, "URIS_BATCH_REF: {0}", urisLoader.getBatchRef());
			}

			total = urisLoader.getTotalCount();
			LOG.log(Level.INFO, "expecting total {0}", total);
			if (total <= 0) {
				LOG.info("nothing to process");
				stop();
				return 0;
			}

			// run pre-batch task, if present.
			runPreBatchTask(tf);

			// now start process tasks
			monitor.setTaskCount(total);
			monitorThread.start();

			// this may return millions of items:
			// try to be memory-efficient
			long lastMessageMillis = System.currentTimeMillis();
			long freeMemory;
			boolean isFirst = true;
			// char primitives use less memory than strings
			// arrays use less memory than lists or queues
			char[][] urisArray = new char[total][];

			String uri;
			count = 0;
			while (urisLoader.hasNext() && null != pool) {
				uri = urisLoader.next();

				if (count >= urisArray.length) {
					throw new ArrayIndexOutOfBoundsException("received more than " + total + " results: " + uri);
				}

				if (uri == null || uri.trim().length() == 0) {
					continue;
				}

				// we want to test the work module immediately,
				// but we also want to ensure that
				// all uris in queue as quickly as possible
				if (isFirst) {
					isFirst = false;
					completionService.submit(tf.newProcessTask(new String[] { uri },options.isFailOnError()));
					urisArray[count] = null;
					LOG.log(Level.INFO, "received first uri: {0}", uri);
				} else {
					urisArray[count] = uri.toCharArray();
				}
				count++;

				if (0 == count % 25000) {
					LOG.log(Level.INFO, "received {0}/{1}: {2}", new Object[] { count, total, uri });

					if (System.currentTimeMillis() - lastMessageMillis > (1000 * 4)) {
						LOG.warning("Slow receive!" + " Consider increasing max heap size" + " and using -XX:+UseConcMarkSweepGC");
						freeMemory = Runtime.getRuntime().freeMemory();
						LOG.log(Level.INFO, "free memory: {0} MiB", (freeMemory / (1024 * 1024)));
					}
					lastMessageMillis = System.currentTimeMillis();
				}

			}

			LOG.log(Level.INFO, "received {0}/{1}", new Object[] { count, total });
			// done with result set - close session to close everything
			if (null != urisLoader) {
				urisLoader.close();
			}

			if (count < total) {
				LOG.log(Level.WARNING,"Resetting total uri count to {0}. Ignore if URIs are loaded from a file that contains blank lines.", count);
				monitor.setTaskCount(total = count);
			}

			// start with 1 not 0 because we already queued result 0
			List<String> ulist = new ArrayList<String>(options.getBatchSize());
			for (int i = 1; i < urisArray.length; i++) {
				// check pool occasionally, for fast-fail
				if (null == pool) {
					break;
				}
				if (urisArray[i] == null || urisArray[i].length == 0) {
					continue;
				}

				uri = new String(urisArray[i]);
				ulist.add(uri);
				urisArray[i] = null;
				if (ulist.size() >= options.getBatchSize() || i >= (urisArray.length - 1)) {
					String[] uris = ulist.toArray(new String[ulist.size()]);
					ulist.clear();
					completionService.submit(tf.newProcessTask(uris, options.isFailOnError()));
				}
				String msg = "queued " + i + "/" + total + ": " + uri;
				if (0 == i % 50000) {
					LOG.info(msg);
					freeMemory = Runtime.getRuntime().freeMemory();
					if (freeMemory < (16 * 1024 * 1024)) {
						LOG.log(Level.WARNING, "free memory: {0} MiB", (freeMemory / (1024 * 1024)));
					}
					lastMessageMillis = System.currentTimeMillis();
				} else {
					LOG.finest(msg);
				}
				if (i > total) {
					LOG.log(Level.WARNING, "expected {0}, got {1}", new Object[] { total, i });
					LOG.warning("check your uri module!");
				}
			}
			LOG.log(Level.INFO, "queued {0}/{1}", new Object[] { urisArray.length, total });
			urisArray = null;
			pool.shutdown();

		} catch (Exception exc) {
			stop();
			throw exc;
		} finally {
			if (null != urisLoader) {
				urisLoader.close();
			}
		}

		if(total == count){
			LOG.log(Level.INFO, "queue is populated with {0} tasks", total);
		}else{
			LOG.log(Level.WARNING,"queue is expected to be populated with {0} tasks, but only got {1} tasks.", new Object[]{total,count});
		}
		return count;
	}

	/**
     */
	public void stop() {
		LOG.info("cleaning up");
		if (null != pool) {
			List<Runnable> remaining = pool.shutdownNow();
			if (remaining.size() > 0) {
				LOG.log(Level.WARNING, "thread pool was shut down with {0} pending tasks", remaining.size());
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
	 * @param e
	 */
	public void stop(ExecutionException e) {
		this.execError=true;
		LOG.log(Level.SEVERE, "fatal error", e.getCause());
		LOG.warning("exiting due to fatal error");
		stop();
	}

}
