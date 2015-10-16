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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.marklogic.developer.Utilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.XdmItem;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * @author Colleen Whitney, MarkLogic Corporation
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class Manager implements Runnable {
	
	static public String VERSION = "2.1.3";
	static private Logger logger = getLogger();

	public class CallerBlocksPolicy implements RejectedExecutionHandler {

		private BlockingQueue<Runnable> queue;
		private boolean warning = false;

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.util.concurrent.RejectedExecutionHandler#rejectedExecution(java
		 * .lang.Runnable, java.util.concurrent.ThreadPoolExecutor)
		 */
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			if (null == queue) {
				queue = executor.getQueue();
			}
			try {
				// block until space becomes available
				if (!warning) {
					logger.info("queue is full: size = " + queue.size() + " (will only appear once)");
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

	public static String URIS_BATCH_REF = "URIS_BATCH_REF";
	public static String DEFAULT_BATCH_URI_DELIM = ";";

	protected static String versionMessage = "version " + VERSION + " on " + System.getProperty("java.version") + " ("+ System.getProperty("java.runtime.name") + ")";
	private static final String DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER = "declare namespace mlss = 'http://marklogic.com/xdmp/status/server'\n";
	private static final String XQUERY_VERSION_0_9_ML = "xquery version \"0.9-ml\"\n";
	protected static final String NAME = Manager.class.getName();

	protected URI connectionUri;
	protected String collection;
	protected Properties properties = new Properties();
	protected TransformOptions options = new TransformOptions();
	private ThreadPoolExecutor pool = null;
	protected ContentSource contentSource;
	private Monitor monitor;
	private Thread monitorThread;
	private CompletionService<String[]> completionService;

	static public Logger getLogger(){
		return Logger.getLogger("Corb2");
	}

	/**
	 * @param connectionUri
	 * @param collection
	 * @param modulePath
	 * @param uriListPath
	 */
	public Manager(URI connectionUri, String collection) {
		this.connectionUri = connectionUri;
		this.collection = collection;
	}

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		Manager tm = createManager(args);
		// now its time to start processing
		if (tm != null){
			tm.run();
		}
		System.exit(0);
	}

	public static Properties loadPropertiesFile(String filename) throws IOException {
		return loadPropertiesFile(filename, true);
	}

	public static Properties loadPropertiesFile(String filename, boolean excIfNotFound) throws IOException {
		Properties props = new Properties();
		if (filename != null && (filename = filename.trim()).length() > 0) {
			InputStream is = null;
			try {
				is = Manager.class.getResourceAsStream("/" + filename);
				if (is != null) {
					logger.info("Loading " + filename + " from classpath");
					props.load(is);
				} else {
					File f = new File(filename);
					if (f.exists() && !f.isDirectory()) {
						logger.info("Loading " + filename + " from filesystem");
						FileInputStream fis = null;
						try {
							fis = new FileInputStream(f);
							props.load(fis);
						} finally {
							if (null != fis) {
								fis.close();
							}
						}
					} else if (excIfNotFound) {
						throw new IllegalStateException("Unable to load properties file " + filename);
					}
				}
			} finally {
				if (null != is) {
					is.close();
				}
			}
		}
		return props;
	}

	static public String getAdhocQuery(String module) {
		InputStream is = null;
		InputStreamReader reader = null;
		StringWriter writer = null;
		try {
			is = TaskFactory.class.getResourceAsStream("/" + module);
			if (is == null) {
				File f = new File(module);
				if (f.exists() && !f.isDirectory()) {
					is = new FileInputStream(f);
				} else {
					throw new IllegalStateException("Unable to find adhoc query module " + module + " in classpath or filesystem");
				}
			}

			reader = new InputStreamReader(is);
			writer = new StringWriter();
			char[] buffer = new char[512];
			int n = 0;
			while (-1 != (n = reader.read(buffer))) {
				writer.write(buffer, 0, n);
			}
			writer.close();
			reader.close();
			return writer.toString().trim();
			
		} catch (IOException exc) {
			throw new IllegalStateException("Prolem reading adhoc query module " + module, exc);
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (Exception exc) {
			}
			try {
				if (reader != null)
					reader.close();
			} catch (Exception exc) {
			}
			try {
				if (is != null)
					is.close();
			} catch (Exception exc) {
			}
		}
	}

	public static Manager createManager(String[] args) throws URISyntaxException, IOException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		String propsFileName = System.getProperty("OPTIONS-FILE");
		Properties props = loadPropertiesFile(propsFileName);

		// gather inputs
		String connectionUri = getOption(args.length > 0 ? args[0] : null, "XCC-CONNECTION-URI", props);
		String collection = getOption(args.length > 1 ? args[1] : null, "COLLECTION-NAME", props);
		String processModule = getOption(args.length > 2 ? args[2] : null, "XQUERY-MODULE", props);
		String threadCount = getOption(args.length > 3 ? args[3] : null, "THREAD-COUNT", props);
		String urisModule = getOption(args.length > 4 ? args[4] : null, "URIS-MODULE", props);
		String moduleRoot = getOption(args.length > 5 ? args[5] : null, "MODULE-ROOT", props);
		String modulesDatabase = getOption(args.length > 6 ? args[6] : null, "MODULES-DATABASE", props);
		String install = getOption(args.length > 7 ? args[7] : null, "INSTALL", props);
		String processTask = getOption(args.length > 8 ? args[8] : null, "PROCESS-TASK", props);
		String preBatchModule = getOption(args.length > 9 ? args[9] : null, "PRE-BATCH-MODULE", props);
		String preBatchTask = getOption(args.length > 10 ? args[10] : null, "PRE-BATCH-TASK", props);
		String postBatchModule = getOption(args.length > 11 ? args[11] : null, "POST-BATCH-MODULE", props);
		String postBatchTask = getOption(args.length > 12 ? args[12] : null, "POST-BATCH-TASK", props);
		String exportFileDir = getOption(args.length > 13 ? args[13] : null, "EXPORT-FILE-DIR", props);
		String exportFileName = getOption(args.length > 14 ? args[14] : null, "EXPORT-FILE-NAME", props);
		String urisFile = getOption(args.length > 15 ? args[15] : null, "URIS-FILE", props);

		if (preBatchModule == null) preBatchModule = getOption(null, "PRE-BATCH-XQUERY-MODULE", props);
		if (postBatchModule == null) postBatchModule = getOption(null, "POST-BATCH-XQUERY-MODULE", props);

		String batchSize = getOption(null, "BATCH-SIZE", props);

		String initModule = getOption(null, "INIT-MODULE", props);
		String initTask = getOption(null, "INIT-TASK", props);

		String username = getOption(null, "XCC-USERNAME", props);
		String password = getOption(null, "XCC-PASSWORD", props);
		String host = getOption(null, "XCC-HOSTNAME", props);
		String port = getOption(null, "XCC-PORT", props);
		String dbname = getOption(null, "XCC-DBNAME", props);

		if (connectionUri == null && (username == null || password == null || host == null || port == null)) {
			System.err.println("XCC-CONNECTION-URI or XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME and XCC-PORT must be specified");
			usage();
			return null;
		}

		String decrypterClassName = getOption(null, "DECRYPTER", props);
		if (decrypterClassName != null) {
			Class<?> decrypterCls = Class.forName(decrypterClassName);
			if (AbstractDecrypter.class.isAssignableFrom(decrypterCls)) {
				AbstractDecrypter decrypter = (AbstractDecrypter) decrypterCls.newInstance();
				decrypter.init(props);
				connectionUri = decrypter.getConnectionURI(connectionUri, username, password, host, port, dbname);
			} else {
				throw new IllegalArgumentException("DECRYPTER must be of type com.marklogic.developer.corb.AbstractDecrypter");
			}
		} else if (connectionUri == null) {
			connectionUri = "xcc://" + username + ":" + password + "@" + host + ":" + port + (dbname != null ? "/" + dbname : "");
		}

		Manager tm = new Manager(new URI(connectionUri), collection != null ? collection : "");
		tm.setProperties(props); // Keep the properties around for the custom tasks
		// options
		TransformOptions options = tm.getOptions();
		if (moduleRoot != null) options.setModuleRoot(moduleRoot);
		if (processModule != null) options.setProcessModule(processModule);
		if (threadCount != null) options.setThreadCount(Integer.parseInt(threadCount));
		if (urisModule != null) options.setUrisModule(urisModule);
		if (modulesDatabase != null) options.setModulesDatabase(modulesDatabase);
		if (install != null && (install.equalsIgnoreCase("true") || install.equals("1")))  options.setDoInstall(true);
		if (urisFile != null) options.setUrisFile(urisFile);
		if (batchSize != null) options.setBatchSize(Integer.parseInt(batchSize));

		if (!props.containsKey("EXPORT-FILE-DIR") && exportFileDir != null) {
			props.put("EXPORT-FILE-DIR", exportFileDir);
		}
		if (!props.containsKey("EXPORT-FILE-NAME") && exportFileName != null) {
			props.put("EXPORT-FILE-NAME", exportFileName);
		}

		if (urisFile != null && urisFile.trim().length() > 0) {
			File f = new File(options.getUrisFile());
			if (!f.exists()) throw new IllegalArgumentException("Uris file " + urisFile + " not found");
		}

		// java class for processing individual tasks.
		// If specified, it is used instead of xquery module, but xquery module is
		// still required.
		if (processTask != null) {
			Class<?> processCls = Class.forName(processTask);
			if (Task.class.isAssignableFrom(processCls)) {
				processCls.newInstance(); // sanity check
				options.setProcessTaskClass((Class<? extends Task>) processCls.asSubclass(Task.class));
				if (ExportToFileTask.class.equals(processCls)) {
					options.setBatchSize(1);
				}
			} else {
				throw new IllegalArgumentException("PROCESS-TASK must be of type com.marklogic.developer.corb.Task");
			}
		}

		if (preBatchModule != null)
			options.setPreBatchModule(preBatchModule);
		if (preBatchTask != null) {
			Class<?> preBatchCls = Class.forName(preBatchTask);
			if (Task.class.isAssignableFrom(preBatchCls)) {
				preBatchCls.newInstance(); // sanity check
				options.setPreBatchTaskClass((Class<? extends Task>) preBatchCls.asSubclass(Task.class));
			} else {
				throw new IllegalArgumentException("PRE-BATCH-TASK must be of type com.marklogic.developer.corb.Task");
			}
		}

		if (postBatchModule != null)
			options.setPostBatchModule(postBatchModule);
		if (postBatchTask != null) {
			Class<?> postBatchCls = Class.forName(postBatchTask);
			if (Task.class.isAssignableFrom(postBatchCls)) {
				postBatchCls.newInstance(); // sanity check
				options.setPostBatchTaskClass((Class<? extends Task>) postBatchCls.asSubclass(Task.class));
			} else {
				throw new IllegalArgumentException("POST-BATCH-TASK must be of type com.marklogic.developer.corb.Task");
			}
		}

		if (props.containsKey("EXPORT-FILE-PART-EXT") && options.getPostBatchTaskClass() == null) {
			props.remove("EXPORT-FILE-PART-EXT");
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
			if (exportFile.exists())
				exportFile.delete();
		}

		if (initModule != null)
			options.setInitModule(initModule);
		if (initTask != null) {
			Class<?> initCls = Class.forName(initTask);
			if (Task.class.isAssignableFrom(initCls)) {
				initCls.newInstance(); // sanity check
				options.setInitTaskClass((Class<? extends Task>) initCls.asSubclass(Task.class));
			} else {
				throw new IllegalArgumentException("INIT-TASK must be of type com.marklogic.developer.corb.Task");
			}
		}

		if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
			throw new NullPointerException("PROCESS-TASK or XQUERY-MODULE must be specified");
		}
		return tm;
	}

	protected static String getOption(String argVal, String propName, Properties props) {
		if (argVal != null && argVal.trim().length() > 0) {
			return argVal.trim();
		} else if (System.getProperty(propName) != null && System.getProperty(propName).trim().length() > 0) {
			return System.getProperty(propName).trim();
		} else if (props.containsKey(propName) && props.getProperty(propName).trim().length() > 0) {
			String val = props.getProperty(propName).trim();
			props.remove(propName);
			return val;
		}
		return null;
	}

	protected void setProperties(Properties props) {
		this.properties = props;
	}

	public Properties getProperties() {
		return this.properties;
	}

	public TransformOptions getOptions() {
		return options;
	}

	public ContentSource getContentSource() {
		return this.contentSource;
	}

	protected static void usage() {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		logger.info(NAME + " starting: " + versionMessage);
		long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
		logger.info("maximum heap size = " + maxMemory + " MiB");

		RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
		List<String> arguments = RuntimemxBean.getInputArguments();
		int uIdx = -1;
		for (int i = 0; uIdx == -1 && i < arguments.size(); i++) {
			if (arguments.get(i).startsWith("-DXCC-CONNECTION-URI")) {
				uIdx = i;
			}
		}
		if (uIdx > -1) {
			arguments = new ArrayList<String>(arguments);
			arguments.remove(uIdx);
		}
		logger.info("runtime arguments = " + Utilities.join(arguments, " "));

		prepareContentSource();
		registerStatusInfo();
		prepareModules();
		monitorThread = preparePool();

		try {
			populateQueue();

			while (monitorThread.isAlive()) {
				try {
					monitorThread.join();
				} catch (InterruptedException e) {
					// reset interrupt status and continue
					Thread.interrupted();
					logger.log(Level.SEVERE,"interrupted while waiting for monitor", e);
				}
			}
			
      runPostBatchTask(); //post batch tasks
      logger.info("all done");
		} catch (XccException e) {
			logger.log(Level.SEVERE,e.getMessage(), e);
			stop();
			System.exit(1);
		} catch (Exception e) {
			logger.log(Level.SEVERE,"unexpected", e);
			stop();
			System.exit(1);
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
		Thread monitorThread = new Thread(monitor);
		return monitorThread;
	}

	/**
	 * @throws IOException
	 * @throws RequestException
	 * 
	 */
	private void prepareModules() {
		String[] resourceModules = new String[] { options.getInitModule(), options.getUrisModule(),
				options.getProcessModule(), options.getPreBatchModule(), options.getPostBatchModule() };
		String modulesDatabase = options.getModulesDatabase();
		logger.info("checking modules, database: " + modulesDatabase);
		Session session = contentSource.newSession(modulesDatabase);
		InputStream is = null;
		Content c = null;
		ContentCreateOptions opts = ContentCreateOptions.newTextInstance();
		try {
			for (int i = 0; i < resourceModules.length; i++) {
				if (resourceModules[i] == null || resourceModules[i].toUpperCase().endsWith("|ADHOC"))
					continue;

				// Start by checking install flag.
				if (!options.isDoInstall()) {
					logger.info("Skipping module installation: " + resourceModules[i]);
					continue;
				}
				// Next check: if XCC is configured for the filesystem, warn
				// user
				else if (options.getModulesDatabase().equals("")) {
					logger.warning("XCC configured for the filesystem: please install modules manually");
					return;
				}
				// Finally, if it's configured for a database, install.
				else {
					File f = new File(resourceModules[i]);
					// If not installed, are the specified files on the
					// filesystem?
					if (f.exists()) {
						String moduleUri = options.getModuleRoot() + f.getName();
						c = ContentFactory.newContent(moduleUri, f, opts);
					}
					// finally, check package
					else {
						logger.warning("looking for " + resourceModules[i] + " as resource");
						String moduleUri = options.getModuleRoot() + resourceModules[i];
						is = this.getClass().getResourceAsStream(resourceModules[i]);
						if (null == is) {
							throw new NullPointerException(resourceModules[i] + " could not be found on the filesystem,"
									+ " or in package resources");
						}
						c = ContentFactory.newContent(moduleUri, is, opts);
					}
					session.insertContent(c);
				}
			}
		} catch (IOException e) {
			logger.log(Level.SEVERE,"fatal error", e);
			throw new RuntimeException(e);
		} catch (RequestException e) {
			logger.log(Level.SEVERE,"fatal error", e);
			throw new RuntimeException(e);
		} finally {
			session.close();
			if (null != is) {
				try {
					is.close();
				} catch (IOException ioe) {
					logger.log(Level.SEVERE,"Couldn't close the stream", ioe);
				}
			}
		}
	}

	/**
     *
     */
	protected void prepareContentSource() {
		// logger.info("using content source " + connectionUri);
		try {
			// support SSL
			boolean ssl = connectionUri != null && connectionUri.getScheme() != null
					&& connectionUri.getScheme().equals("xccs");
			contentSource = ssl ? ContentSourceFactory.newContentSource(connectionUri, newTrustAnyoneOptions())
					: ContentSourceFactory.newContentSource(connectionUri);
		} catch (XccConfigException e) {
			logger.log(Level.SEVERE,"Problem creating content source. Check if URI is valid. If encrypted, check options are configured correctly.",e);
			throw new RuntimeException(e);
		} catch (KeyManagementException e) {
			logger.log(Level.SEVERE,"Problem creating content source with ssl", e);
			throw new RuntimeException(e);
		} catch (NoSuchAlgorithmException e) {
			logger.log(Level.SEVERE,"Problem creating content source with ssl", e);
			throw new RuntimeException(e);
		}
	}

	private void registerStatusInfo() {
		Session session = contentSource.newSession();
		AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_0_9_ML + DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
				+ "let $status := \n" + " xdmp:server-status(xdmp:host(), xdmp:server())\n"
				+ "let $modules := $status/mlss:modules\n" + "let $root := $status/mlss:root\n"
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

		// HACK
		if (options.getUrisModule() == null && options.getUrisFile() == null) {
			String urisFile = getOption(null, "URIS-FILE", properties);
			if (urisFile != null && (urisFile = urisFile.trim()).length() > 0) {
				options.setUrisFile(urisFile);
			}
		}
		// END HACK

		logger.info("Configured modules db: " + options.getModulesDatabase());
		logger.info("Configured modules xdbc root: " + options.getXDBC_ROOT());
		logger.info("Configured modules root: " + options.getModuleRoot());
		logger.info("Configured uri module: " + options.getUrisModule());
		logger.info("Configured uri file: " + options.getUrisFile());
		logger.info("Configured process module: " + options.getProcessModule());
		logger.info("Configured process task: " + options.getProcessTaskClass());
		logger.info("Configured pre batch module: " + options.getPreBatchModule());
		logger.info("Configured pre batch task: " + options.getPreBatchTaskClass());
		logger.info("Configured post batch module: " + options.getPostBatchModule());
		logger.info("Configured post batch task: " + options.getPostBatchTaskClass());
		logger.info("Configured init module: " + options.getInitModule());
		logger.info("Configured init task: " + options.getInitTaskClass());

		for (Entry<Object, Object> e : properties.entrySet()) {
			if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
				logger.info("Loaded property " + e.getKey() + "=" + e.getValue());
			}
		}
	}

	private void runInitTask(TaskFactory tf) throws Exception {
		Task initTask = tf.newInitTask();
		if (initTask != null) {
			logger.info("Running init Task");
			initTask.call();
		}
	}

	private void runPreBatchTask(TaskFactory tf) throws Exception {
		Task preTask = tf.newPreBatchTask();
		if (preTask != null) {
			logger.info("Running pre batch Task");
			preTask.call();
		}
	}

	private void runPostBatchTask() throws Exception {
		TaskFactory tf = new TaskFactory(this);
		Task postTask = tf.newPostBatchTask();
		if (postTask != null) {
			logger.info("Running post batch Task");
			postTask.call();
		}
	}

	private UrisLoader getUriLoader() {
		UrisLoader loader = null;
		if (options.getUrisModule() != null && options.getUrisModule().trim().length() > 0) {
			loader = new XQueryUrisLoader();
		} else if (options.getUrisFile() != null && options.getUrisFile().trim().length() > 0) {
			loader = new FileUrisLoader();
		} else {
			throw new IllegalArgumentException("Cannot find URIS-MODULE or URIS-FILE");
		}

		loader.setOptions(options);
		loader.setContentSource(contentSource);
		loader.setCollection(collection);
		loader.setProperties(properties);
		return loader;
	}

	private void populateQueue() throws Exception {
		logger.info("populating queue");
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
				logger.info("URIS_BATCH_REF: " + urisLoader.getBatchRef());
			}

			total = urisLoader.getTotalCount();
			logger.info("expecting total " + total);
			if (total <= 0) {
				logger.info("nothing to process");
				stop();
				return;
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

				if (uri == null || uri.trim().length() == 0)
					continue;

				// we want to test the work module immediately,
				// but we also want to ensure that
				// all uris in queue as quickly as possible
				if (isFirst) {
					isFirst = false;
					completionService.submit(tf.newProcessTask(new String[] { uri }));
					urisArray[count] = null;
					logger.info("received first uri: " + uri);
				} else {
					urisArray[count] = uri.toCharArray();
				}
				count++;

				if (0 == count % 25000) {
					logger.info("received " + count + "/" + total + ": " + uri);

					if (System.currentTimeMillis() - lastMessageMillis > (1000 * 4)) {
						logger.warning("Slow receive!" + " Consider increasing max heap size"
								+ " and using -XX:+UseConcMarkSweepGC");
						freeMemory = Runtime.getRuntime().freeMemory();
						logger.info("free memory: " + (freeMemory / (1024 * 1024)) + " MiB");
					}
					lastMessageMillis = System.currentTimeMillis();
				}

			}

			logger.info("received " + count + "/" + total);
			// done with result set - close session to close everything
			if (null != urisLoader) {
				urisLoader.close();
			}

			if (count < total) {
				logger.warning("Resetting total uri count to " + count
						+ ". Ignore if URIs are loaded from a file that contains blank lines.");
				monitor.setTaskCount(total = count);
			}

			// start with 1 not 0 because we already queued result 0
			List<String> ulist = new ArrayList<String>(options.getBatchSize());
			for (int i = 1; i < urisArray.length; i++) {
				// check pool occasionally, for fast-fail
				if (null == pool) {
					break;
				}
				if (urisArray[i] == null || urisArray[i].length == 0)
					continue;

				uri = new String(urisArray[i]);
				ulist.add(uri);
				urisArray[i] = null;
				if (ulist.size() >= options.getBatchSize() || i >= (urisArray.length - 1)) {
					String[] uris = ulist.toArray(new String[ulist.size()]);
					ulist.clear();
					completionService.submit(tf.newProcessTask(uris));
				}
				String msg = "queued " + i + "/" + total + ": " + uri;
				if (0 == i % 50000) {
					logger.info(msg);
					freeMemory = Runtime.getRuntime().freeMemory();
					if (freeMemory < (16 * 1024 * 1024)) {
						logger.warning("free memory: " + (freeMemory / (1024 * 1024)) + " MiB");
					}
					lastMessageMillis = System.currentTimeMillis();
				} else {
					logger.finest(msg);
				}
				if (i > total) {
					logger.warning("expected " + total + ", got " + i);
					logger.warning("check your uri module!");
				}
			}
			logger.info("queued " + urisArray.length + "/" + total);
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

		// if the pool went away, the monitor stopped it: bail out.
		if (null == pool) {
			return;
		}

		assert total == count;
		logger.fine("queue is populated with " + total + " tasks");
	}

	/**
	 * @param e
	 */
	public void stop() {
		logger.info("cleaning up");
		if (null != pool) {
			List<Runnable> remaining = pool.shutdownNow();
			if (remaining.size() > 0) {
				logger.warning("thread pool was shut down with " + remaining.size() + " pending tasks");
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
		logger.log(Level.SEVERE,"fatal error", e.getCause());
		logger.warning("exiting due to fatal error");
		stop();
	}

	protected static SecurityOptions newTrustAnyoneOptions() throws KeyManagementException, NoSuchAlgorithmException {
		TrustManager[] trust = new TrustManager[] { new X509TrustManager() {
			public X509Certificate[] getAcceptedIssuers() {
				return new X509Certificate[0];
			}

			/**
			 * @throws CertificateException
			 */
			public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
				// no exception means it's okay
			}

			/**
			 * @throws CertificateException
			 */
			public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
				// no exception means it's okay
			}
		} };

		SSLContext sslContext = SSLContext.getInstance("SSLv3");
		sslContext.init(null, trust, null);
		return new SecurityOptions(sslContext);
	}
}
