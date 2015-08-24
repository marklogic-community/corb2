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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import com.marklogic.developer.SimpleLogger;
import com.marklogic.developer.Utilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;

public class ModuleExecutor {

	public static String VERSION = "2015-07-27";

	protected static String versionMessage = "version " + VERSION + " on "
			+ System.getProperty("java.version") + " ("
			+ System.getProperty("java.runtime.name") + ")";

	private static final String DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER = "declare namespace mlss = 'http://marklogic.com/xdmp/status/server'\n";

	private static final String XQUERY_VERSION_0_9_ML = "xquery version \"0.9-ml\"\n";

	protected static final String NAME = ModuleExecutor.class.getName();

	protected URI connectionUri;

	protected Properties properties = new Properties();

	protected TransformOptions options = new TransformOptions();

	protected ContentSource contentSource;

	private Session session;
	private ResultSequence res;

	private static byte[] EMPTY_BYTE_ARRAY = new byte[0];
	protected static byte[] NEWLINE = "\n".getBytes();

	static public SimpleLogger logger;
	static {
		logger = SimpleLogger.getSimpleLogger();
		Properties props = new Properties();
		props.setProperty("LOG_LEVEL", "INFO");
		props.setProperty("LOG_HANDLER", "CONSOLE");
		logger.configureLogger(props);
	}

	/**
	 * @param connectionUri
	 * @param collection
	 * @param modulePath
	 * @param uriListPath
	 */
	public ModuleExecutor(URI connectionUri, String collection) {
		this.connectionUri = connectionUri;
	}

	/**
	 * @param args
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public static void main(String[] args) throws URISyntaxException,
			IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, Exception {
		ModuleExecutor moduleExecutor = createExecutor(args);

		if (moduleExecutor != null)
			moduleExecutor.run();
	}

	public static ModuleExecutor createExecutor(String[] args)
			throws URISyntaxException, IOException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		String propsFileName = System.getProperty("OPTIONS-FILE");
		Properties props = Manager.loadPropertiesFile(propsFileName);

		// gather inputs
		String connectionUri = getOption(args.length > 0 ? args[0] : null,
				"XCC-CONNECTION-URI", props);
		String processModule = getOption(args.length > 1 ? args[1] : null,
				"XQUERY-MODULE", props);
		String moduleRoot = getOption(args.length > 2 ? args[2] : null,
				"MODULE-ROOT", props);
		String modulesDatabase = getOption(args.length > 3 ? args[3] : null,
				"MODULES-DATABASE", props);
		String processTask = getOption(args.length > 4 ? args[4] : null,
				"PROCESS-TASK", props);
		String exportFileDir = getOption(args.length > 5 ? args[5] : null,
				"EXPORT-FILE-DIR", props);
		String exportFileName = getOption(args.length > 6 ? args[6] : null,
				"EXPORT-FILE-NAME", props);

		String username = getOption(null, "XCC-USERNAME", props);
		String password = getOption(null, "XCC-PASSWORD", props);
		String host = getOption(null, "XCC-HOSTNAME", props);
		String port = getOption(null, "XCC-PORT", props);
		String dbname = getOption(null, "XCC-DBNAME", props);

		if (connectionUri == null
				&& (username == null || password == null || host == null || port == null)) {
			System.err
					.println("XCC-CONNECTION-URI or XCC-USERNAME, XCC-PASSWORD, XCC-HOSTNAME and XCC-PORT must be specified");
			usage();
			return null;
		}

		String decrypterClassName = getOption(null, "DECRYPTER", props);
		if (decrypterClassName != null) {
			Class<?> decrypterCls = Class.forName(decrypterClassName);
			if (AbstractDecrypter.class.isAssignableFrom(decrypterCls)) {
				AbstractDecrypter decrypter = (AbstractDecrypter) decrypterCls
						.newInstance();
				decrypter.init(props);
				connectionUri = decrypter.getConnectionURI(connectionUri,
						username, password, host, port, dbname);
			} else {
				throw new IllegalArgumentException(
						"DECRYPTER must be of type com.marklogic.developer.corb.AbstractDecrypter");
			}
		} else if (connectionUri == null) {
			connectionUri = "xcc://" + username + ":" + password + "@" + host
					+ ":" + port + (dbname != null ? "/" + dbname : "");
		}

		ModuleExecutor moduleExecutor = new ModuleExecutor(new URI(connectionUri), "");
		moduleExecutor.setProperties(props); // Keep the properties around for
												// the custom tasks
		// options
		TransformOptions options = moduleExecutor.getOptions();
		if (moduleRoot != null) {
			options.setModuleRoot(moduleRoot);
		}
		if (processModule != null) {
			options.setProcessModule(processModule);
		}
		if (modulesDatabase != null) {
			options.setModulesDatabase(modulesDatabase);
		}
		if (!props.containsKey("EXPORT-FILE-DIR") && exportFileDir != null) {
			props.put("EXPORT-FILE-DIR", exportFileDir);
		}
		if (!props.containsKey("EXPORT-FILE-NAME") && exportFileName != null) {
			props.put("EXPORT-FILE-NAME", exportFileName);
		}

		// java class for processing individual tasks.
		// If specified, it is used instead of xquery module, but xquery module
		// is still required.
		if (processTask != null) {
			Class<?> processCls = Class.forName(processTask);
			if (Task.class.isAssignableFrom(processCls)) {
				processCls.newInstance(); // sanity check
				options.setProcessTaskClass((Class<? extends Task>) processCls
						.asSubclass(Task.class));
			} else {
				throw new IllegalArgumentException(
						"PROCESS-TASK must be of type com.marklogic.developer.corb.Task");
			}
		}

		if (exportFileDir != null) {
			File dirFile = new File(exportFileDir);
			if (dirFile.exists() && dirFile.canWrite()) {
				options.setExportFileDir(exportFileDir);
			} else {
				throw new IllegalArgumentException(
						"Cannot write to export folder " + exportFileDir);
			}
		}
		// delete the export file if it exists
		if (exportFileName != null) {
			File exportFile = new File(exportFileDir, exportFileName);
			if (exportFile.exists())
				exportFile.delete();
		}
		if (null == options.getProcessTaskClass()
				&& null == options.getProcessModule()) {
			throw new NullPointerException("PROCESS-TASK must be specified");
		}
		return moduleExecutor;
	}

	protected static String getOption(String argVal, String propName,
			Properties props) {
		if (argVal != null && argVal.trim().length() > 0) {
			return argVal.trim();
		} else if (System.getProperty(propName) != null
				&& System.getProperty(propName).trim().length() > 0) {
			return System.getProperty(propName).trim();
		} else if (props.containsKey(propName)
				&& props.getProperty(propName).trim().length() > 0) {
			String val = props.getProperty(propName).trim();
			props.remove(propName);
			return val;
		}
		return null;
	}

	protected static void usage() {
		PrintStream err = System.err;
		err.println("usage 1:");
		err.println("\t"
				+ NAME
				+ " xcc://user:password@host:port/[ database ]"
				+ " xquery-module [module-root [modules-database [process-task [ export-file-dir [ export-file-name"
				+ " ] ] ] ] ] ");
		err.println("\nusage 2:");
		err.println("\t"
				+ "-DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ]"
				+ " -DXQUERY-MODULE=module-name.xqy"
				+ " -DPROCESS-TASK=com.marklogic.developer.corb.ExportBatchToFileTask"
				+ " -D... " + NAME);
		err.println("\nusage 3:");
		err.println("\t" + "-DOPTIONS-FILE=myjob.properties " + NAME);
		err.println("\nusage 4:");
		err.println("\t" + "-DOPTIONS-FILE=myjob.properties " + NAME
				+ " xcc://user:password@host:port/[ database ]");
	}

	public void run() throws CorbException {
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

		try {
			RequestOptions opts = new RequestOptions();
			opts.setCacheResult(false);
			session = contentSource.newSession();
			Request req = null;
			List<String> propertyNames = new ArrayList<String>(
					properties.stringPropertyNames());
			propertyNames.addAll(System.getProperties().stringPropertyNames());
			if (options.getProcessModule().toUpperCase().endsWith(".SJS|ADHOC")
					|| options.getProcessModule().toUpperCase()
							.endsWith(".JS|ADHOC")) {
				String queryPath = options.getProcessModule().substring(0,
						options.getProcessModule().indexOf('|'));
				String adhocQuery = TaskFactory.getAdhocQuery(queryPath);
				if (adhocQuery == null || (adhocQuery.length() == 0)) {
					throw new IllegalStateException(
							"Unable to read adhoc query " + queryPath
									+ " from classpath or filesystem");
				}
				logger.info("invoking adhoc javascript xquery module "
						+ queryPath);
				StringBuffer sb = new StringBuffer();
				sb.append("xdmp:javascript-eval('");
				sb.append(adhocQuery);
				sb.append("',(");
				int varCount = 0;

				for (String propName : propertyNames) {
					if (propName.startsWith("XQUERY-MODULE.")) {
						String varName = propName.substring("XQUERY-MODULE."
								.length());
						String value = getProperty(propName);
						if (value != null) {
							if (varCount > 0)
								sb.append(",");
							sb.append("\"" + varName + "\"").append(
									",\"" + value + "\"");
							varCount++;
						}
					}
				}
				sb.append("))");

				req = session.newAdhocQuery(sb.toString());

			} else {
				if (options.getProcessModule().toUpperCase().endsWith("|ADHOC")) {
					String queryPath = options.getProcessModule().substring(0,
							options.getProcessModule().indexOf('|'));

					String adhocQuery = getAdhocQuery(queryPath);
					if (adhocQuery == null || (adhocQuery.length() == 0)) {
						throw new IllegalStateException(
								"Unable to read adhoc query " + queryPath
										+ " from classpath or filesystem");
					}
					logger.info("invoking adhoc xquery module " + queryPath);
					req = session.newAdhocQuery(adhocQuery);
				} else {
					String root = options.getModuleRoot();
					if (!root.endsWith("/"))
						root = root + "/";

					String module = options.getProcessModule();
					if (module.startsWith("/") && module.length() > 1)
						module = module.substring(1);

					String modulePath = root + module;
					logger.info("invoking xquery module " + modulePath);
					req = session.newModuleInvoke(modulePath);
				}
				// NOTE: collection will be treated as a CWSV
				// req.setNewStringVariable("URIS", collection);
				// TODO support DIRECTORY as type
				req.setNewStringVariable("TYPE",
						TransformOptions.COLLECTION_TYPE);
				req.setNewStringVariable("PATTERN", "[,\\s]+");

				// custom inputs
				for (String propName : propertyNames) {
					if (propName.startsWith("XQUERY-MODULE.")) {
						String varName = propName.substring("XQUERY-MODULE."
								.length());
						String value = getProperty(propName);
						if (value != null)
							req.setNewStringVariable(varName, value);
					}
				}
				req.setOptions(opts);
			}
			res = session.submitRequest(req);

			if (getProperty("PROCESS-TASK") != null
					&& getProperty("PROCESS-TASK")
							.equals("com.marklogic.developer.corb.ExportBatchToFileTask")) {
				logger.info("Writing output to file");
				writeToFile(res);
			}

			logger.info("Done");
		} catch (RequestException exc) {
			throw new CorbException("While invoking XQuery Module", exc);
		} catch (IOException exc) {
			throw new CorbException("While trying to write output to file", exc);
		} catch (Exception exd) {
			throw new CorbException("While invoking XCC...", exd);
		}
	}

	protected void prepareContentSource() {
		// logger.info("using content source " + connectionUri);
		try {
			System.out.println("connectionUri=" + connectionUri);
			System.out.println("connectionUri.getScheme()="
					+ connectionUri.getScheme());
			// support SSL
			boolean ssl = connectionUri != null
					&& connectionUri.getScheme() != null
					&& connectionUri.getScheme().equals("xccs");
			contentSource = ssl ? ContentSourceFactory.newContentSource(
					connectionUri, newTrustAnyoneOptions())
					: ContentSourceFactory.newContentSource(connectionUri);
		} catch (XccConfigException e) {
			logger.logException(
					"Problem creating content source. Check if URI is valid. If encrypted, check options are configured correctly.",
					e);
			throw new RuntimeException(e);
		} catch (KeyManagementException e) {
			logger.logException("Problem creating content source with ssl", e);
			throw new RuntimeException(e);
		} catch (NoSuchAlgorithmException e) {
			logger.logException("Problem creating content source with ssl", e);
			throw new RuntimeException(e);
		}
	}

	protected static SecurityOptions newTrustAnyoneOptions()
			throws KeyManagementException, NoSuchAlgorithmException {
		TrustManager[] trust = new TrustManager[] { new X509TrustManager() {
			public X509Certificate[] getAcceptedIssuers() {
				return new X509Certificate[0];
			}

			/**
			 * @throws CertificateException
			 */
			public void checkClientTrusted(X509Certificate[] certs,
					String authType) throws CertificateException {
				// no exception means it's okay
			}

			/**
			 * @throws CertificateException
			 */
			public void checkServerTrusted(X509Certificate[] certs,
					String authType) throws CertificateException {
				// no exception means it's okay
			}
		} };

		SSLContext sslContext = SSLContext.getInstance("SSLv3");
		sslContext.init(null, trust, null);
		return new SecurityOptions(sslContext);
	}

	private void registerStatusInfo() {
		Session session = contentSource.newSession();
		AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_0_9_ML
				+ DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
				+ "let $status := \n"
				+ " xdmp:server-status(xdmp:host(), xdmp:server())\n"
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
		logger.info("Configured modules db: " + options.getModulesDatabase());
		logger.info("Configured modules root: " + options.getModuleRoot());
		logger.info("Configured process module: " + options.getProcessModule());
		logger.info("Configured process task: " + options.getProcessTaskClass());

		for (Entry<Object, Object> e : properties.entrySet()) {
			if (e.getKey() != null
					&& !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
				logger.info("Loaded property " + e.getKey() + "="
						+ e.getValue());
			}
		}
	}

	private String getAdhocQuery(String module) {
		InputStream is = null;
		InputStreamReader reader = null;
		StringWriter writer = null;
		try {
			is = ModuleExecutor.class.getResourceAsStream("/" + module);

			if (is == null) {
				File f = new File(module);
				if (f.exists() && !f.isDirectory()) {
					is = new FileInputStream(f);
				} else {
					throw new IllegalStateException(
							"Unable to find adhoc query module " + module
									+ " in classpath or filesystem");
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
			throw new IllegalStateException(
					"Problem reading adhoc query module " + module, exc);
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

	/**
	 * @throws IOException
	 * @throws RequestException
	 * 
	 */
	private void prepareModules() {
		String[] resourceModules = new String[] { options.getInitModule(),
				options.getUrisModule(), options.getProcessModule(),
				options.getPreBatchModule(), options.getPostBatchModule() };
		String modulesDatabase = options.getModulesDatabase();
		logger.info("checking modules, database: " + modulesDatabase);
		Session session = contentSource.newSession(modulesDatabase);
		InputStream is = null;
		Content c = null;
		ContentCreateOptions opts = ContentCreateOptions.newTextInstance();
		try {
			for (int i = 0; i < resourceModules.length; i++) {
				if (resourceModules[i] == null
						|| resourceModules[i].toUpperCase().endsWith("|ADHOC"))
					continue;

				// Start by checking install flag.
				if (!options.isDoInstall()) {
					logger.info("Skipping module installation: "
							+ resourceModules[i]);
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
						String moduleUri = options.getModuleRoot()
								+ f.getName();
						c = ContentFactory.newContent(moduleUri, f, opts);
					}
					// finally, check package
					else {
						logger.warning("looking for " + resourceModules[i]
								+ " as resource");
						String moduleUri = options.getModuleRoot()
								+ resourceModules[i];
						is = this.getClass().getResourceAsStream(
								resourceModules[i]);
						if (null == is) {
							throw new NullPointerException(resourceModules[i]
									+ " could not be found on the filesystem,"
									+ " or in package resources");
						}
						c = ContentFactory.newContent(moduleUri, is, opts);
					}
					session.insertContent(c);
				}
			}
		} catch (IOException e) {
			logger.logException("fatal error", e);
			throw new RuntimeException(e);
		} catch (RequestException e) {
			logger.logException("fatal error", e);
			throw new RuntimeException(e);
		} finally {
			session.close();
			if (null != is) {
				try {
					is.close();
				} catch (IOException ioe) {
					logger.logException("Couldn't close the stream", ioe);
				}
			}
		}
	}

	public String getProperty(String key) {
		String val = System.getProperty(key);
		if (val == null || val.trim().length() == 0) {
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}

	private void writeToFile(ResultSequence seq) throws IOException,
			CorbException {
		if (seq == null || !seq.hasNext())
			return;
		BufferedOutputStream writer = null;
		try {
			String fileName = (String) this.properties.get("EXPORT-FILE-NAME");
			if (fileName == null || fileName.length() == 0) {
				throw new CorbException("Export file name must be specified");
			}
			File f = new File(fileName);
			f.getParentFile().mkdirs();
			writer = new BufferedOutputStream(new FileOutputStream(f));
			while (seq.hasNext()) {
				writer.write(getValueAsBytes(seq.next().getItem()));
				writer.write(NEWLINE);
			}
			writer.flush();
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
	}

	protected byte[] getValueAsBytes(XdmItem item) {
		if (item instanceof XdmBinary) {
			return ((XdmBinary) item).asBinaryData();
		} else if (item != null) {
			return item.asString().getBytes();
		} else {
			return EMPTY_BYTE_ARRAY;
		}
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

}
