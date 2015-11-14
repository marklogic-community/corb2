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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;

/**
 * This class replaces RunXQuery.  It can run both XQuery and Javascript and when built, doesn't wrap the 
 * XCC connection jar as RunXQuery does.
 * @author matthew.heckel MarkLogic Corportation
 *
 */
public class ModuleExecutor extends AbstractManager{

	public static final String VERSION = Manager.VERSION;

	protected static String versionMessage = "version " + VERSION + " on "
			+ System.getProperty("java.version") + " ("
			+ System.getProperty("java.runtime.name") + ")";

	protected static final String NAME = ModuleExecutor.class.getSimpleName();

	private Session session;
	private ResultSequence res;

	private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	protected static final byte[] NEWLINE = "\n".getBytes();

	protected static Logger LOG = Logger.getLogger(ModuleExecutor.class.getCanonicalName());

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
		ModuleExecutor moduleExecutor = new ModuleExecutor();
		try{
			moduleExecutor.init(args);
		}catch(Exception exc){
			LOG.log(Level.SEVERE, "Error initializing ModuleExecutor",exc);
			System.exit(1);
		}
		//now we can start corb. 
		try{
			moduleExecutor.run();
			System.exit(0);
		} catch (XccConfigException | GeneralSecurityException exc) {
			LOG.log(Level.SEVERE, "Problem with XCC connection configuration.",exc);
			System.exit(1);
		}catch(Exception exc){
			LOG.log(Level.SEVERE, "Error while running ModuleExecutor",exc);
			System.exit(2);
		}
	}
	
	public ModuleExecutor() {
		
	}
	
	public void init(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException{	
		String propsFileName = System.getProperty("OPTIONS-FILE");
		loadPropertiesFile(propsFileName,true,this.properties);
		
		initDecrypter();
		initSSLConfig();
		
		initURI(args.length > 0 ? args[0] : null);
		
		initOptions(args);
	}
	
	protected void initOptions(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		String processModule = getOption(args.length > 1 ? args[1] : null,"PROCESS-MODULE");
		String moduleRoot = getOption(args.length > 2 ? args[2] : null,"MODULE-ROOT");
		String modulesDatabase = getOption(args.length > 3 ? args[3] : null,"MODULES-DATABASE");
		String exportFileDir = getOption(args.length > 4 ? args[4] : null,"EXPORT-FILE-DIR");
		String exportFileName = getOption(args.length > 5 ? args[5] : null,"EXPORT-FILE-NAME");
				
		if (moduleRoot != null) options.setModuleRoot(moduleRoot);
		if (processModule != null) options.setProcessModule(processModule);
		if (modulesDatabase != null) options.setModulesDatabase(modulesDatabase);
		
		if (null == options.getProcessModule()) {
			throw new NullPointerException("PROCESS-MODULE must be specified");
		}
		
		if (!this.properties.containsKey("EXPORT-FILE-DIR") && exportFileDir != null) {
			this.properties.put("EXPORT-FILE-DIR", exportFileDir);
		}
		if (!this.properties.containsKey("EXPORT-FILE-NAME") && exportFileName != null) {
			this.properties.put("EXPORT-FILE-NAME", exportFileName);
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
	}

	
	protected void usage() {
		PrintStream err = System.err;
		err.println("usage 1:");
		err.println("\t"
				+ NAME
				+ " xcc://user:password@host:port/[ database ]"
				+ " process-module [module-root [modules-database [ export-file-name ] ] ]");
		err.println("\nusage 2:");
		err.println("\t"
				+ "-DXCC-CONNECTION-URI=xcc://user:password@host:port/[ database ]"
				+ " -DPROCESS-MODULE=module-name.xqy"
				+ " -D... " + NAME);
		err.println("\nusage 3:");
		err.println("\t" + "-DOPTIONS-FILE=myjob.properties " + NAME);
		err.println("\nusage 4:");
		err.println("\t" + "-DOPTIONS-FILE=myjob.properties " + NAME
				+ " xcc://user:password@host:port/[ database ]");
	}

	private void registerStatusInfo() {
		Session session = contentSource.newSession();
		AdhocQuery q = session.newAdhocQuery(Manager.XQUERY_VERSION_0_9_ML
				+ Manager.DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
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
		LOG.info("Configured modules db: " + options.getModulesDatabase());
		LOG.info("Configured modules root: " + options.getModuleRoot());
		LOG.info("Configured process module: " + options.getProcessModule());

		for (Entry<Object, Object> e : properties.entrySet()) {
			if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
				LOG.info("Loaded property " + e.getKey() + "=" + e.getValue());
			}
		}
	}

	
	public void run() throws Exception {
		LOG.info(NAME + " starting: " + Manager.VERSION_MSG);
		long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
		LOG.info("maximum heap size = " + maxMemory + " MiB");

		logRuntimeArgs();

		prepareContentSource();
		registerStatusInfo();

		try {
			RequestOptions opts = new RequestOptions();
			opts.setCacheResult(false);
			session = contentSource.newSession();
			Request req = null;
			List<String> propertyNames = new ArrayList<String>(properties.stringPropertyNames());
			propertyNames.addAll(System.getProperties().stringPropertyNames());
			
			if (options.getProcessModule().toUpperCase().endsWith("|ADHOC")) {
			  String queryPath = options.getProcessModule().substring(0, options.getProcessModule().indexOf('|'));

			  String adhocQuery = getAdhocQuery(queryPath);
				if (adhocQuery == null || (adhocQuery.length() == 0)) {
					throw new IllegalStateException("Unable to read adhoc query " + queryPath+ " from classpath or filesystem");
				}
				LOG.info("invoking adhoc process module " + queryPath);
				req = session.newAdhocQuery(adhocQuery);
				if (queryPath.toUpperCase().endsWith(".SJS") || queryPath.toUpperCase().endsWith(".JS")) {
					opts.setQueryLanguage("javascript");
				}
			} else {
				String root = options.getModuleRoot();
				if (!root.endsWith("/")) {
					root = root + "/";
        }
				String module = options.getProcessModule();
				if (module.startsWith("/") && module.length() > 1) {
					module = module.substring(1);
        }
				String modulePath = root + module;
				LOG.info("invoking module " + modulePath);
				req = session.newModuleInvoke(modulePath);
			}

			// custom inputs
			for (String propName : propertyNames) {
				if (propName.startsWith("PROCESS-MODULE.")) {
				String varName = propName.substring("PROCESS-MODULE.".length());
					String value = getProperty(propName);
					if (value != null)
						req.setNewStringVariable(varName, value);
				}
			}
			
			req.setOptions(opts);
			res = session.submitRequest(req);

			writeToFile(res);

			LOG.info("Done");
		} catch (Exception exc) {
			LOG.log(Level.SEVERE, exc.getMessage());
			throw exc;
		} 
	}

	
	public String getProperty(String key) {
		String val = System.getProperty(key);
		if (val == null || val.trim().length() == 0) {
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}

	private void writeToFile(ResultSequence seq) throws IOException{
		if (seq == null || !seq.hasNext())
			return;
		String fileDir = getProperty("EXPORT-FILE-DIR");
		String fileName = getProperty("EXPORT-FILE-NAME");
		if (fileName == null || fileName.length() == 0) {
			return;
		}
		LOG.info("Writing output to file");
		
		BufferedOutputStream writer = null;
		try {		
			File f = new File(fileDir,fileName);
			if(f.getParentFile() != null) f.getParentFile().mkdirs();
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

}
