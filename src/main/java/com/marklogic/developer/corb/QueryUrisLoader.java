package com.marklogic.developer.corb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import java.util.logging.Level;

public class QueryUrisLoader implements UrisLoader {
	private static final int DEFAULT_MAX_OPTS_FROM_MODULE = 10;
	private static Pattern pattern = Pattern.compile("(PRE-BATCH-MODULE|PROCESS-MODULE|XQUERY-MODULE|POST-BATCH-MODULE)\\.[A-Za-z0-9]+=[A-Za-z0-9]+");
	
	TransformOptions options;
	ContentSource cs;
	String collection;
	Properties properties;

	Session session;
	ResultSequence res;

	String batchRef;
	int total = 0;

	String[] replacements = new String[0];

	private static final Logger LOG = Logger.getLogger(QueryUrisLoader.class.getSimpleName());

	public QueryUrisLoader() {
	}

	@Override
	public void setOptions(TransformOptions options) {
		this.options = options;
	}

	@Override
	public void setContentSource(ContentSource cs) {
		this.cs = cs;
	}

	@Override
	public void setCollection(String collection) {
		this.collection = collection;
	}

	@Override
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void open() throws CorbException {
		List<String> propertyNames = new ArrayList<String>(properties.stringPropertyNames());
		propertyNames.addAll(System.getProperties().stringPropertyNames());

		if (propertyNames.contains("URIS-REPLACE-PATTERN")) {
			String urisReplacePattern = getProperty("URIS-REPLACE-PATTERN");
			if (urisReplacePattern != null && urisReplacePattern.length() > 0) {
				replacements = urisReplacePattern.split(",", -1);
				if (replacements.length % 2 != 0) {
					throw new IllegalArgumentException("Invalid replacement pattern " + urisReplacePattern);
				}
			}
		}

		try {
			RequestOptions opts = new RequestOptions();
			opts.setCacheResult(false);
			// this should be a noop, but xqsync does it
			opts.setResultBufferSize(0);
			LOG.log(Level.INFO, "buffer size = {0}, caching = {1}",
					new Object[] { opts.getResultBufferSize(), opts.getCacheResult() });

			session = cs.newSession();
			Request req = null;
			
			if (options.getUrisModule().toUpperCase().endsWith("|ADHOC")) {
				String queryPath = options.getUrisModule().substring(0, options.getUrisModule().indexOf('|'));
				String adhocQuery = AbstractManager.getAdhocQuery(queryPath);
				if (adhocQuery == null || (adhocQuery.length() == 0)) {
					throw new IllegalStateException("Unable to read adhoc query " + queryPath + " from classpath or filesystem");
				}
				LOG.log(Level.INFO, "invoking adhoc uris module {0}", queryPath);
				req = session.newAdhocQuery(adhocQuery);
				if (queryPath.toUpperCase().endsWith(".SJS") || queryPath.toUpperCase().endsWith(".JS")) {
					opts.setQueryLanguage("javascript");
				}
			} else {
				String root = options.getModuleRoot();
				if (!root.endsWith("/")) {
					root += "/";
				}

				String module = options.getUrisModule();
				if (module.startsWith("/") && module.length() > 1) {
					module = module.substring(1);
				}

				String modulePath = root + module;
				LOG.log(Level.INFO, "invoking uris module {0}", modulePath);
				req = session.newModuleInvoke(modulePath);
			}
			// NOTE: collection will be treated as a CWSV
			req.setNewStringVariable("URIS", collection);
			// TODO support DIRECTORY as type
			req.setNewStringVariable("TYPE", TransformOptions.COLLECTION_TYPE);
			req.setNewStringVariable("PATTERN", "[,\\s]+");

			// custom inputs
			for (String propName : propertyNames) {
				if (propName.startsWith("URIS-MODULE.")) {
					String varName = propName.substring("URIS-MODULE.".length());
					String value = getProperty(propName);
					if (value != null) {
						req.setNewStringVariable(varName, value);
					}
				}
			}

			req.setOptions(opts);

			res = session.submitRequest(req);
			ResultItem next = res.next();
			
			int maxOpts = this.getMaxOptionsFromModule();
			for (int i=0; i < maxOpts && next != null && batchRef == null && !(next.getItem().asString().matches("\\d+")); i++){
				String value = next.getItem().asString();
				if (pattern.matcher(value).matches()) {
					int idx = value.indexOf('=');
					properties.put(value.substring(0, idx).replace("XQUERY-MODULE.", "PROCESS-MODULE."), value.substring(idx+1));
				} else {
					batchRef = value;
				}
				next = res.next();
			}
			
			try {
				total = Integer.parseInt(next.getItem().asString());
			} catch(NumberFormatException exc) {
				throw new CorbException("Uris module " + options.getUrisModule() + " does not return total URI count");
			}
		} catch (RequestException exc) {
			throw new CorbException("While invoking Uris Module", exc);
		}
	}

	@Override
	public String getBatchRef() {
		return this.batchRef;
	}

	@Override
	public int getTotalCount() {
		return this.total;
	}

	@Override
	public boolean hasNext() throws CorbException {
		return res != null && res.hasNext();
	}

	@Override
	public String next() throws CorbException {
		String next = res.next().asString();
		for (int i = 0; i < replacements.length - 1; i += 2) {
			next = next.replaceAll(replacements[i], replacements[i + 1]);
		}
		return next;
	}

	@Override
	public void close() {
		if (session != null) {
			LOG.info("closing uris session");
			try {
				if (res != null) {
					res.close();
					res = null;
				}
			} finally {
				session.close();
				session = null;
			}
		}
		cleanup();
	}

	protected void cleanup() {
		// release
		options = null;
		cs = null;
		collection = null;
		properties = null;
		batchRef = null;
		replacements = null;
	}

	public String getProperty(String key) {
		String val = System.getProperty(key);
		if (val == null && properties != null) {
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}
	
	private int getMaxOptionsFromModule(){
		int max = DEFAULT_MAX_OPTS_FROM_MODULE;
		try {
			String maxStr = getProperty("MAX_OPTS_FROM_MODULE");
			if (maxStr != null && maxStr.length() > 0) {
				max = Integer.parseInt(maxStr);
			}
		} catch(Exception exc){}
		return max;
	}
}
