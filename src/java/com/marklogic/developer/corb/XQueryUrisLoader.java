package com.marklogic.developer.corb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.marklogic.developer.SimpleLogger;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;

public class XQueryUrisLoader implements UrisLoader {
	TransformOptions options;	
	ContentSource cs;
	String collection;
	Properties properties;
	
	Session session;
	ResultSequence res;
	
	String batchRef;
	int total=0;
	
	SimpleLogger logger;
	
	String[] replacements = new String[0];
	
	public XQueryUrisLoader(){		
	}
	
	public void setOptions(TransformOptions options){
		this.options = options;
	}
	
	public void setContentSource(ContentSource cs){
		this.cs = cs;
	}
	
	public void setCollection(String collection){
		this.collection = collection;
	}
	
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public void open() throws CorbException{
		configureLogger();
		
        List<String> propertyNames = new ArrayList<String>(properties.stringPropertyNames());
		propertyNames.addAll(System.getProperties().stringPropertyNames());
		
		if(propertyNames.contains("URIS-REPLACE-PATTERN")){
			String pattern = getProperty("URIS-REPLACE-PATTERN").trim(); 
			replacements = pattern.split(",",-1);
			if(replacements.length % 2 != 0) throw new IllegalArgumentException("Invalid replacement pattern " + pattern);
		}
		
		try{
			RequestOptions opts = new RequestOptions();
			opts.setCacheResult(false);
	        // this should be a noop, but xqsync does it
	        opts.setResultBufferSize(0);
	        logger.info("buffer size = " + opts.getResultBufferSize()+ ", caching = " + opts.getCacheResult());
	        
			session = cs.newSession();
			Request req = null;
			if(options.getUrisModule().toUpperCase().endsWith(".SJS|ADHOC") || options.getUrisModule().toUpperCase().endsWith(".JS|ADHOC")){
				String queryPath = options.getUrisModule().substring(0, options.getUrisModule().indexOf('|'));
				String adhocQuery = TaskFactory.getAdhocQuery(queryPath);
				if(adhocQuery == null || (adhocQuery.length() == 0)){
    				throw new IllegalStateException("Unable to read adhoc query "+queryPath+" from classpath or filesystem");
    			}
				logger.info("invoking adhoc javascript uris module " + queryPath);
				StringBuffer sb = new StringBuffer();
				sb.append("xdmp:javascript-eval('");
				sb.append(adhocQuery);
				sb.append("',(");
				int varCount=0;
				for(String propName:propertyNames){
		        	if(propName.startsWith("URIS-MODULE.")){
		        		String varName = propName.substring("URIS-MODULE.".length());
		        		String value = getProperty(propName);
		        		if(value != null){
		        			if(varCount > 0) sb.append(",");
		        			sb.append("\""+varName+"\"").append(",\""+value+"\"");
		        			varCount++;
		        		}
		        	}
		        }
				sb.append("))");
				
				req = session.newAdhocQuery(sb.toString());
			}else{
				if(options.getUrisModule().toUpperCase().endsWith("|ADHOC")){
					String queryPath = options.getUrisModule().substring(0, options.getUrisModule().indexOf('|'));
					String adhocQuery = TaskFactory.getAdhocQuery(queryPath);
					if(adhocQuery == null || (adhocQuery.length() == 0)){
	    				throw new IllegalStateException("Unable to read adhoc query "+queryPath+" from classpath or filesystem");
	    			}
					logger.info("invoking adhoc uris module " + queryPath);
					req = session.newAdhocQuery(adhocQuery);
				}else{
					String root = options.getModuleRoot();
					if(!root.endsWith("/")) root = root + "/";
					
					String module = options.getUrisModule();
					if(module.startsWith("/") && module.length() > 1) module = module.substring(1);
					
					String modulePath = root + module;
					logger.info("invoking uris module " + modulePath);
					req = session.newModuleInvoke(modulePath);
				}
		        // NOTE: collection will be treated as a CWSV
		        req.setNewStringVariable("URIS", collection);
		        // TODO support DIRECTORY as type
		        req.setNewStringVariable("TYPE", TransformOptions.COLLECTION_TYPE);
		        req.setNewStringVariable("PATTERN", "[,\\s]+");
		        
		        //custom inputs	        
				for(String propName:propertyNames){
		        	if(propName.startsWith("URIS-MODULE.")){
		        		String varName = propName.substring("URIS-MODULE.".length());
		        		String value = getProperty(propName);
		        		if(value != null) req.setNewStringVariable(varName, value);
		        	}
		        }
		        
		        req.setOptions(opts);
			}
	        res = session.submitRequest(req);
	        ResultItem next = res.next();
	        if(!(next.getItem().asString().matches("\\d+"))){
	        	batchRef = next.asString();
	        	next = res.next();
	        }
			
	        total = Integer.parseInt(next.getItem().asString());
		}catch(RequestException exc){
			throw new CorbException("While invoking Uris Module",exc);
		}
	}

	public String getBatchRef() {
		return this.batchRef;
	}

	public int getTotalCount() {
		return this.total;
	}
	
	public boolean hasNext() throws CorbException {
		return res != null & res.hasNext();
	}

	public String next() throws CorbException {
		String next = res.next().asString();
		for(int i=0; i<replacements.length-1; i=i+2){
			next = next.replaceAll(replacements[i], replacements[i+1]);
		}
		return next;
	}

	@Override
	public void close() {
		if(session != null){
			logger.info("closing uris session");
			try{
				if(res != null){
					res.close();
					res = null;
				}
			}finally{
				session.close();
				session = null;
			}
		}
		cleanup();
	}
	
	protected void cleanup(){
		//release 
		options=null;	
		cs=null;
		collection=null;
		properties=null;	
		batchRef=null;
		logger=null;
		replacements=null;
	}
	
	private void configureLogger() {
        if (logger == null) {
            logger = SimpleLogger.getSimpleLogger();
        }
        Properties props = new Properties();
        props.setProperty("LOG_LEVEL", options.getLogLevel());
        props.setProperty("LOG_HANDLER", options.getLogHandler());
        logger.configureLogger(props);
    }
	
	public String getProperty(String key){
		String val = System.getProperty(key);
		if(val == null || val.trim().length() == 0){
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}
}
