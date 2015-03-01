package com.marklogic.developer.corb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.marklogic.developer.SimpleLogger;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
/**
 * 
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public abstract class AbstractTask implements Task{
	protected static String TRUE = "true";
	protected static String FALSE = "false";
	protected static byte[] NEWLINE = "\n".getBytes();
	private static byte[] EMPTY_BYTE_ARRAY = new byte[0];
	
	protected ContentSource cs;
	protected String moduleType;
	protected String moduleUri;
	protected Properties properties;
	protected String inputUri;
	
	protected String adhocQuery;
		
	protected int DEFAULT_RETRY_LIMIT=3;
	protected int DEFAULT_RETRY_INTERVAL=60;
	
	private int connectRetryCount=0;
		
	static protected SimpleLogger logger;
	static{
		logger = SimpleLogger.getSimpleLogger();
		Properties props = new Properties();
        props.setProperty("LOG_LEVEL", "INFO");
        props.setProperty("LOG_HANDLER", "CONSOLE");
        logger.configureLogger(props);
	}
		
    public void setContentSource(ContentSource cs){
    	this.cs = cs;
    }
    
    public void setModuleType(String moduleType){
    	this.moduleType=moduleType;
    }
    
    public void setModuleURI(String moduleUri){
    	this.moduleUri = moduleUri;
    }
    
    public void setAdhocQuery(String adhocQuery){
    	this.adhocQuery = adhocQuery;
    }
    
    public void setProperties(Properties properties){
    	this.properties = properties;
    }
    
	public void setInputURI(String inputUri) {
		this.inputUri = inputUri;
	}
	
	public Session newSession() {
        return cs.newSession();
    }
	
	protected String invokeModule() throws CorbException{
		if(moduleUri == null && adhocQuery == null) return null;
		
        Session session = null;
        ResultSequence seq = null;
		Thread.yield();// try to avoid thread starvation
        try {
            session = newSession();
            Request request = null;
            if(moduleUri != null){
            	request = session.newModuleInvoke(moduleUri);
            }else{
            	request = session.newAdhocQuery(adhocQuery);
            }
            request.setNewStringVariable("URI", inputUri);
            
            if(properties != null && properties.containsKey(Manager.URIS_BATCH_REF)){
            	request.setNewStringVariable(Manager.URIS_BATCH_REF, properties.getProperty(Manager.URIS_BATCH_REF));
            }
            
        	List<String> propertyNames = new ArrayList<String>();
        	if(properties != null) propertyNames.addAll(properties.stringPropertyNames());
        	propertyNames.addAll(System.getProperties().stringPropertyNames());
			
        	for(String propName:propertyNames){
            	if(moduleType != null && propName.startsWith(moduleType+".")){
            		String varName = propName.substring(moduleType.length()+1);
            		String value = getProperty(propName);
            		if(value != null) request.setNewStringVariable(varName, value);
            	}
            }
            
            Thread.yield();// try to avoid thread starvation
            seq = session.submitRequest(request);
            connectRetryCount=0;
            // no need to hold on to the session as results will be cached.
            session.close(); 
            Thread.yield();// try to avoid thread starvation
            
            processResult(seq);
            seq.close();
            Thread.yield();// try to avoid thread starvation
              
            return inputUri;
        }catch(Exception exc){
        	if(exc instanceof ServerConnectionException){
        		int retryLimit = this.getConnectRetryLimit();
        		int retryInterval = this.getConnectRetryInterval();
        		if(connectRetryCount < retryLimit){
        			connectRetryCount++;
        			logger.severe("Connection failed to Marklogic Server. Retrying attempt "+connectRetryCount+" after "+retryInterval+" seconds..: "+exc.getMessage()+" at URI: "+inputUri);
        			try{Thread.sleep(retryInterval*1000L);}catch(Exception exc2){}        			
        			return invokeModule();
        		}else{
        			throw new CorbException(exc.getMessage()+" at URI: "+inputUri,exc);
        		}
        	}else{
        		throw new CorbException(exc.getMessage()+" at URI: "+inputUri,exc);
        	}
        }finally {
        	if(null != session && !session.isClosed()) {
                session.close();
            }
        	if(null != seq && !seq.isClosed()){
        		seq.close();
        	}
            Thread.yield();// try to avoid thread starvation
        }
	}
	
	protected abstract String processResult(ResultSequence seq) throws CorbException;
	
	protected void cleanup(){
		//release resources
        cs=null;
    	moduleType=null;
    	moduleUri=null;
    	properties=null;
    	inputUri=null;
    	adhocQuery=null;
	}
		
	public String getProperty(String key){
		String val = System.getProperty(key);
		if((val == null || val.trim().length() == 0) && properties != null){
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}
	
	protected byte[] getValueAsBytes(XdmItem item){
		if(item instanceof XdmBinary){
			return ((XdmBinary) item).asBinaryData();
		}else if(item != null){
			return item.asString().getBytes();
		}else{
			return EMPTY_BYTE_ARRAY;
		}
	}
	
	private int getConnectRetryLimit(){
		int connectRetryLimit = -1;
		String propStr = getProperty("XCC-CONNECTION-RETRY-LIMIT");	
		if(propStr != null && propStr.length() > 0){
			try{
				connectRetryLimit = Integer.parseInt(propStr);
			}catch(Exception exc){}
		}
		return connectRetryLimit < 0 ? DEFAULT_RETRY_LIMIT : connectRetryLimit;
	}
	
	private int getConnectRetryInterval(){
		int connectRetryInterval = -1;
		String propStr = getProperty("XCC-CONNECTION-RETRY-INTERVAL");	
		if(propStr != null && propStr.length() > 0){
			try{
				connectRetryInterval = Integer.parseInt(propStr);
			}catch(Exception exc){}
		}
		return connectRetryInterval < 0 ? DEFAULT_RETRY_INTERVAL : connectRetryInterval;
	}
	
    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#finalize()
     */
    protected void finalize() throws Throwable {
        super.finalize();
    }
    
}
