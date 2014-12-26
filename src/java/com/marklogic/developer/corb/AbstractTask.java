package com.marklogic.developer.corb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
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
	
	static private Object sync = new Object();
	static private List<String> propertyNames;
	
    public void setContentSource(ContentSource cs){
    	this.cs = cs;
    }
    
    public void setModuleType(String moduleType){
    	this.moduleType=moduleType;
    }
    
    public void setModuleURI(String moduleUri){
    	this.moduleUri = moduleUri;
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
		if(moduleUri == null) return null;
		
        Session session = null;
        ResultSequence seq = null;
		Thread.yield();// try to avoid thread starvation
        try {
            session = newSession();
            Request request = session.newModuleInvoke(moduleUri);
            request.setNewStringVariable("URI", inputUri);
            
            if(properties.containsKey(Manager.URIS_BATCH_REF)){
            	request.setNewStringVariable(Manager.URIS_BATCH_REF, properties.getProperty(Manager.URIS_BATCH_REF));
            }
            
            if(propertyNames == null){
            	synchronized(sync){
            		if(propertyNames == null){
            			propertyNames = new ArrayList<String>(properties.stringPropertyNames());
            			propertyNames.addAll(System.getProperties().stringPropertyNames());
            		}
            	}
            }
                    
            for(String propName:propertyNames){
            	if(moduleType != null && propName.startsWith(moduleType+".")){
            		String varName = propName.substring(moduleType.length()+1);
            		String value = getProperty(propName);
            		if(value != null) request.setNewStringVariable(varName, value);
            	}
            } 

            Thread.yield();// try to avoid thread starvation
            seq = session.submitRequest(request);
            // no need to hold on to the session as results will be cached.
            session.close(); 
            Thread.yield();// try to avoid thread starvation
            
            String result = processResult(seq);
            seq.close();
            Thread.yield();// try to avoid thread starvation
              
            return result;
        }catch(Exception exc){
        	throw new CorbException(exc.getMessage(),exc);
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
		
	public String getProperty(String key){
		String val = System.getProperty(key);
		if(val == null || val.trim().length() == 0){
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
	
    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#finalize()
     */
    protected void finalize() throws Throwable {
        super.finalize();
    }
    
}
