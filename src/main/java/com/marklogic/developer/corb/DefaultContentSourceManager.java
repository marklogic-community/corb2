package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XdmVariable;

public class DefaultContentSourceManager extends AbstractContentSourceManager{
    protected static final int CONNECTION_POLICY_ROUND_ROBIN = 0;
    protected static final int CONNECTION_POLICY_RANDOM = 1;
    protected static final int CONNECTION_POLICY_LOAD = 2;
        
    protected int connectionPolicy = CONNECTION_POLICY_ROUND_ROBIN;
    
    protected ArrayList<ContentSource> contentSourceList = new ArrayList<ContentSource>();
    
    protected HashMap<ContentSource,Integer> errorCountsMap = new HashMap<ContentSource,Integer>();
    protected HashMap<ContentSource,Integer> connectionCountsMap = new HashMap<ContentSource,Integer>();
    protected HashMap<ContentSource,Long> errorTimeMap = new HashMap<ContentSource,Long>();
    
    protected int roundRobinIndex =  -1;
    
    private static final Logger LOG = Logger.getLogger(DefaultContentSourceManager.class.getName());
    
    public DefaultContentSourceManager(){}
    
    @Override
    public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings){
        super.init(properties,sslConfig);
        if(connectionStrings == null || connectionStrings.length == 0){
            throw new NullPointerException("XCC connection strings cannot be null or empty");
        }
                       
        for (String connectionString : connectionStrings){
            initContentSource(connectionString);
        }
        
        int policy = getIntProperty(CONNECTION_POLICY);
        if(CONNECTION_POLICY_RANDOM == policy || CONNECTION_POLICY_LOAD == policy){
            this.connectionPolicy = policy;
        }
        LOG.log(INFO,"Using the connection policy "+this.connectionPolicy);
    }
    
    protected void initContentSource(String connectionString){
        ContentSource contentSource = super.createContentSource(connectionString);
        if(contentSource != null) {
            contentSourceList.add(contentSource);
        }
    }
        
    @Override
    public ContentSource get(){
        ContentSource contentSource = nextContentSource();
        if(contentSource == null){
            throw new NullPointerException("ContentSource not available.");
        }
        
        Integer failedCount = errorCountsMap.get(contentSource);
        if(failedCount != null && failedCount.intValue() > 0){
            int retryInterval = getConnectRetryInterval();
            String hostname = contentSource.getConnectionProvider().getHostName();
            int port = contentSource.getConnectionProvider().getPort();
            LOG.log(WARNING,"Connection failed from MarkLogic server at {0}:{1}. Waiting for {2} seconds before retry attempt {3}",
                    new Object[]{hostname,port,retryInterval,failedCount.intValue()+1});
            try {
                Thread.sleep(retryInterval * 1000L);
            } catch (Exception ex) {
            }
        }
        
        return createContentSourceProxy(contentSource);
    }
    
    protected synchronized ContentSource nextContentSource(){
    		ArrayList<ContentSource> availableList = getAvailableContentSources();
        if(availableList.isEmpty()){
            return null;
        }
        
        ContentSource contentSource = null;
        if(availableList.size() == 1){
            contentSource = availableList.get(0);
        }else if(CONNECTION_POLICY_RANDOM == connectionPolicy){
            contentSource = availableList.get((int)(Math.random() * availableList.size()));
        }else if(CONNECTION_POLICY_LOAD == connectionPolicy){            
            for(ContentSource next: availableList){
                Integer count = connectionCountsMap.get(next);
                if(count == null || count.intValue() == 0){
                    contentSource = next;
                    break;
                }else if(contentSource == null || count < connectionCountsMap.get(contentSource)){
                    contentSource = next;
                }               
            }
        }else{
            roundRobinIndex = roundRobinIndex + 1;
            if(roundRobinIndex >= availableList.size()) roundRobinIndex = 0;
            contentSource = availableList.get(roundRobinIndex);
        }
               
        return contentSource;
    }
    
    private ArrayList<ContentSource> getAvailableContentSources(){
    		//check if any errored connections are eligible for retries with out further wait
    		if(errorTimeMap.size() > 0) {
        		long current = System.currentTimeMillis();
    			int retryInterval = getConnectRetryInterval();
	    		for(Iterator<Map.Entry<ContentSource, Long>> it = errorTimeMap.entrySet().iterator();it.hasNext();) {
	    			Map.Entry<ContentSource, Long> next = it.next();
	    			if((current - next.getValue().longValue()) >= (retryInterval * 1000L)) {
	    				it.remove();
	    			}	    				
	    		}
    		}
    		if(errorTimeMap.size() > 0) {
    			ArrayList<ContentSource> availableList = new ArrayList<ContentSource>();
	    		for(ContentSource cs: contentSourceList) {
	    			if(!errorTimeMap.containsKey(cs)) {
	    				availableList.add(cs);
	    			}
	    		}
	    		//if nothing available, then return the whole list as we have to wait anyway
	    		return availableList.size() > 0 ? availableList : contentSourceList;
    		}else {
    			return contentSourceList;
    		}
    }
    
    @Override
    public synchronized void remove(ContentSource contentSource){
        String hostname = contentSource.getConnectionProvider().getHostName();
        int port = contentSource.getConnectionProvider().getPort();
        
        if(contentSourceList.contains(contentSource)) {
        		LOG.log(WARNING,"Removing the MarkLogic server at {0}:{1} from the content source pool.", new Object[]{hostname,port});
	        contentSourceList.remove(contentSource);
	        connectionCountsMap.remove(contentSource);
	        errorCountsMap.remove(contentSource);
	        errorTimeMap.remove(contentSource);	        
        }
    }
                    
    @Override
    public boolean available(){
        return !contentSourceList.isEmpty();
    }
    
    @Override
    public void close() {
        connectionCountsMap.clear();
        errorCountsMap.clear();
        errorTimeMap.clear();
		contentSourceList.clear();
    }
    
    @Override
    public ContentSource[] getAllContentSources() {
    		return contentSourceList.toArray(new ContentSource[contentSourceList.size()]);
    }
    
    private boolean isLoadPolicy() {
    		return CONNECTION_POLICY_LOAD == connectionPolicy;
    }
    
    synchronized protected void hold(ContentSource cs) {
        Integer count = connectionCountsMap.get(cs);
        count = count == null ? new Integer(1) : new Integer (count.intValue() + 1);
        connectionCountsMap.put(cs, count);
    }
    
    synchronized protected void release(ContentSource cs) {
    		Integer count = connectionCountsMap.get(cs);
        count = (count == null || count.intValue() > 0 ) ? new Integer(0) : new Integer (count.intValue() - 1);
        connectionCountsMap.put(cs, count);
    }
    
    synchronized protected void success(ContentSource cs) {
		errorCountsMap.remove(cs);
		errorTimeMap.remove(cs);
	}
    
    synchronized protected void error(ContentSource cs) {
    		Integer count = errorCountsMap.get(cs);
        count = count == null ? new Integer(1) : new Integer (count.intValue() + 1);
        errorCountsMap.put(cs, count);
        errorTimeMap.put(cs, System.currentTimeMillis());
        
        int limit = getConnectRetryLimit();
        String hostname = cs.getConnectionProvider().getHostName();
        int port = cs.getConnectionProvider().getPort();
        LOG.log(WARNING,"Connection error count for host {0}:{1} is {2}. Max limit is {3}.", new Object[]{hostname,port,count,limit});
        if(count > limit){
        		remove(cs);
        }
	}
    
    protected int errorCount(ContentSource cs){
        Integer count = errorCountsMap.get(cs);
        return count != null ? count.intValue() : 0;
    }
    
    //methods to create dynamic proxies. 
    protected ContentSource createContentSourceProxy(ContentSource contentSource) {
    		return (ContentSource) Proxy.newProxyInstance(
    				DefaultContentSourceManager.class.getClassLoader(), new Class[] { ContentSource.class }, 
    				  new ContentSourceInvocationHandler(this,contentSource));
    }
        
    //invocation handlers
    static protected class ContentSourceInvocationHandler implements InvocationHandler {
    		DefaultContentSourceManager csm;
    		ContentSource target;
    		protected ContentSourceInvocationHandler(DefaultContentSourceManager cm, ContentSource target) {
    			this.csm = cm;
    			this.target = target;
    		}
		
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Object obj = method.invoke(target, args);
			
			if(obj != null && method.getName().equals("newSession") && obj instanceof Session) {
				obj = createSessionProxy((Session)obj);
			}
			
			return obj;
		}
		
	    protected Session createSessionProxy(Session session) {
			return (Session)Proxy.newProxyInstance(
					DefaultContentSourceManager.class.getClassLoader(), new Class[] { Session.class }, 
					  new SessionInvocationHandler(csm, target, session));
	    }
    	
    }
        
    static protected class SessionInvocationHandler implements InvocationHandler {
    		DefaultContentSourceManager csm;
    		ContentSource cs;
    		Session target;
    		int attempts = 0;
    		
		protected SessionInvocationHandler(DefaultContentSourceManager cm, ContentSource cs, Session target) {
			this.csm = cm;
			this.cs = cs;
			this.target = target;
		}
	
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			//NOTE: We only need to track connection counts for LOAD policy
			if(isSubmitRequest(method) || isInsertContent(method)) {
				if(isSubmitRequest(method)) validRequest(args);
				
				if(csm.isLoadPolicy()) csm.hold(cs);
				attempts++;
			}
			try {
				Object obj = method.invoke(target, args);
				//TODO: connection is held longer for streaming result sequence even after request is submitted.
				//We are ok now as we only use streaming results for query uris loader.
				if(isSubmitRequest(method) || isInsertContent(method)) {
					csm.success(cs);
					if(csm.isLoadPolicy()) csm.release(cs);
				}
				return obj;
			}catch(Exception exc) {
				if(exc.getCause() instanceof ServerConnectionException) {
					csm.error(cs);
					
					if(csm.isLoadPolicy() && (isSubmitRequest(method) || isInsertContent(method))) {
						csm.release(cs); //we should don't this in finally clause. 
					}
					
					int retryLimit = csm.getConnectRetryLimit();
					if(isSubmitRequest(method) && attempts < retryLimit) {
						LOG.info("Submit request failed "+attempts+" times. Max Limit is "+retryLimit+". Retrying..");
						return submitAsNewRequest(args);
					}else if(isInsertContent(method) && attempts < retryLimit) {
						LOG.info("Insert content failed "+attempts+" times. Max Limit is "+retryLimit+". Retrying..");
						return insertAsNewRequest(args);
					}else {
						throw exc;
					}
				}else {
					throw exc;
				}
			}
		}
		
		private void validRequest(Object[] args) {
			Request request = (Request)args[0];
			if(!(request instanceof AdhocQuery || request instanceof ModuleInvoke)) {
				throw new IllegalArgumentException("Only moduleInvoke or adhocQuery requests are supported by corb");
			}
		}
		
		private Object submitAsNewRequest(Object[] args) throws RequestException{
			Request request = (Request)args[0];
			Session newSession = csm.get().newSession();
			Request newRequest = null;
			if(request instanceof AdhocQuery) {
				newRequest = newSession.newAdhocQuery(((AdhocQuery)request).getQuery());
			}else {
				newRequest = newSession.newModuleInvoke(((ModuleInvoke)request).getModuleUri());
			}
			newRequest.setOptions(request.getOptions());
			
			XdmVariable[] vars = request.getVariables();
			for(int i=0; vars!= null && i < vars.length;i++) {
				newRequest.setVariable(vars[i]);
			}
			
			return newSession.submitRequest(newRequest);
		}
		
		private Object insertAsNewRequest(Object[] args) throws RequestException{
			Session newSession = csm.get().newSession();
			if(args[0] instanceof Content) {
				newSession.insertContent((Content)args[0]);
			}else if(args[0] instanceof Content[]) {
				newSession.insertContent((Content[])args[0]);
			}
			return null;
		}
		
		private boolean isSubmitRequest(Method method) {
			return method.getName().equals("submitRequest");
		}
		
		private boolean isInsertContent(Method method) {
			return method.getName().equals("insertContent");
		}		
	}    
}
