package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
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
                    new Object[]{hostname,port+"",retryInterval,failedCount.intValue()+1});
            try {
                Thread.sleep(retryInterval * 1000L);
            } catch (Exception ex) {
            }
        }
        
        return contentSource;
    }
    
    protected synchronized ContentSource nextContentSource(){
        if(contentSourceList.isEmpty()){
            return null;
        }
        
        ContentSource contentSource = null;
        if(contentSourceList.size() == 1){
            contentSource = contentSourceList.get(0);
        }else if(CONNECTION_POLICY_RANDOM == connectionPolicy){
            contentSource = contentSourceList.get((int)(Math.random() * contentSourceList.size()));
        }else if(CONNECTION_POLICY_LOAD == connectionPolicy){            
            for(ContentSource next: contentSourceList){
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
            if(roundRobinIndex >= contentSourceList.size()) roundRobinIndex = 0;
            contentSource = contentSourceList.get(roundRobinIndex);
        }
               
        return contentSource;
    }
    
    @Override
    public synchronized void remove(ContentSource contentSource){
        String hostname = contentSource.getConnectionProvider().getHostName();
        LOG.log(WARNING,"Removed the MarkLogic server at {0} from the content source pool.", new Object[]{hostname});
        contentSourceList.remove(contentSource);
        errorCountsMap.remove(contentSource);
    }
                    
    @Override
    public boolean available(){
        return !contentSourceList.isEmpty();
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
	}
    
    synchronized protected void error(ContentSource cs) {
    		Integer count = errorCountsMap.get(cs);
        count = count == null ? new Integer(1) : new Integer (count.intValue() + 1);
        errorCountsMap.put(cs, count);
        
        int limit = getConnectRetryLimit();
        String hostname = cs.getConnectionProvider().getHostName();
        LOG.log(WARNING,"Connection error count for host {0} is {1}. Max limit is {3}.", new Object[]{hostname,count,limit});
        if(count > limit){
        		remove(cs);
        }
	}
    
    protected int errorCount(ContentSource cs){
        Integer count = errorCountsMap.get(cs);
        return count != null ? count.intValue() : 0;
    }
        
    protected ContentSource getContentSourceProxy(ContentSource contentSource) {
    		return (ContentSource) Proxy.newProxyInstance(
    				DefaultContentSourceManager.class.getClassLoader(), new Class[] { ContentSource.class }, 
    				  new ContentSourceInvocationHandler(this,contentSource));
    }
    
    protected class ContentSourceInvocationHandler implements InvocationHandler {
    		DefaultContentSourceManager cm;
    		ContentSource target;
    		protected ContentSourceInvocationHandler(DefaultContentSourceManager cm, ContentSource target) {
    			this.cm = cm;
    			this.target = target;
    		}
		
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Object obj = method.invoke(target, args);
			
			if(obj != null && method.getName().equals("newSession") && obj instanceof Session) {
				obj = (Session) Proxy.newProxyInstance(
	    				DefaultContentSourceManager.class.getClassLoader(), new Class[] { Session.class }, 
	    				  new SessionInvocationHandler(cm, (Session)obj));
			}
			
			return obj;
		}
    	
    }
    
    protected class SessionInvocationHandler implements InvocationHandler {
    		DefaultContentSourceManager cm;	
    		Session target;
    		int attempts = 0;
    		
		protected SessionInvocationHandler(DefaultContentSourceManager cm, Session target) {
			this.cm = cm;
			this.target = target;
		}
	
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			//NOTE: We only need to track connection counts for LOAD policy
			if(isSubmitRequest(method) || isInsertContent(method)) {
				if(isSubmitRequest(method)) validRequest(args);
				
				if(cm.isLoadPolicy()) cm.hold(target.getContentSource());
				attempts++;
			}
			try {
				Object obj = method.invoke(target, args);
				//TODO: connection is held longer for streaming result sequence even after request is submitted.
				//We are ok now as we only use streaming results for query uris loader.
				if(isSubmitRequest(method) || isInsertContent(method)) {
					cm.success(target.getContentSource());
					if(cm.isLoadPolicy()) cm.release(target.getContentSource());
				}
				return obj;
			}catch(Exception exc) {
				if(exc.getCause() instanceof ServerConnectionException) {
					error(target.getContentSource());
					
					if(cm.isLoadPolicy() && (isSubmitRequest(method) || isInsertContent(method))) {
						cm.release(target.getContentSource()); //we should don't this in finally clause. 
					}
					
					if(isSubmitRequest(method) && attempts < cm.getConnectRetryLimit()) {
						return submitAsNewRequest(args);
					}else if(isInsertContent(method) && attempts < cm.getConnectRetryLimit()) {
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
			Session newSession = cm.get().newSession();
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
			Session newSession = cm.get().newSession();
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
