package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
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
import java.util.List;

public class DefaultContentSourcePool extends AbstractContentSourcePool{
    protected static final String CONNECTION_POLICY_ROUND_ROBIN = "ROUND-ROBIN";
    protected static final String CONNECTION_POLICY_RANDOM = "RANDOM";
    protected static final String CONNECTION_POLICY_LOAD = "LOAD";

    protected String connectionPolicy = CONNECTION_POLICY_ROUND_ROBIN;

    protected List<ContentSource> contentSourceList = new ArrayList<>();

    protected Map<ContentSource,Integer> errorCountsMap = new HashMap<>();
    protected Map<ContentSource,Integer> connectionCountsMap = new HashMap<>();
    protected Map<ContentSource,Long> errorTimeMap = new HashMap<>();

    protected int roundRobinIndex =  -1;

    private static final Logger LOG = Logger.getLogger(DefaultContentSourcePool.class.getName());

    public DefaultContentSourcePool(){}

    @Override
    public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings){
        super.init(properties,sslConfig);
        if(connectionStrings == null || connectionStrings.length == 0){
            throw new NullPointerException("XCC connection strings cannot be null or empty");
        }

        for (String connectionString : connectionStrings){
            initContentSource(connectionString);
        }

        String policy = getProperty(CONNECTION_POLICY);
        if (CONNECTION_POLICY_RANDOM.equals(policy) || CONNECTION_POLICY_LOAD.equals(policy)) {
            this.connectionPolicy = policy;
        }
        LOG.log(INFO, "Using the connection policy {0}", this.connectionPolicy);
    }

    protected void initContentSource(String connectionString){
        ContentSource contentSource = super.createContentSource(connectionString);
        if (contentSource != null) {
            contentSourceList.add(contentSource);
        }
    }

    @Override
    public ContentSource get(){
        ContentSource contentSource = nextContentSource();
        if (contentSource == null) {
            throw new NullPointerException("ContentSource not available.");
        }

        Integer failedCount = errorCountsMap.get(contentSource);
        if (failedCount != null && failedCount > 0){
            int retryInterval = getConnectRetryInterval();
            String hostname = contentSource.getConnectionProvider().getHostName();
            int port = contentSource.getConnectionProvider().getPort();
            LOG.log(WARNING, "Connection failed from MarkLogic server at {0}:{1}. Waiting for {2} seconds before retry attempt {3}",
                    new Object[]{hostname,port,retryInterval,failedCount + 1});
            try {
                Thread.sleep(retryInterval * 1000L);
            } catch (Exception ex) {
            }
        }

        return createContentSourceProxy(contentSource);
    }

    protected synchronized ContentSource nextContentSource(){
    		List<ContentSource> availableList = getAvailableContentSources();
        if (availableList.isEmpty()){
            return null;
        }

        ContentSource contentSource = null;
        if (availableList.size() == 1) {
            contentSource = availableList.get(0);
        } else if (CONNECTION_POLICY_RANDOM.equals(connectionPolicy)) {
            contentSource = availableList.get((int)(Math.random() * availableList.size()));
        } else if (CONNECTION_POLICY_LOAD.equals(connectionPolicy)) {
            for (ContentSource next: availableList){
                Integer count = connectionCountsMap.get(next);
                if (count == null || count == 0){
                    contentSource = next;
                    break;
                } else if(contentSource == null || count < connectionCountsMap.get(contentSource)){
                    contentSource = next;
                }
            }
        } else {
            roundRobinIndex += 1;
            if (roundRobinIndex >= availableList.size()) {
                roundRobinIndex = 0;
            }
            contentSource = availableList.get(roundRobinIndex);
        }

        return contentSource;
    }

    protected List<ContentSource> getAvailableContentSources(){
        //check if any errored connections are eligible for retries with out further wait
        if (!errorTimeMap.isEmpty()) {
            long current = System.currentTimeMillis();
            int retryInterval = getConnectRetryInterval();
            errorTimeMap.entrySet().removeIf(next -> (current - next.getValue()) >= (retryInterval * 1000L));
        }
        if (!errorTimeMap.isEmpty()) {
            List<ContentSource> availableList = new ArrayList<>();
            for (ContentSource cs: contentSourceList) {
                if(!errorTimeMap.containsKey(cs)) {
                    availableList.add(cs);
                }
            }
            //if nothing available, then return the whole list as we have to wait anyway
            return !availableList.isEmpty() ? availableList : contentSourceList;
        } else {
            return contentSourceList;
        }
    }

    @Override
    public synchronized void remove(ContentSource contentSource){
        if (contentSourceList.contains(contentSource)) {
            String hostname = contentSource.getConnectionProvider().getHostName();
            int port = contentSource.getConnectionProvider().getPort();
            
            LOG.log(WARNING, "Removing the MarkLogic server at {0}:{1} from the content source pool.", new Object[]{hostname,port});
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

    protected boolean isLoadPolicy() {
    		return CONNECTION_POLICY_LOAD.equals(connectionPolicy);
    }

    synchronized protected void hold(ContentSource cs) {
        Integer count = connectionCountsMap.get(cs);
        count = count == null ? 1 : count + 1;
        connectionCountsMap.put(cs, count);
    }

    synchronized protected void release(ContentSource cs) {
    		Integer count = connectionCountsMap.get(cs);
        count = (count == null || count > 0 ) ? 0 : count - 1;
        connectionCountsMap.put(cs, count);
    }

    synchronized protected void success(ContentSource cs) {
		errorCountsMap.remove(cs);
		errorTimeMap.remove(cs);
	}

    synchronized protected void error(ContentSource cs) {
    		Integer count = errorCountsMap.get(cs);
        count = count == null ? 1 : count + 1;
        errorCountsMap.put(cs, count);
        errorTimeMap.put(cs, System.currentTimeMillis());

        int limit = getConnectRetryLimit();
        String hostname = cs.getConnectionProvider().getHostName();
        int port = cs.getConnectionProvider().getPort();
        LOG.log(WARNING, "Connection error count for host {0}:{1} is {2}. Max limit is {3}.", new Object[]{hostname,port,count,limit});
        if (count > limit){
        		remove(cs);
        }
	}

    protected int errorCount(ContentSource cs){
        Integer count = errorCountsMap.get(cs);
        return count != null ? count : 0;
    }

    //methods to create dynamic proxy instances.
    protected ContentSource createContentSourceProxy(ContentSource contentSource) {
        return (ContentSource) Proxy.newProxyInstance(
                DefaultContentSourcePool.class.getClassLoader(), new Class[] { ContentSource.class },
                  new ContentSourceInvocationHandler(this,contentSource));
    }

    //invocation handlers
    static protected class ContentSourceInvocationHandler implements InvocationHandler {
        DefaultContentSourcePool csp;
        ContentSource target;
        protected ContentSourceInvocationHandler(DefaultContentSourcePool csp, ContentSource target) {
            this.csp = csp;
            this.target = target;
        }

        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Object obj = method.invoke(target, args);

			if(obj != null && method.getName().equals("newSession") && obj instanceof Session) {
				obj = createSessionProxy((Session)obj);
			}

			return obj;
		}

	    public Session createSessionProxy(Session session) {
			return (Session)Proxy.newProxyInstance(
					DefaultContentSourcePool.class.getClassLoader(), new Class[] { Session.class },
					  new SessionInvocationHandler(csp, target, session));
	    }

    }

    static protected class SessionInvocationHandler implements InvocationHandler {
        DefaultContentSourcePool csp;
    		ContentSource cs;
    		Session target;
    		int attempts = 0;

		protected SessionInvocationHandler(DefaultContentSourcePool csp, ContentSource cs, Session target) {
			this.csp = csp;
			this.cs = cs;
			this.target = target;
		}

        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			//NOTE: We only need to track connection counts for LOAD policy
			if (isSubmitRequest(method) || isInsertContent(method)) {
				if (isSubmitRequest(method)) {
                    validRequest(args);
                }

				if (csp.isLoadPolicy()) {
                    csp.hold(cs);
                }
				attempts++;
			}
			try {
				Object obj = method.invoke(target, args);
				//TODO: connection is held longer for streaming result sequence even after request is submitted.
				//We are ok now as we only use streaming results for query uris loader.
				if (isSubmitRequest(method) || isInsertContent(method)) {
					csp.success(cs);
					if (csp.isLoadPolicy()) {
                        csp.release(cs);
                    }
				}
				return obj;
			} catch(Exception exc) {
				if (exc.getCause() instanceof ServerConnectionException) {
					csp.error(cs);

					if (csp.isLoadPolicy() && (isSubmitRequest(method) || isInsertContent(method))) {
						csp.release(cs); //we should don't this in finally clause.
					}

					int retryLimit = csp.getConnectRetryLimit();
					if (isSubmitRequest(method) && attempts < retryLimit) {
						LOG.log(INFO, "Submit request failed {0} times. Max Limit is {1}. Retrying..", new Object[]{attempts, retryLimit});
						return submitAsNewRequest(args);
					} else if (isInsertContent(method) && attempts < retryLimit) {
						LOG.log(INFO, "Insert content failed {0} times. Max Limit is {1}. Retrying..", new Object[]{attempts, retryLimit});
						return insertAsNewRequest(args);
					} else {
						throw exc;
					}
				} else {
					throw exc;
				}
			}
		}

		private void validRequest(Object[] args) {
			Request request = (Request)args[0];
			if (!(request instanceof AdhocQuery || request instanceof ModuleInvoke)) {
				throw new IllegalArgumentException("Only moduleInvoke or adhocQuery requests are supported by corb");
			}
		}

		private Object submitAsNewRequest(Object[] args) throws RequestException{
			Request request = (Request)args[0];
			Session newSession = csp.get().newSession();
			Request newRequest;
			if (request instanceof AdhocQuery) {
				newRequest = newSession.newAdhocQuery(((AdhocQuery)request).getQuery());
			} else {
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
			Session newSession = csp.get().newSession();
			if (args[0] instanceof Content) {
				newSession.insertContent((Content)args[0]);
			} else if(args[0] instanceof Content[]) {
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
