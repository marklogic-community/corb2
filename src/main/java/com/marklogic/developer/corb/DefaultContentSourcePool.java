package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
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
/**
 * @since 2.4.0
 */
public class DefaultContentSourcePool extends AbstractContentSourcePool {
    protected static final String CONNECTION_POLICY_ROUND_ROBIN = "ROUND-ROBIN";
    protected static final String CONNECTION_POLICY_RANDOM = "RANDOM";
    protected static final String CONNECTION_POLICY_LOAD = "LOAD";

    protected String connectionPolicy = CONNECTION_POLICY_ROUND_ROBIN;

    protected List<ContentSource> contentSourceList = new ArrayList<>();

    protected Map<ContentSource,Integer> errorCountsMap = new HashMap<>();
    protected Map<ContentSource,Integer> connectionCountsMap = new HashMap<>();
    protected Map<ContentSource,Long> errorTimeMap = new HashMap<>();
    
    protected int retryInterval = 0;
    protected int hostRetryLimit = 0;
    protected int retryLimit = 0;

    protected int roundRobinIndex =  -1;

    private static final Logger LOG = Logger.getLogger(DefaultContentSourcePool.class.getName());

    public DefaultContentSourcePool(){}

    @Override
    public void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings){
        super.init(properties, sslConfig);
        if (connectionStrings == null || connectionStrings.length == 0) {
            throw new NullPointerException("XCC connection strings cannot be null or empty");
        }

        retryInterval = getConnectRetryInterval();
        retryLimit = getConnectRetryLimit();
        hostRetryLimit = getConnectHostRetryLimit();
        
        String policy = getProperty(CONNECTION_POLICY);
        if (CONNECTION_POLICY_RANDOM.equals(policy) || CONNECTION_POLICY_LOAD.equals(policy)) {
            this.connectionPolicy = policy;
        }
        LOG.log(INFO, "Using the connection policy {0}", this.connectionPolicy);
        
        for (String connectionString : connectionStrings) {
            initContentSource(connectionString);
        }
    }

    protected void initContentSource(String connectionString){
        ContentSource contentSource = super.createContentSource(connectionString);
        if (contentSource != null) {
            contentSourceList.add(contentSource);
            LOG.log(INFO, "Initialized ContentSource {0}", new Object[]{asString(contentSource)});
        }
    }

    /**
     * Note: Do not make this synchronized, it will affect performance significantly due to sleep.
     */
    @Override
    public ContentSource get() throws CorbException{
        ContentSource contentSource = nextContentSource();
        if (contentSource == null) {
            throw new CorbException("ContentSource not available.");
        }

        //if the nextContentSource() returns the connection with existing errors, then it means it could not find
        //any clean connections, so we need to wait.
        //even if errored, but wait expired, then no need to wait.
        Integer failedCount = errorCountsMap.get(contentSource);
        if (failedCount != null && failedCount > 0 && errorTimeMap.containsKey(contentSource)) {
            LOG.log(WARNING, "Connection failed for ContentSource {0}. Waiting for {1} seconds before retry attempt {2}",
                    new Object[]{asString(contentSource),retryInterval,failedCount + 1});
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
                if (count == null || count == 0) {
                    contentSource = next;
                    break;
                } else if(contentSource == null || count < connectionCountsMap.get(contentSource)){
                    contentSource = next;
                }
            }
        } else {
            roundRobinIndex++;
            if (roundRobinIndex >= availableList.size()) {
                roundRobinIndex = 0;
            }
            contentSource = availableList.get(roundRobinIndex);
        }

        return contentSource;
    }

    protected synchronized List<ContentSource> getAvailableContentSources(){
        //check if any errored connections are eligible for retries with out further wait
        if (!errorTimeMap.isEmpty()) {
            long current = System.currentTimeMillis();
            errorTimeMap.entrySet().removeIf(next -> (current - next.getValue()) >= (retryInterval * 1000L));
        }
        if (!errorTimeMap.isEmpty()) {
            List<ContentSource> availableList = new ArrayList<>(contentSourceList.size());
            for (ContentSource cs: contentSourceList) {
                if (!errorTimeMap.containsKey(cs)) {
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
    public void remove(ContentSource contentSource){
    		removeInternal(getContentSourceFromProxy(contentSource));
    }

    @Override
    public boolean available() {
        return !contentSourceList.isEmpty();
    }

    @Override
    public ContentSource[] getAllContentSources() {
        return contentSourceList.toArray(new ContentSource[contentSourceList.size()]);
    }

    @Override
    public void close() {
        connectionCountsMap.clear();
        errorCountsMap.clear();
        errorTimeMap.clear();
		contentSourceList.clear();
    }

    protected boolean isLoadPolicy() {
        return CONNECTION_POLICY_LOAD.equals(connectionPolicy);
    }

    synchronized protected void hold(ContentSource cs) {
        if (contentSourceList.contains(cs)) {
	        Integer count = connectionCountsMap.get(cs);
	        count = count == null ? 1 : count + 1;
	        connectionCountsMap.put(cs, count);
        }
    }

    synchronized protected void release(ContentSource cs) {
        Integer count = connectionCountsMap.get(cs);
        if (count != null && count > 0) {
            connectionCountsMap.put(cs, count-1);
        }
    }

    synchronized protected void success(ContentSource cs) {
		errorCountsMap.remove(cs);
		errorTimeMap.remove(cs);
	}

    protected void error(ContentSource cs) {
        error(cs,-1);
    }

    synchronized protected void error(ContentSource cs, long allocTime) {
        if (contentSourceList.contains(cs)) {
            Long lastErrorTime = errorTimeMap.get(cs);
            if (lastErrorTime == null || allocTime <= 0 || allocTime > lastErrorTime.longValue()) {
		        Integer count = errorCountsMap.get(cs);
		        count = count == null ? 1 : count + 1;
		        errorCountsMap.put(cs, count);
		        errorTimeMap.put(cs, System.currentTimeMillis());

		        
		        LOG.log(WARNING, "Connection error count for ContentSource {0} is {1}. Max limit is {2}.", new Object[]{asString(cs),count,hostRetryLimit});
		        if (count > hostRetryLimit) {
                    removeInternal(cs);
		        }
            } else {
                LOG.log(WARNING, "Connection error for ContentSource {0} is not counted towards the limit as it was allocated before last error.", new Object[]{asString(cs)});
            }
        }
	}

    protected int errorCount(ContentSource cs) {
        Integer count = errorCountsMap.get(cs);
        return count != null ? count : 0;
    }

    //this is not a proxy
    protected synchronized void removeInternal(ContentSource cs) {
        if (contentSourceList.contains(cs)) {
	        LOG.log(WARNING, "Removing the ContentSource {0} from the content source pool.", new Object[]{asString(cs)});
	        contentSourceList.remove(cs);
	        connectionCountsMap.remove(cs);
	        errorCountsMap.remove(cs);
	        errorTimeMap.remove(cs);
        }
    }

    //TODO: handle redaction if necessary?
    protected String asString(ContentSource cs) {
    		return cs == null ? "null" : cs.toString();
    }

    //methods to create dynamic proxy instances.
    protected ContentSource createContentSourceProxy(ContentSource cs) {
        return (ContentSource) Proxy.newProxyInstance(
                DefaultContentSourcePool.class.getClassLoader(), new Class[] { ContentSource.class },
                  new ContentSourceInvocationHandler(this, cs));
    }

    static public ContentSource getContentSourceFromProxy(ContentSource proxy) {
		ContentSource target = proxy;
		if (proxy != null && Proxy.isProxyClass(proxy.getClass())) {
			InvocationHandler handler = Proxy.getInvocationHandler(proxy);
			if (handler instanceof ContentSourceInvocationHandler) {
				target = ((ContentSourceInvocationHandler)handler).target;
			}
		}
		return target;
	}

	static public Session getSessionFromProxy(Session proxy) {
		Session target = proxy;
		if (proxy != null && Proxy.isProxyClass(proxy.getClass())) {
			InvocationHandler handler = Proxy.getInvocationHandler(proxy);
			if (handler instanceof SessionInvocationHandler) {
				target = ((SessionInvocationHandler)handler).target;
			}
		}
		return target;
	}

    //invocation handlers
    static protected class ContentSourceInvocationHandler implements InvocationHandler{
    		static final String NEW_SESSION = "newSession";
        DefaultContentSourcePool csp;
        ContentSource target;
        long allocTime;

        protected ContentSourceInvocationHandler(DefaultContentSourcePool csp, ContentSource target) {
            this.csp = csp;
            this.target = target;
            this.allocTime = System.currentTimeMillis();
        }

        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable  {
			Object obj = method.invoke(target, args);

			if (obj != null && isNewSession(method) && obj instanceof Session) {
				obj = createSessionProxy((Session)obj);
			}

			return obj;
		}

        protected Session createSessionProxy(Session session) {
    			return (Session)Proxy.newProxyInstance(
    				DefaultContentSourcePool.class.getClassLoader(), new Class[] { Session.class },
    				  new SessionInvocationHandler(csp, target, session, allocTime));
        }

        private boolean isNewSession(Method method) {
        		return NEW_SESSION.equals(method.getName());
        }
    }

    //TODO: This code does not handle explicit commits and rollbacks.
    static protected class SessionInvocationHandler implements InvocationHandler {
    		static final String SUBMIT_REQUEST = "submitRequest";
    		static final String INSERT_CONTENT = "insertContent";
    		static final String COMMIT = "commit";
    		static final String ROLLBACK = "rollback";
    		static final String CLOSE = "close";
    		static final String EMPTY_SEQ = "";
    		
        DefaultContentSourcePool csp;
        ContentSource cs;
        Session target;
        long allocTime;

        int attempts = 0;

        Session retryProxy;

		protected SessionInvocationHandler(DefaultContentSourcePool csp, ContentSource cs, Session target, long allocTime) {
			this.csp = csp;
			this.cs = cs;
			this.target = target;
			this.allocTime = allocTime;
		}

        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        		checkUnsupported(method);

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
				if (retryProxy != null && isClose(method)) {
					retryProxy.close(); //Use proxy only as there can be multiple retry attemps in a chain.
				}
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
			} catch (Exception exc) {
				if (csp.isLoadPolicy() && (isSubmitRequest(method) || isInsertContent(method))) {
                    csp.release(cs); //we should do this before the recursion.. not finally.
                }
                if (exc instanceof InvocationTargetException) {
                    if (exc.getCause() instanceof ServerConnectionException) {
	                    csp.error(cs,allocTime); //we should do this before the recursion.. not finally.

	                    String name = exc.getCause().getClass().getSimpleName();
	                    if (isSubmitRequest(method) && attempts <= csp.retryLimit) {
	                        LOG.log(WARNING, "Submit request failed {0} times with {1}. Max Limit is {2}. Retrying..", new Object[]{attempts, name, csp.retryLimit});
	                        return submitAsNewRequest(args);
	                    } else if (isInsertContent(method) && attempts <= csp.retryLimit) {
	                        LOG.log(WARNING, "Insert content failed {0} times {1}. Max Limit is {2}. Retrying..", new Object[]{attempts, name, csp.retryLimit});
	                        return insertAsNewRequest(args);
	                    } else {
	                        throw exc.getCause();
	                    }
	                } else {
					    throw exc.getCause();
					}
				} else {
					throw exc;
				}
			}
		}

		protected void validRequest(Object[] args) {
			Request request = (Request)args[0];
			if (!(request instanceof AdhocQuery || request instanceof ModuleInvoke)) {
				throw new IllegalArgumentException("Only moduleInvoke or adhocQuery requests are supported by corb");
			}
		}

		protected Object submitAsNewRequest(Object[] args) throws RequestException{
			Request request = (Request)args[0];
			try {
				retryProxy = csp.get().newSession();
				setAttemptsToNewSession(retryProxy);
				Request newRequest;
				if (request instanceof AdhocQuery) {
					newRequest = retryProxy.newAdhocQuery(((AdhocQuery)request).getQuery());
				} else {
					newRequest = retryProxy.newModuleInvoke(((ModuleInvoke)request).getModuleUri());
				}
				newRequest.setOptions(request.getOptions());

				XdmVariable[] vars = request.getVariables();
				for (int i = 0; vars != null && i < vars.length; i++) {
					newRequest.setVariable(vars[i]);
				}

				return retryProxy.submitRequest(newRequest);
			} catch (CorbException exc) {
				throw new RequestException(exc.getMessage(),request,exc);
			}
		}

		protected Object insertAsNewRequest(Object[] args) throws RequestException{
			try {
				retryProxy = csp.get().newSession();
				setAttemptsToNewSession(retryProxy);
				if (args[0] instanceof Content) {
					retryProxy.insertContent((Content)args[0]);
				} else if (args[0] instanceof Content[]) {
					retryProxy.insertContent((Content[])args[0]);
				}
				return null;
			} catch (CorbException exc) {
				throw new RequestException(exc.getMessage(),target.newAdhocQuery(EMPTY_SEQ),exc);
			}
		}

		private boolean isSubmitRequest(Method method) {
			return SUBMIT_REQUEST.equals(method.getName());
		}

		private boolean isInsertContent(Method method) {
			return INSERT_CONTENT.equals(method.getName());
		}

		private boolean isClose(Method method) {
			return CLOSE.equals(method.getName());
		}

		private void checkUnsupported(Method method) {
			if (COMMIT.equals(method.getName()) || ROLLBACK.equals(method.getName())) {
				throw new UnsupportedOperationException(method.getName()+" is not supported by "+getClass().getName());
			}
		}

		protected void setAttemptsToNewSession(Session newProxy) {
			if (Proxy.isProxyClass(newProxy.getClass())) {
				InvocationHandler handler = Proxy.getInvocationHandler(newProxy);
				if (handler instanceof SessionInvocationHandler) {
					((SessionInvocationHandler)handler).attempts = this.attempts;
				}
			}
		}
	}
}
