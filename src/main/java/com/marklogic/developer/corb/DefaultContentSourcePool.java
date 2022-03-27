/*
 * * Copyright (c) 2004-2022 MarkLogic Corporation
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 * *
 * * The use of the Apache License does not indicate that this project is
 * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.CONNECTION_POLICY;
import static com.marklogic.developer.corb.Options.CONTENT_SOURCE_RENEW;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
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
import java.util.Random;
import java.util.logging.Logger;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.spi.ConnectionProvider;
import com.marklogic.xcc.spi.SingleHostAddress;
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
    protected Map<ContentSource, Integer> errorCountsMap = new HashMap<>();
    protected Map<ContentSource, Integer> connectionCountsMap = new HashMap<>();
    protected Map<ContentSource, Long> errorTimeMap = new HashMap<>();
    protected Map<ContentSource, String> connectionStringMap = new HashMap<>();

    protected int retryInterval = 0;
    protected int hostRetryLimit = 0;
    protected int retryLimit = 0;

    protected int roundRobinIndex =  -1;

    protected boolean isLoadPolicy = false;
    protected boolean isRandomPolicy = false;
    boolean replaceContentSourceOnError = stringToBoolean(getProperty(CONTENT_SOURCE_RENEW), true);

    private static final Logger LOG = Logger.getLogger(DefaultContentSourcePool.class.getName());
    private final Random random = new Random();

    public DefaultContentSourcePool() {
        super();
    }

    @Override
    public void init(Properties properties, SSLConfig sslConfig, String... connectionStrings) {
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
        LOG.log(INFO, "Using the connection policy {0}", connectionPolicy);

        for (String connectionString : connectionStrings) {
            initContentSource(connectionString);
        }

        //for better performance avoid to many string equals later for every get and submit
        isRandomPolicy = CONNECTION_POLICY_RANDOM.equals(connectionPolicy);
        isLoadPolicy = CONNECTION_POLICY_LOAD.equals(connectionPolicy);
    }

    protected void initContentSource(String connectionString) {
        ContentSource contentSource = super.createContentSource(connectionString);
        if (contentSource != null) {
            contentSourceList.add(contentSource);
            connectionStringMap.put(contentSource, connectionString);
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
                    new Object[]{asString(contentSource), retryInterval, failedCount + 1});
            try {
                Thread.sleep(retryInterval * 1000L);
            } catch (InterruptedException ex) {
                LOG.log(WARNING, "Interrupted!", ex);
                Thread.currentThread().interrupt();
            }
        }

        return createContentSourceProxy(contentSource);
    }

    protected synchronized ContentSource nextContentSource() {
        List<ContentSource> availableList = getAvailableContentSources();
        if (availableList.isEmpty()) {
            return null;
        }

        ContentSource contentSource = null;
        if (availableList.size() == 1) {
            contentSource = availableList.get(0);
        } else if (isRandomPolicy) {
            contentSource = availableList.get(this.random.nextInt(availableList.size()));
        } else if (isLoadPolicy) {
            for (ContentSource next: availableList) {
                Integer count = connectionCountsMap.get(next);
                if (count == null || count == 0) {
                    contentSource = next;
                    break;
                } else if (contentSource == null || count < connectionCountsMap.get(contentSource)) {
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

    protected synchronized List<ContentSource> getAvailableContentSources() {
        //check if any errored connections are eligible for retries without further wait
        if (!errorTimeMap.isEmpty()) {
            long current = System.currentTimeMillis();
            errorTimeMap.entrySet().removeIf(next -> (current - next.getValue()) >= (retryInterval * 1000L));
        }
        if (!errorTimeMap.isEmpty()) {
            List<ContentSource> availableList = new ArrayList<>(contentSourceList.size());
            for (ContentSource contentSource: contentSourceList) {
                if (!errorTimeMap.containsKey(contentSource)) {
                    availableList.add(contentSource);
                }
            }
            //if nothing available, then return the whole list as we have to wait anyway
            return !availableList.isEmpty() ? availableList : contentSourceList;
        } else {
            return contentSourceList;
        }
    }

    @Override
    public void remove(ContentSource contentSource) {
    		removeInternal(getContentSourceFromProxy(contentSource));
    }

    @Override
    public boolean available() {
        return !contentSourceList.isEmpty();
    }

    @Override
    public ContentSource[] getAllContentSources() {
        return contentSourceList.toArray(new ContentSource[0]);
    }

    @Override
    public void close() {
        connectionCountsMap.clear();
        errorCountsMap.clear();
        errorTimeMap.clear();
		contentSourceList.clear();
    }

    protected boolean isRandomPolicy() {
        return this.isRandomPolicy;
    }

    protected boolean isLoadPolicy() {
        return this.isLoadPolicy;
    }

    protected synchronized void hold(ContentSource contentSource) {
        if (contentSourceList.contains(contentSource)) {
	        Integer count = connectionCountsMap.getOrDefault(contentSource, 0) + 1;
	        connectionCountsMap.put(contentSource, count);
        }
    }

    protected synchronized void release(ContentSource contentSource) {
        Integer count = connectionCountsMap.getOrDefault(contentSource, 0);
        if (count > 0) {
            connectionCountsMap.put(contentSource, count - 1);
        }
    }

    protected synchronized void success(ContentSource contentSource) {
		errorCountsMap.remove(contentSource);
		errorTimeMap.remove(contentSource);
	}

    protected void error(ContentSource contentSource) {
        error(contentSource,-1);
    }

    protected synchronized void error(ContentSource contentSource, long allocTime) {
        if (contentSourceList.contains(contentSource)) {
            Long lastErrorTime = errorTimeMap.get(contentSource);
            if (lastErrorTime == null || allocTime <= 0 || allocTime > lastErrorTime) {
		        int count = errorCountsMap.getOrDefault(contentSource, 0) + 1;
                errorCountsMap.put(contentSource, count);
                errorTimeMap.put(contentSource, System.currentTimeMillis());

                LOG.log(WARNING, "Connection error count for ContentSource {0} is {1}. Max limit is {2}.", new Object[]{asString(contentSource), count, hostRetryLimit});
		        // if we haven't exhausted retries, replace this ContentSource with a fresh one (will re-bind and obtain IP, which can help with proxies with dynamic IP until XCC knows how to handle that better
                if (count > hostRetryLimit) {
                    removeInternal(contentSource);
                } else {
                    /* Due to issues with how ContentSource statically resolves the IP address of the host when constructed,
                    * dynamic pools of IP addresses for a given FQDN may not be used, and if a host is removed from a pool
                    * then persistent errors would be encountered.
                    */
                    renewContentSource(contentSource);
                }
            } else {
                LOG.log(WARNING, "Connection error for ContentSource {0} is not counted towards the limit as it was allocated before last error.", new Object[]{asString(contentSource)});
            }
        }
	}

    /**
     * Replace the specified ContentSource with a newly constructed one
     * @param contentSource the ContentSource to be replaced with a new instance
     */
    protected synchronized void renewContentSource(ContentSource contentSource) {
        if (replaceContentSourceOnError) {
            String xccConnectionString = connectionStringMap.get(contentSource);
            ContentSource freshContentSource = super.createContentSource(xccConnectionString);
            if (haveDifferentIP(contentSource, freshContentSource)) {
                replaceContentSource(contentSource, freshContentSource);
            }
        }
    }

    protected boolean haveDifferentIP(ContentSource contentSourceA, ContentSource contentSourceB) {
        boolean result = false;
        if (contentSourceB != null) {
            String currentIP = getIPAddress(contentSourceA);
            String freshIP = getIPAddress(contentSourceB);
            if (!currentIP.equals(freshIP)) {
                LOG.log(INFO, () -> String.format("%s IP changed from: %s to: %s", contentSourceA.getConnectionProvider().getHostName(), currentIP, freshIP));
                result = true;
            }
        }
        return result;
    }

    protected String getIPAddress(ContentSource contentSource) {
        String ip = null;
        ConnectionProvider connectionProvider = contentSource.getConnectionProvider();
        if (connectionProvider instanceof SingleHostAddress) {
            SingleHostAddress currentProvider = (SingleHostAddress) connectionProvider;
            ip = currentProvider.getAddress().getAddress().getHostAddress();
        }
        return ip;
    }

    /**
     * Replace the current ContentSource with a new instance
     * @param current
     * @param freshContentSource
     */
    protected void replaceContentSource(ContentSource current, ContentSource freshContentSource) {
        //replace the contentSource at the same position
        contentSourceList.set(contentSourceList.indexOf(current), freshContentSource);
        //then clear contentSource entries from the other tracking maps and create new entries with the contentSource values
        connectionStringMap.put(freshContentSource, connectionStringMap.get(current));
        connectionStringMap.remove(current);
        connectionCountsMap.put(freshContentSource, connectionCountsMap.getOrDefault(current, 0));
        connectionCountsMap.remove(current);
        errorCountsMap.put(freshContentSource, errorCountsMap.getOrDefault(current, 1));
        errorCountsMap.remove(current);
        errorTimeMap.put(freshContentSource, errorTimeMap.get(current));
        errorTimeMap.remove(current);
    }

    protected int errorCount(ContentSource contentSource) {
        return errorCountsMap.getOrDefault(contentSource, 0);
    }

    //this is not a proxy
    protected synchronized void removeInternal(ContentSource contentSource) {
        if (contentSourceList.contains(contentSource)) {
	        LOG.log(WARNING, "Removing the ContentSource {0} from the content source pool.", new Object[]{asString(contentSource)});
	        contentSourceList.remove(contentSource);
	        connectionCountsMap.remove(contentSource);
	        errorCountsMap.remove(contentSource);
	        errorTimeMap.remove(contentSource);
            connectionStringMap.remove(contentSource);
        }
    }

    //TODO: handle redaction if necessary?
    protected String asString(ContentSource contentSource) {
        return contentSource == null ? "null" : contentSource.toString();
    }

    //methods to create dynamic proxy instances.
    protected ContentSource createContentSourceProxy(ContentSource contentSource) {
        return (ContentSource) Proxy.newProxyInstance(
            DefaultContentSourcePool.class.getClassLoader(),
            new Class[] { ContentSource.class },
            new ContentSourceInvocationHandler(this, contentSource));
    }

    public static ContentSource getContentSourceFromProxy(ContentSource contentSourceProxy) {
		ContentSource target = contentSourceProxy;
		if (contentSourceProxy != null && Proxy.isProxyClass(contentSourceProxy.getClass())) {
			InvocationHandler handler = Proxy.getInvocationHandler(contentSourceProxy);
			if (handler instanceof ContentSourceInvocationHandler) {
				target = ((ContentSourceInvocationHandler)handler).target;
			}
		}
		return target;
	}

    public static Session getSessionFromProxy(Session sessionProxy) {
		Session target = sessionProxy;
		if (sessionProxy != null && Proxy.isProxyClass(sessionProxy.getClass())) {
			InvocationHandler handler = Proxy.getInvocationHandler(sessionProxy);
			if (handler instanceof SessionInvocationHandler) {
				target = ((SessionInvocationHandler)handler).target;
			}
		}
		return target;
	}

    //invocation handlers
    protected static class ContentSourceInvocationHandler implements InvocationHandler {
        static final String NEW_SESSION = "newSession";
        DefaultContentSourcePool contentSourcePool;
        ContentSource target;
        long allocTime;

        protected ContentSourceInvocationHandler(DefaultContentSourcePool contentSourcePool, ContentSource target) {
            this.contentSourcePool = contentSourcePool;
            this.target = target;
            this.allocTime = System.currentTimeMillis();
        }

        @Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			Object obj = method.invoke(target, args);
			if (obj != null && isNewSession(method) && obj instanceof Session) {
				obj = createSessionProxy((Session)obj);
			}
			return obj;
		}

        protected Session createSessionProxy(Session session) {
            return (Session)Proxy.newProxyInstance(
                DefaultContentSourcePool.class.getClassLoader(),
                new Class[] { Session.class },
                new SessionInvocationHandler(contentSourcePool, target, session, allocTime));
        }

        private boolean isNewSession(Method method) {
        		return NEW_SESSION.equals(method.getName());
        }
    }

    //TODO: This code does not handle explicit commits and rollbacks.
    protected static class SessionInvocationHandler implements InvocationHandler {
        static final String SUBMIT_REQUEST = "submitRequest";
        static final String INSERT_CONTENT = "insertContent";
        static final String COMMIT = "commit";
        static final String ROLLBACK = "rollback";
        static final String CLOSE = "close";
        static final String EMPTY_SEQ = "()";

        private final DefaultContentSourcePool contentSourcePool;
        private final ContentSource contentSource;
        private final Session target;
        private final long allocTime;

        private int attempts = 0;

        private Session retryProxy;

		protected SessionInvocationHandler(DefaultContentSourcePool contentSourcePool, ContentSource contentSource, Session target, long allocTime) {
			this.contentSourcePool = contentSourcePool;
			this.contentSource = contentSource;
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

				if (contentSourcePool.isLoadPolicy()) {
                    contentSourcePool.hold(contentSource);
                }
				attempts++;
			}
			try {
				if (retryProxy != null && isClose(method)) {
					retryProxy.close(); //Use proxy only as there can be multiple retry attempts in a chain.
				}
				Object obj = method.invoke(target, args);
				//TODO: connection is held longer for streaming result sequence even after request is submitted.
				//We are ok now as we only use streaming results for query uris loader.
				if (isSubmitRequest(method) || isInsertContent(method)) {
					contentSourcePool.success(contentSource);
					if (contentSourcePool.isLoadPolicy()) {
                        contentSourcePool.release(contentSource);
                    }
				}
				return obj;
			} catch (Exception exc) {
				return handleInvokeException(exc, method, args);
			}
		}

        protected Object handleInvokeException(Exception exc, Method method, Object[] args) throws Throwable {
            if (contentSourcePool.isLoadPolicy() && (isSubmitRequest(method) || isInsertContent(method))) {
                contentSourcePool.release(contentSource); //we should do this before the recursion. not finally.
            }
            if (exc instanceof InvocationTargetException) {
                if (exc.getCause() instanceof ServerConnectionException) {
                    contentSourcePool.error(contentSource, allocTime); //we should do this before the recursion.. not finally.

                    String name = exc.getCause().getClass().getSimpleName();
                    if (isSubmitRequest(method) && attempts <= contentSourcePool.retryLimit) {
                        LOG.log(WARNING, "Submit request failed {0} times with {1}. Max Limit is {2}. Retrying..", new Object[]{attempts, name, contentSourcePool.retryLimit});
                        return submitAsNewRequest(args);
                    } else if (isInsertContent(method) && attempts <= contentSourcePool.retryLimit) {
                        LOG.log(WARNING, "Insert content failed {0} times {1}. Max Limit is {2}. Retrying..", new Object[]{attempts, name, contentSourcePool.retryLimit});
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

		protected void validRequest(Object... args) {
			Request request = (Request)args[0];
			if (!(request instanceof AdhocQuery || request instanceof ModuleInvoke)) {
				throw new IllegalArgumentException("Only moduleInvoke or adhocQuery requests are supported by CoRB");
			}
		}

		protected Object submitAsNewRequest(Object... args) throws RequestException {
			Request request = (Request)args[0];
			try {
				retryProxy = contentSourcePool.get().newSession();
				setAttemptsToNewSession(retryProxy);
				Request newRequest;
				if (request instanceof AdhocQuery) {
					newRequest = retryProxy.newAdhocQuery(((AdhocQuery)request).getQuery());
				} else {
					newRequest = retryProxy.newModuleInvoke(((ModuleInvoke)request).getModuleUri());
				}
				newRequest.setOptions(request.getOptions());
                for (XdmVariable xdmVariable: request.getVariables()) {
                    newRequest.setVariable(xdmVariable);
                }
				return retryProxy.submitRequest(newRequest);
			} catch (CorbException exc) {
				throw new RequestException(exc.getMessage(),request,exc);
			}
		}

		protected Object insertAsNewRequest(Object... args) throws RequestException {
			try {
				retryProxy = contentSourcePool.get().newSession();
				setAttemptsToNewSession(retryProxy);
				if (args[0] instanceof Content) {
					retryProxy.insertContent((Content)args[0]);
				} else if (args[0] instanceof Content[]) {
					retryProxy.insertContent((Content[])args[0]);
				}
				return null;
			} catch (CorbException exc) {
				throw new RequestException(exc.getMessage(), target.newAdhocQuery(EMPTY_SEQ), exc);
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
				throw new UnsupportedOperationException(method.getName() + " is not supported by " + getClass().getName());
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
