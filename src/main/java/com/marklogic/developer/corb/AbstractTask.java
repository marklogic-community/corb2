/*
 * Copyright 2005-2015 MarkLogic Corporation
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.QueryException;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RetryableQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;

import java.util.logging.Level;
import java.util.logging.Logger;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

/**
 * 
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public abstract class AbstractTask implements Task {
	private static final Object ERROR_SYNC_OBJ = new Object();
	
	protected static final String TRUE = "true";
	protected static final String FALSE = "false";
	protected static final byte[] NEWLINE = 
			System.getProperty("line.separator") != null ? System.getProperty("line.separator").getBytes() : "\n".getBytes();
	private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

	protected ContentSource cs;
	protected String moduleType;
	protected String moduleUri;
	protected Properties properties;
	protected String[] inputUris;

	protected String adhocQuery;
	protected String language;
	
	protected String exportDir;

	private static final Object SYNC_OBJ = new Object();
	protected static final Map<String, Set<String>> MODULE_PROPS = new HashMap<String, Set<String>>();

	protected static final int DEFAULT_CONNECTION_RETRY_LIMIT = 3;
	protected static final int DEFAULT_CONNECTION_RETRY_INTERVAL = 60;
	
	protected static final int DEFAULT_QUERY_RETRY_LIMIT = 2;
	protected static final int DEFAULT_QUERY_RETRY_INTERVAL = 15;

	protected int connectRetryCount = 0;
	
	protected boolean failOnError = true; 

	private static final Logger LOG = Logger.getLogger(AbstractTask.class.getSimpleName());

	@Override
	public void setContentSource(ContentSource cs) {
		this.cs = cs;
	}

	@Override
	public void setModuleType(String moduleType) {
		this.moduleType = moduleType;
	}

	@Override
	public void setModuleURI(String moduleUri) {
		this.moduleUri = moduleUri;
	}

	@Override
	public void setAdhocQuery(String adhocQuery) {
		this.adhocQuery = adhocQuery;
	}

	@Override
	public void setQueryLanguage(String language) {
		this.language = language;
	}

	@Override
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void setInputURI(String[] inputUri) {
		this.inputUris = inputUri != null ? inputUri.clone() : new String[]{};
	}
	
    @Override
	public void setFailOnError(boolean failOnError){
		this.failOnError = failOnError;
	}
	
    @Override
	public void setExportDir(String exportFileDir) {
		this.exportDir = exportFileDir;
	}

	public String getExportDir() {
		return this.exportDir;
	}

	public Session newSession() {
		return cs.newSession();
	}
	
    @Override
	public String[] call() throws Exception {
		try {
			return invokeModule();
		} finally {
			cleanup();
		}
	}

	protected String[] invokeModule() throws CorbException {
		if (moduleUri == null && adhocQuery == null) {
			return null;
		}

		Session session = null;
		ResultSequence seq = null;
		Thread.yield();// try to avoid thread starvation
		try {
			session = newSession();
			Request request = null;

			Set<String> modulePropNames = MODULE_PROPS.get(moduleType);
			if (modulePropNames == null) {
				synchronized (SYNC_OBJ) {
					modulePropNames = MODULE_PROPS.get(moduleType);
					if (modulePropNames == null) {
						HashSet<String> propSet = new HashSet<String>();
						if (properties != null) {
							for (String propName : properties.stringPropertyNames()) {
								if (propName.startsWith(moduleType + ".")) {
									propSet.add(propName);
								}
							}
						}
						for (String propName : System.getProperties().stringPropertyNames()) {
							if (propName.startsWith(moduleType + ".")) {
								propSet.add(propName);
							}
						}
						MODULE_PROPS.put(moduleType, modulePropNames = propSet);
					}
				}
			}

			if (moduleUri == null) {
				request = session.newAdhocQuery(adhocQuery);
			} else {
				request = session.newModuleInvoke(moduleUri);
			}

			if (language != null) {
				request.getOptions().setQueryLanguage(language);
			}

			if (inputUris != null && inputUris.length > 0) {
				if (inputUris.length == 1) {
					request.setNewStringVariable("URI", inputUris[0]);
				} else {
					String delim = getProperty("BATCH-URI-DELIM");
					if (delim == null || delim.length() == 0) {
						delim = Manager.DEFAULT_BATCH_URI_DELIM;
					}
					StringBuffer buff = new StringBuffer();
					for (String uri : inputUris) {
						if (buff.length() > 0) {
							buff.append(delim);
						}
						buff.append(uri);
					}
					request.setNewStringVariable("URI", buff.toString());
				}
			}

			if (properties != null && properties.containsKey(Manager.URIS_BATCH_REF)) {
				request.setNewStringVariable(Manager.URIS_BATCH_REF, properties.getProperty(Manager.URIS_BATCH_REF));
			}

			for (String propName : modulePropNames) {
				if (propName.startsWith(moduleType + ".")) {
					String varName = propName.substring(moduleType.length() + 1);
					String value = getProperty(propName);
					if (value != null) {
						request.setNewStringVariable(varName, value);
					}
				}
			}

			Thread.yield();// try to avoid thread starvation
			seq = session.submitRequest(request);
			connectRetryCount = 0;
			// no need to hold on to the session as results will be cached.
			session.close();
			Thread.yield();// try to avoid thread starvation

			processResult(seq);
			seq.close();
			Thread.yield();// try to avoid thread starvation

			return inputUris;
		} catch (RequestException exc) {
      return handleRequestException(exc);  
    } catch (Exception exc) {
			throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
		} finally {
			if (null != session && !session.isClosed()) {
				session.close();
				session = null;
			}
			if (null != seq && !seq.isClosed()) {
				seq.close();
				seq = null;
			}
			Thread.yield();// try to avoid thread starvation
		}
	}
	
  protected String[] handleRequestException(RequestException exc) throws CorbException {
    String name = exc.getClass().getSimpleName();    
    if(exc instanceof ServerConnectionException || 
    		exc instanceof RetryableQueryException || 
    		(exc instanceof QueryException && ((QueryException)exc).isRetryable())) {
    	int retryLimit = exc instanceof ServerConnectionException ? this.getConnectRetryLimit() : this.getQueryRetryLimit();
      int retryInterval = exc instanceof ServerConnectionException ? this.getConnectRetryInterval() : this.getQueryRetryInterval();
      if (connectRetryCount < retryLimit) {
          connectRetryCount++;
          LOG.log(Level.WARNING,
                  "Encountered " + name + " from Marklogic Server. Retrying attempt {0} after {1} seconds..: {2} at URI: {3}",
                  new Object[]{connectRetryCount, retryInterval, exc.getMessage(), asString(inputUris)});
          try {
          	Thread.sleep(retryInterval * 1000L);
          } catch (Exception exc2) {
          }
          return invokeModule();
      } else if(exc instanceof ServerConnectionException || failOnError){	
          throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
      }else{
      	LOG.log(Level.WARNING, "failOnError is false. Encountered " + name + " at URI: " + asString(inputUris), exc);
        writeToErrorFile(inputUris, exc.getMessage());
        return inputUris;
      }
    }else{   
	    if (failOnError) {
	        throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
	    } else {
	        LOG.log(Level.WARNING, "failOnError is false. Encountered " + name + " at URI: " + asString(inputUris), exc);
	        writeToErrorFile(inputUris, exc.getMessage());
	        return inputUris;
	    }
    }
  }
    
  protected String asString(String[] uris) {
    if (uris == null || uris.length == 0) {
    	return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < uris.length; i++) {
    	if (i > 0) {
    		sb.append(',');
      }
      sb.append(uris[i]);
    }
    return sb.toString();
  }

	protected abstract String processResult(ResultSequence seq) throws CorbException;

	protected void cleanup() {
		// release resources
		cs = null;
		moduleType = null;
		moduleUri = null;
		properties = null;
		inputUris = null;
		adhocQuery = null;
		language = null;
		exportDir = null;
	}

	public String getProperty(String key) {
		String val = System.getProperty(key);
		if (val == null && properties != null) {
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : null;
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

	private int getConnectRetryLimit() {
		int connectRetryLimit = getIntProperty("XCC-CONNECTION-RETRY-LIMIT");
		return connectRetryLimit < 0 ? DEFAULT_CONNECTION_RETRY_LIMIT : connectRetryLimit;
	}

	private int getConnectRetryInterval() {
		int connectRetryInterval = getIntProperty("XCC-CONNECTION-RETRY-INTERVAL");
		return connectRetryInterval < 0 ? DEFAULT_CONNECTION_RETRY_INTERVAL : connectRetryInterval;
	}
	
	private int getQueryRetryLimit() {
		int queryRetryLimit = getIntProperty("QUERY-RETRY-LIMIT");
		return queryRetryLimit < 0 ? DEFAULT_QUERY_RETRY_LIMIT : queryRetryLimit;
	}

	private int getQueryRetryInterval() {
		int queryRetryInterval = getIntProperty("QUERY-RETRY-INTERVAL");
		return queryRetryInterval < 0 ? DEFAULT_QUERY_RETRY_INTERVAL : queryRetryInterval;
	}
	
    /**
    * Retrieves an int value.
    * @param key The key name.
    * @return The requested value (<code>-1</code> if not found or could not parse value as int).
    */
    protected int getIntProperty(String key) {
        int intVal = -1;
		String value = getProperty(key);
		if (isNotEmpty(value)) {
			try {
				intVal = Integer.parseInt(value);
			} catch (Exception exc) {
                LOG.log(Level.WARNING, "Unable to parese ''{0}'' value ''{1}'' as an int", new Object[]{key, value});
			}
		}
        return intVal;
    }
    
	private void writeToErrorFile(String[] uris, String message){
		if (uris == null || uris.length == 0) { return; }
		
		String errorFileName = getProperty("ERROR-FILE-NAME");
		if (errorFileName == null || errorFileName.length() == 0) { return; }
		
		String delim = getProperty("BATCH-URI-DELIM");
		if (delim == null || delim.length() == 0) {
			delim = Manager.DEFAULT_BATCH_URI_DELIM;
		}
		
		synchronized(ERROR_SYNC_OBJ){
			BufferedOutputStream writer = null;
			try {
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, errorFileName), true));
        for (String uri : uris) {
            writer.write(uri.getBytes());
            if (isNotEmpty(message)) {
                writer.write(delim.getBytes());
                writer.write(message.getBytes());
            }       
            writer.write(NEWLINE);
        }
        writer.flush();
			} catch(Exception exc) {
				LOG.log(Level.SEVERE, "Problem writing uris to ERROR-FILE-NAME",exc);
			} finally {
                closeQuietly(writer);
			}
		}
	}
	
}
