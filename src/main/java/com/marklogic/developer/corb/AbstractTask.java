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
import com.marklogic.xcc.exceptions.RequestPermissionException;
import com.marklogic.xcc.exceptions.RequestServerException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public abstract class AbstractTask implements Task {
	private static final Object ERROR_SYNC_OBJ = new Object();
	
	protected static final String TRUE = "true";
	protected static final String FALSE = "false";
	protected static final byte[] NEWLINE = "\n".getBytes();
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
	private static final Map<String, Set<String>> MODULE_PROPS = new HashMap<String, Set<String>>();

	protected static final int DEFAULT_RETRY_LIMIT = 3;
	protected static final int DEFAULT_RETRY_INTERVAL = 60;

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
		this.inputUris = inputUri;
	}
	
	public void setFailOnError(boolean failOnError){
		this.failOnError = failOnError;
	}
	
	public void setExportDir(String exportFileDir) {
		this.exportDir = exportFileDir;
	}

	public String getExportDir() {
		return this.exportDir;
	}

	public Session newSession() {
		return cs.newSession();
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
		}catch(ServerConnectionException exc){
			int retryLimit = this.getConnectRetryLimit();
			int retryInterval = this.getConnectRetryInterval();
			if (connectRetryCount < retryLimit) {
				connectRetryCount++;
				LOG.log(Level.SEVERE,
						"Connection failed to Marklogic Server. Retrying attempt {0} after {1} seconds..: {2} at URI: {3}",
						new Object[] { connectRetryCount, retryInterval, exc.getMessage(), asString(inputUris) });
				try {
					Thread.sleep(retryInterval * 1000L);
				} catch (Exception exc2) {}
				return invokeModule();
			} else {
				throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
			}
		}catch(RequestServerException exc){
			if(failOnError){
				throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
			}else{
				LOG.log(Level.WARNING,"failOnErroris is false. Encountered server exception at URI: "+ asString(inputUris),exc);
				writeToErrorFile(inputUris,exc.getMessage());
				return inputUris;
			}
		}catch(RequestPermissionException exc){
			if(failOnError){
				throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
			}else{
				LOG.log(Level.WARNING,"failOnErroris is false. Encountered permission exception at URI: "+ asString(inputUris),exc);
				writeToErrorFile(inputUris,exc.getMessage());
				return inputUris;
			}
		}catch(Exception exc){
			throw new CorbException(exc.getMessage() + " at URI: " + asString(inputUris), exc);
		}finally{
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
	
	protected String asString(String[] uris){
    if(uris == null || uris.length == 0) return "";
    StringBuilder sb = new StringBuilder();
    for(int i=0; i < uris.length; i++){
    	if (i > 0) sb.append(',');
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
		int connectRetryLimit = -1;
		String propStr = getProperty("XCC-CONNECTION-RETRY-LIMIT");
		if (propStr != null && propStr.length() > 0) {
			try {
				connectRetryLimit = Integer.parseInt(propStr);
			} catch (Exception exc) {
			}
		}
		return connectRetryLimit < 0 ? DEFAULT_RETRY_LIMIT : connectRetryLimit;
	}

	private int getConnectRetryInterval() {
		int connectRetryInterval = -1;
		String propStr = getProperty("XCC-CONNECTION-RETRY-INTERVAL");
		if (propStr != null && propStr.length() > 0) {
			try {
				connectRetryInterval = Integer.parseInt(propStr);
			} catch (Exception exc) {
			}
		}
		return connectRetryInterval < 0 ? DEFAULT_RETRY_INTERVAL : connectRetryInterval;
	}
	
	private void writeToErrorFile(String[] uris, String message){
		if(uris == null || uris.length == 0) return;
		
		String errorFileName = getProperty("ERROR-FILE-NAME");
		if(errorFileName == null || errorFileName.length() == 0) return;
		
		String delim = getProperty("BATCH-URI-DELIM");
		if (delim == null || delim.length() == 0) {
			delim = Manager.DEFAULT_BATCH_URI_DELIM;
		}
		
		synchronized(ERROR_SYNC_OBJ){
			BufferedOutputStream writer = null;
			try{
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,errorFileName), true));
				for (int i=0; i< uris.length;i++) {
            writer.write(uris[i].getBytes());
            if(message != null && message.length() > 0){
            	writer.write(delim.getBytes());
            	writer.write(message.getBytes());
            }
            writer.write(NEWLINE);
        }
        writer.flush();
			}catch(Exception exc){
				LOG.log(Level.SEVERE, "Problem writing uris to ERROR-FILE-NAME",exc);
			}finally{
				try{
					if(writer != null) writer.close();
				}catch(Exception exc){}
			}
		}
	}
}
