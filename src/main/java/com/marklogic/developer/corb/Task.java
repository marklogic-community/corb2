package com.marklogic.developer.corb;

import java.util.Properties;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
import java.util.concurrent.Callable;

import com.marklogic.xcc.ContentSource;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public interface Task extends Callable<String[]> {
	
	public void setContentSource(ContentSource cs);
	
	/**
	 * XQUERY-MODULE,PRE-BATCH-MODULE,POST-BATCH-MODULE,INIT-MODULE
	 * @param moduleType
	 */
	public void setModuleType(String moduleType);
		
	public void setModuleURI(String moduleURI);
	
	public void setAdhocQuery(String adhocQuery);
	
	public void setAdhocQueryLanguage(String language);
		
	/**
	 * If additional data is sent from CUSTOM URI module, it is available in properties
	 * with key URIS_BATCH_REF 
	 * @param props
	 */
	public void setProperties(Properties props);
	
	public void setInputURI(String[] inputUri);
}
