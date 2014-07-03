package com.marklogic.developer.corb;

import java.util.Properties;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
import java.util.concurrent.Callable;

import com.marklogic.xcc.ContentSource;

public interface Task extends Callable<String> {
	
	public void setContentSource(ContentSource cs);
	
	public void setModuleURI(String moduleURI);
	
	/**
	 * If additional data is sent from CUSTOM URI module, it is available in properties
	 * with key URIS_MODULE_METADATA 
	 * @param props
	 */
	public void setProperties(Properties props);
	
	public void setInputURI(String inputUri);
}
