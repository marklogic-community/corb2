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
	 * 
	 * @param moduleType
	 */
	public void setModuleType(String moduleType);

	public void setModuleURI(String moduleURI);

	public void setAdhocQuery(String adhocQuery);

	public void setQueryLanguage(String language);

	/**
	 * If additional data is sent from CUSTOM URI module, it is available in
	 * properties with key URIS_BATCH_REF
	 * 
	 * @param props
	 */
	public void setProperties(Properties props);

	public void setInputURI(String[] inputUri);

	public void setFailOnError(boolean failOnError);
	
	public void setExportDir(String exportFileDir);
}
