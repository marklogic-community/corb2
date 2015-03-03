/*
 * Copyright (c)2005-2008 Mark Logic Corporation
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public class TaskFactory {
    protected Manager manager;

    /**
     * @param _cs
     * @param _uri
     */
    public TaskFactory(Manager manager) {
    	this.manager = manager;
    }
    

    /**
     * @param _uri
     * @return
     */
    public Task newProcessTask(String _uri) {
    	if(null == manager.getOptions().getProcessTaskClass() && null == manager.getOptions().getProcessModule()){
    		throw new NullPointerException("null process task and xquery module");
    	}
    	if(null != manager.getOptions().getProcessModule() && (null == _uri || null == manager.getContentSource())){
    		throw new NullPointerException("null content source or input uri");
    	}
        try{
        	Task task = manager.getOptions().getProcessTaskClass() == null ? 
        				new Transform() : manager.getOptions().getProcessTaskClass().newInstance();
        	setupTask(task,"XQUERY-MODULE",manager.getOptions().getProcessModule(),_uri);
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    public Task newPreBatchTask(){
    	if(null == manager.getOptions().getPreBatchTaskClass() && null == manager.getOptions().getPreBatchModule()){
    		return null;
    	}
    	if(null != manager.getOptions().getPreBatchModule() && null == manager.getContentSource()){
    		throw new NullPointerException("null content source");
    	}
        try{
        	Task task = manager.getOptions().getPreBatchTaskClass() == null ? 
        				new Transform() : manager.getOptions().getPreBatchTaskClass().newInstance();
        	setupTask(task,"PRE-BATCH-MODULE",manager.getOptions().getPreBatchModule(),"");
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    public Task newPostBatchTask(){
    	if(null == manager.getOptions().getPostBatchTaskClass() && null == manager.getOptions().getPostBatchModule()){
    		return null;
    	}
    	if(null != manager.getOptions().getPostBatchModule() && null == manager.getContentSource()){
    		throw new NullPointerException("null content source");
    	}
        try{
        	Task task = manager.getOptions().getPostBatchTaskClass() == null ? 
        				new Transform() : manager.getOptions().getPostBatchTaskClass().newInstance();
        	setupTask(task,"POST-BATCH-MODULE",manager.getOptions().getPostBatchModule(),"");
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    public Task newInitTask(){
    	if(null == manager.getOptions().getInitTaskClass() && null == manager.getOptions().getInitModule()){
    		return null;
    	}
    	if(null != manager.getOptions().getInitModule() && null == manager.getContentSource()){
    		throw new NullPointerException("null content source");
    	}
        try{
        	Task task = manager.getOptions().getInitTaskClass() == null ? 
        				new Transform() : manager.getOptions().getInitTaskClass().newInstance();
        	setupTask(task,"INIT-MODULE",manager.getOptions().getInitModule(),"");
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    static public String getAdhocQuery(String module){
    	InputStream is = null;
    	InputStreamReader reader = null;
		StringWriter writer =null;
		try{
			is = TaskFactory.class.getResourceAsStream("/" + module);
			if(is == null){
				File f = new File(module);
				if (f.exists() && !f.isDirectory()) {
					is = new FileInputStream(f);
				}else{
					throw new IllegalStateException("Unable to find adhoc query module "+module+" in classpath or filesystem");
				}
			}
			
			reader = new InputStreamReader(is);
			writer = new StringWriter();
			char[] buffer = new char[512];
			int n = 0;
			while (-1 != (n = reader.read(buffer))) {
				writer.write(buffer, 0, n);
			}
			writer.close();
			reader.close();
			
			return writer.toString().trim();
		}catch(IOException exc){
			throw new IllegalStateException("Prolem reading adhoc query module "+module,exc);
		}finally{
			try{if(writer != null) writer.close();}catch(Exception exc){}
			try{if(reader != null) reader.close();}catch(Exception exc){}
			try{if(is != null ) is.close();}catch(Exception exc){}
		}
    }
    
    private void setupTask(Task task, String moduleType, String module, String _uri){
    	if(module != null){
    		if(module.toUpperCase().endsWith("|ADHOC")){
    			String adhocQuery = getAdhocQuery(module.substring(0, module.indexOf('|')));
    			if(adhocQuery == null || (adhocQuery.length() == 0)){
    				throw new IllegalStateException("Unable to read adhoc query "+module+" from classpath or filesystem");
    			}
    			task.setAdhocQuery(adhocQuery);
    		}else{
    			String root = manager.getOptions().getModuleRoot();
    			task.setModuleURI(root + module);
    		}
    	}
    	task.setModuleType(moduleType);
    	task.setContentSource(manager.getContentSource());
    	task.setProperties(manager.getProperties());
    	task.setInputURI(_uri);
    	if(task instanceof ExportToFileTask){
    		((ExportToFileTask)task).setExportDir(manager.getOptions().getExportFileDir());
    	}
    	if(task instanceof ExportBatchToFileTask){
    		String fileName = ((ExportBatchToFileTask)task).getFileName();
    		if(fileName == null || fileName.trim().length() == 0){
    			throw new IllegalArgumentException("No file name for ExportBatchToFileTask");
    		}
    	}
    }
}
