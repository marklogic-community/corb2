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
        	if(manager.getOptions().getProcessModule() != null){
        		String root = manager.getOptions().getModuleRoot();
        		String module = manager.getOptions().getProcessModule();
        		module = module.substring(module.lastIndexOf("/")+1);
        		task.setModuleURI(root + module);
        	}
        	setupTask(task,_uri);
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    public Task newPostBatchTask(String _uri){
    	if(null == manager.getOptions().getPostBatchTaskClass() && null == manager.getOptions().getPostBatchModule()){
    		throw new NullPointerException("null post batch task and module");
    	}
    	if(null != manager.getOptions().getPostBatchModule() && null == manager.getContentSource()){
    		throw new NullPointerException("null content source");
    	}
        try{
        	Task task = manager.getOptions().getPostBatchTaskClass() == null ? 
        				new Transform() : manager.getOptions().getPostBatchTaskClass().newInstance();
        	if(manager.getOptions().getPostBatchModule() != null){
        		String root = manager.getOptions().getModuleRoot();
        		String module = manager.getOptions().getPostBatchModule();
        		module = module.substring(module.lastIndexOf("/")+1);
        		task.setModuleURI(root + module);
        	}
        	setupTask(task,_uri);
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    private void setupTask(Task task, String _uri){
    	task.setContentSource(manager.getContentSource());
    	task.setProperties(manager.getProperties());
    	task.setInputURI(_uri);
    	if(task instanceof ExportToFileTask){
    		((ExportToFileTask)task).setExportDir(manager.getOptions().getExportFileDir());
    	}
    }
}
