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
        	setupTask(task,manager.getOptions().getProcessModule(),_uri);
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    public Task newPreBatchTask(){
    	if(null == manager.getOptions().getPreBatchTaskClass() && null == manager.getOptions().getPreBatchModule()){
    		throw new NullPointerException("null pre batch task and module");
    	}
    	if(null != manager.getOptions().getPreBatchModule() && null == manager.getContentSource()){
    		throw new NullPointerException("null content source");
    	}
        try{
        	Task task = manager.getOptions().getPreBatchTaskClass() == null ? 
        				new Transform() : manager.getOptions().getPreBatchTaskClass().newInstance();
        	setupTask(task,manager.getOptions().getPreBatchModule(),"");
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    public Task newPostBatchTask(){
    	if(null == manager.getOptions().getPostBatchTaskClass() && null == manager.getOptions().getPostBatchModule()){
    		throw new NullPointerException("null post batch task and module");
    	}
    	if(null != manager.getOptions().getPostBatchModule() && null == manager.getContentSource()){
    		throw new NullPointerException("null content source");
    	}
        try{
        	Task task = manager.getOptions().getPostBatchTaskClass() == null ? 
        				new Transform() : manager.getOptions().getPostBatchTaskClass().newInstance();
        	setupTask(task,manager.getOptions().getPostBatchModule(),"");
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }
    
    private void setupTask(Task task, String module, String _uri){
    	if(module != null){
    		String root = manager.getOptions().getModuleRoot();
    		module = module.substring(module.lastIndexOf("/")+1);
    		task.setModuleURI(root + module);
    	}
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
