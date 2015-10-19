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

import java.util.HashMap;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public class TaskFactory {
    protected Manager manager;
    private HashMap<String, String> moduleToAdhocQueryMap = new HashMap<>();
    private HashMap<String, String> moduleToPathMap = new HashMap<>();

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
    public Task newProcessTask(String[] _uris) {
        if (null == manager.getOptions().getProcessTaskClass() && null == manager.getOptions().getProcessModule()) {
            throw new NullPointerException("null process task and xquery module");
        }
        if (null != manager.getOptions().getProcessModule() && (null == _uris || _uris.length == 0 || null == manager.getContentSource())) {
            throw new NullPointerException("null content source or input uri");
        }
        try {
            Task task = manager.getOptions().getProcessTaskClass() == null
                    ? new Transform() : manager.getOptions().getProcessTaskClass().newInstance();
            setupTask(task, "XQUERY-MODULE", manager.getOptions().getProcessModule(), _uris);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
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
        	setupTask(task,"PRE-BATCH-MODULE",manager.getOptions().getPreBatchModule(), new String[0]);
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
        	setupTask(task,"POST-BATCH-MODULE",manager.getOptions().getPostBatchModule(), new String[0]);
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
        	setupTask(task,"INIT-MODULE",manager.getOptions().getInitModule(), new String[0]);
        	return task;
        }catch(Exception exc){
        	throw new IllegalArgumentException(exc.getMessage(),exc);
        }
    }

    private void setupTask(Task task, String moduleType, String module, String[] _uri) {
        if (module != null) {
            if (module.toUpperCase().endsWith("|ADHOC")) {
                String modulePath = module.substring(0, module.indexOf('|'));
                String adhocQuery = moduleToAdhocQueryMap.get(modulePath);
                if (adhocQuery == null) {
                    adhocQuery = Manager.getAdhocQuery(modulePath);
                    if (adhocQuery == null || (adhocQuery.length() == 0)) {
                        throw new IllegalStateException("Unable to read adhoc query " + module + " from classpath or filesystem");
                    }
                    moduleToAdhocQueryMap.put(modulePath, adhocQuery);
                }
                task.setAdhocQuery(adhocQuery);
                if (modulePath.toUpperCase().endsWith(".SJS") || modulePath.toUpperCase().endsWith(".JS")) {
                    task.setQueryLanguage("javascript");
                }
            } else {
                String modulePath = moduleToPathMap.get(module);
                if (modulePath == null) {
                    String root = manager.getOptions().getModuleRoot();
                    if (!root.endsWith("/")) {
                        root = root + "/";
                    }
                    if (module.startsWith("/") && module.length() > 1) {
                        module = module.substring(1);
                    }
                    modulePath = root + module;
                    moduleToPathMap.put(module, modulePath);
                }
                task.setModuleURI(modulePath);

                if(module.toUpperCase().endsWith(".SJS") || module.toUpperCase().endsWith(".JS")){
                    task.setQueryLanguage("javascript");
                }
            }
        }
        task.setModuleType(moduleType);
        task.setContentSource(manager.getContentSource());
        task.setProperties(manager.getProperties());
        task.setInputURI(_uri);
        if (task instanceof ExportToFileTask) {
            ((ExportToFileTask) task).setExportDir(manager.getOptions().getExportFileDir());
        }
        if (task instanceof ExportBatchToFileTask) {
            String fileName = ((ExportBatchToFileTask) task).getFileName();
            if (fileName == null || fileName.trim().length() == 0) {
                throw new IllegalArgumentException("No file name for ExportBatchToFileTask");
            }
        }
    }
}
