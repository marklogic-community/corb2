/*
 * Copyright (c) 2004-2015 MarkLogic Corporation
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
import java.util.Map;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.isAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public class TaskFactory {

    protected Manager manager;
    private final Map<String, String> moduleToAdhocQueryMap = new HashMap<String, String>();
    private final Map<String, String> moduleToPathMap = new HashMap<String, String>();

    /**
     * @param manager
     */
    public TaskFactory(Manager manager) {
        this.manager = manager;
    }

    public Task newProcessTask(String[] uris) {
        return newProcessTask(uris, true);
    }

    public Task newProcessTask(String[] uris, boolean failOnError) {
        TransformOptions options = manager.getOptions();
        if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
            throw new NullPointerException("null process task and xquery module");
        }
        if (null != options.getProcessModule()
                && (null == uris || uris.length == 0 || null == manager.getContentSource())) {
            throw new NullPointerException("null content source or input uri");
        }
        try {
            Task task = options.getProcessTaskClass() == null ? new Transform() : options.getProcessTaskClass().newInstance();
            setupTask(task, "PROCESS-MODULE", options.getProcessModule(), uris, failOnError);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    public Task newPreBatchTask() {
        TransformOptions options = manager.getOptions();
        if (null == options.getPreBatchTaskClass() && null == options.getPreBatchModule()) {
            return null;
        }
        if (null != options.getPreBatchModule() && null == manager.getContentSource()) {
            throw new NullPointerException("null content source");
        }
        try {
            Task task = options.getPreBatchTaskClass() == null ? new Transform() : options.getPreBatchTaskClass().newInstance();
            setupTask(task, "PRE-BATCH-MODULE", options.getPreBatchModule(), new String[0]);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    public Task newPostBatchTask() {
        TransformOptions options = manager.getOptions();
        if (null == options.getPostBatchTaskClass() && null == options.getPostBatchModule()) {
            return null;
        }
        if (null != options.getPostBatchModule() && null == manager.getContentSource()) {
            throw new NullPointerException("null content source");
        }
        try {
            Task task = options.getPostBatchTaskClass() == null ? new Transform() : options.getPostBatchTaskClass().newInstance();
            setupTask(task, "POST-BATCH-MODULE", options.getPostBatchModule(), new String[0]);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    public Task newInitTask() {
        TransformOptions options = manager.getOptions();
        if (null == manager.getOptions().getInitTaskClass() && null == options.getInitModule()) {
            return null;
        }
        if (null != options.getInitModule() && null == manager.getContentSource()) {
            throw new NullPointerException("null content source");
        }
        try {
            Task task = options.getInitTaskClass() == null ? new Transform() : options.getInitTaskClass().newInstance();
            setupTask(task, "INIT-MODULE", options.getInitModule(), new String[0]);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    private void setupTask(Task task, String moduleType, String module, String[] uris) {
        setupTask(task, moduleType, module, uris, true);
    }

    private void setupTask(Task task, String moduleType, String module, String[] uris, boolean failOnError) {
        if (module != null) {
            if (isAdhoc(module)) {
                String adhocQuery = moduleToAdhocQueryMap.get(module);
                if (adhocQuery == null) {
                    String modulePath = module.substring(0, module.indexOf('|'));
                    adhocQuery = AbstractManager.getAdhocQuery(modulePath);
                    if (adhocQuery == null || (adhocQuery.length() == 0)) {
                        throw new IllegalStateException("Unable to read adhoc query " + module + " from classpath or filesystem");
                    }
                    moduleToAdhocQueryMap.put(module, adhocQuery);
                }
                task.setAdhocQuery(adhocQuery);
            } else {
                String modulePath = moduleToPathMap.get(module);
                if (modulePath == null) {
                    String root = manager.getOptions().getModuleRoot();
                    modulePath = buildModulePath(root, module);
                    moduleToPathMap.put(module, modulePath);
                }
                task.setModuleURI(modulePath);
            }
            if (isJavaScriptModule(module)) {
                task.setQueryLanguage("javascript");
            }
        }
        task.setModuleType(moduleType);
        task.setContentSource(manager.contentSource);
        task.setProperties(manager.properties);
        task.setInputURI(uris);
        task.setFailOnError(failOnError);
        task.setExportDir(manager.getOptions().getExportFileDir());

        if (task instanceof ExportBatchToFileTask) {
            String fileName = ((ExportBatchToFileTask) task).getFileName();
            if (isBlank(fileName)) {
                throw new IllegalArgumentException("No file name for ExportBatchToFileTask");
            }
        }
    }
}
