/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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

import static com.marklogic.developer.corb.AbstractManager.getAdhocQuery;
import static com.marklogic.developer.corb.Options.INIT_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.XCC_TIME_ZONE;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.getInlineModuleCode;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isInlineModule;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public class TaskFactory {

    protected Manager manager;
    private final Map<String, String> moduleToAdhocQueryMap = new HashMap<>();
    private final Map<String, String> moduleToPathMap = new HashMap<>();
    private static final String EXCEPTION_MSG_UNABLE_READ_ADHOC = "Unable to read adhoc query ";
    private static final String EXCEPTION_MSG_NULL_CONTENT = "null content source";
    /**
     * @param manager
     */
    public TaskFactory(Manager manager) {
        this.manager = manager;
    }

    public Task newProcessTask(String... uris) {
        return newProcessTask(uris, true);
    }

    public Task newProcessTask(String[] uris, boolean failOnError) {
        TransformOptions options = manager.getOptions();
        if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
            throw new NullPointerException("null process task and xquery module");
        }
        if (null != options.getProcessModule()
                && (null == uris || uris.length == 0 || null == manager.getContentSourcePool())) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT + " or input uri");
        }
        try {
            Task task = options.getProcessTaskClass() == null ? new Transform() : options.getProcessTaskClass().newInstance();
            setupTask(task, PROCESS_MODULE, options.getProcessModule(), uris, failOnError);
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
        if (null != options.getPreBatchModule() && null == manager.getContentSourcePool()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getPreBatchTaskClass() == null ? new Transform() : options.getPreBatchTaskClass().newInstance();
            setupTask(task, PRE_BATCH_MODULE, options.getPreBatchModule());
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
        if (null != options.getPostBatchModule() && null == manager.getContentSourcePool()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getPostBatchTaskClass() == null ? new Transform() : options.getPostBatchTaskClass().newInstance();
            setupTask(task, POST_BATCH_MODULE, options.getPostBatchModule());
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
        if (null != options.getInitModule() && null == manager.getContentSourcePool()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getInitTaskClass() == null ? new Transform() : options.getInitTaskClass().newInstance();
            setupTask(task, INIT_MODULE, options.getInitModule());
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    private void setupTask(Task task, String moduleType, String module, String... uris) {
        setupTask(task, moduleType, module, uris, true);
    }

    private void setupTask(Task task, String moduleType, String module, String[] uris, boolean failOnError) {
        if (module != null) {
            if (isInlineOrAdhoc(module)) {
                String adhocQuery;
                if (isInlineModule(module)) {
                    adhocQuery = getInlineModuleCode(module);
                    if (isEmpty(adhocQuery)) {
                        throw new IllegalStateException(EXCEPTION_MSG_UNABLE_READ_ADHOC + module);
                    }
                } else {
                    adhocQuery = moduleToAdhocQueryMap.get(module);
                    if (adhocQuery == null) {
                        String modulePath = module.substring(0, module.indexOf('|'));
                        adhocQuery = getAdhocQuery(modulePath);
                        if (isEmpty(adhocQuery)) {
                            throw new IllegalStateException(EXCEPTION_MSG_UNABLE_READ_ADHOC + module + " from classpath or filesystem");
                        }
                        moduleToAdhocQueryMap.put(module, adhocQuery);
                    }
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
        task.setContentSourcePool(manager.getContentSourcePool());

        Properties managerProperties = manager.getProperties();
        task.setProperties(managerProperties);

        String timeZoneId = Options.findOption(managerProperties, XCC_TIME_ZONE);
        if (isNotBlank(timeZoneId)) {
            TimeZone timeZone = TimeZone.getTimeZone(timeZoneId);
            task.setTimeZone(timeZone);
        }
        task.setInputURI(uris);
        task.setFailOnError(failOnError);
        task.setExportDir(manager.getOptions().getExportFileDir());

        if (task instanceof ExportBatchToFileTask) {
            String fileName = ((ExportToFileTask) task).getFileName();
            if (isBlank(fileName)) {
                throw new IllegalArgumentException("No file name for ExportBatchToFileTask");
            }
        }
    }
}
