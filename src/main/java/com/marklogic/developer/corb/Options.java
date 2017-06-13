/*
  * * Copyright (c) 2004-2017 MarkLogic Corporation
  * *
  * * Licensed under the Apache License, Version 2.0 (the "License");
  * * you may not use this file except in compliance with the License.
  * * You may obtain a copy of the License at
  * *
  * * http://www.apache.org/licenses/LICENSE-2.0
  * *
  * * Unless required by applicable law or agreed to in writing, software
  * * distributed under the License is distributed on an "AS IS" BASIS,
  * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * * See the License for the specific language governing permissions and
  * * limitations under the License.
  * *
  * * The use of the Apache License does not indicate that this project is
  * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.util.CollectionUtils.removeBlanksAndTrim;
import static com.marklogic.developer.corb.util.CollectionUtils.removeNullsAndTrim;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.stringToBoolean;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import static java.util.logging.Level.WARNING;

import java.io.File;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Options that allow users to configure CoRB and control various aspects of
 * execution.
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @author Bhagat Bandlamudi
 * @since 2.3.0
 */
public final class Options extends Constants{
    //internal constants
    public static final int SLEEP_TIME_MS = 500;
    public static final long PROGRESS_INTERVAL_MS = 60L * SLEEP_TIME_MS;
    
    public static final int DEFAULT_EXIT_CODE_SUCCESS = 0;
    public static final int DEFAULT_EXIT_CODE_INIT_ERROR = 1;
    public static final int DEFAULT_EXIT_CODE_PROCESSING_ERROR = 2;
    public static final int DEFAULT_EXIT_CODE_STOP_COMMAND = 3;
    public static final int DEFAULT_EXIT_CODE_NO_URIS = 0;
    
    public static final String SLASH = "/";
    public static final String COLLECTION_TYPE = "COLLECTION";
    public static final String DIRECTORY_TYPE = "DIRECTORY";
    public static final String QUERY_TYPE = "QUERY";
    
    public static final String XDBC_ROOT = "XDBC-ROOT";
    public static final String INIT_TASK_CLASS = "INIT-TASK-CLASS";
    public static final String PROCESS_TASK_CLASS = "PROCESS-TASK-CLASS";
    public static final String PRE_BATCH_TASK_CLASS = "PRE-BATCH-TASK-CLASS";
    public static final String POST_BATCH_TASK_CLASS = "POST-BATCH-TASK-CLASS";
    
    public static final String DEFAULT_MODULE_ROOT = SLASH;
    public static final int DEFAULT_THREAD_COUNT = 1;
    public static final int DEFAULT_BATCH_SIZE = 1;
    public static final boolean DEFAULT_FAIL_ON_ERROR = true;
    public static final boolean DEFAULT_DISK_QUEUE = false;
    public static final int DEFAULT_DISK_QUEUE_MAX_IN_MEMORY_SIZE = 1000;
    public static final int DEFAULT_NUM_TPS_FOR_ETC = 10;
    public static final int DEFAULT_COMMAND_FILE_POLL_INTERVAL = 1;
    
    @Deprecated
    public static final String DEFAULT_MODULES_DATABASE = "Modules";
    
    private static final int QUEUE_SIZE = 100 * 1000;
    private static final int DISK_QUEUE_MIN_IN_MEMORY_SIZE = 100;
        
    private Map<String,String> defaults = new HashMap<String,String>();
    private Map<String,String> arguments = new HashMap<String,String>();
    private Map<String,String> properties = new HashMap<String,String>();
    
    private Set<String> propertyNames = new HashSet<String>();
    
    private Class<? extends UrisLoader> loaderCls;
    private Map<String,Class<? extends Task>> taskClsMap = new HashMap<String,Class<? extends Task>>();
    
    private static final Logger LOG = Logger.getLogger(Options.class.getName());
    
    private static Class<? extends Task> getTaskCls(String type, String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> cls = Class.forName(className);
        if (Task.class.isAssignableFrom(cls)) {
            cls.newInstance(); // sanity check
            return cls.asSubclass(Task.class);
        } else {
            throw new IllegalArgumentException(type + " must be of type "+Task.class.getName());
        }
    }

    private static Class<? extends UrisLoader> getUrisLoaderCls(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> cls = Class.forName(className);
        if (UrisLoader.class.isAssignableFrom(cls)) {
            cls.newInstance(); // sanity check
            return cls.asSubclass(UrisLoader.class);
        } else {
            throw new IllegalArgumentException("Uris Loader must be of type "+UrisLoader.class.getName());
        }
    }
    
    private static void validateDirectoryPath(String propertyName, String path){
        if(isNotBlank(path)){
            File dirFile = new File(path);
            if (!dirFile.exists() || !dirFile.canWrite()) {
                throw new IllegalArgumentException("Cannot write to directory "+path+" under property " + propertyName);
            }
        }
    }
    
    public Options() throws CorbException{
        this.init();
    }
      
    public Options(Map<String,String> props) throws CorbException{
        this(null,props);
    }
    
    public Options(Map<String,String> arguments, Map<String,String> properties) throws CorbException{
        this.arguments.putAll(removeBlanksAndTrim(arguments)); //blanks in arguments are not considered explicit
        this.properties.putAll(removeNullsAndTrim(properties)); //nulls in properties are considered explicit. 
        this.init();
    }
    
    private void init() throws CorbException{
        initDefaults();
        validateDirectoryPaths();
        
        initLoaderCls();
        initTaskCls(INIT_TASK_CLASS,INIT_TASK);
        initTaskCls(PROCESS_TASK_CLASS,PROCESS_TASK);
        initTaskCls(PRE_BATCH_TASK_CLASS,PRE_BATCH_TASK);
        initTaskCls(POST_BATCH_TASK_CLASS,POST_BATCH_TASK);
        
        initPropertyNames();
    }
    
    private void initDefaults(){
        this.defaults.put(MODULE_ROOT, DEFAULT_MODULE_ROOT);
        this.defaults.put(THREAD_COUNT, String.valueOf(DEFAULT_THREAD_COUNT));
        this.defaults.put(BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE));
        this.defaults.put(FAIL_ON_ERROR, String.valueOf(DEFAULT_FAIL_ON_ERROR));
        this.defaults.put(DISK_QUEUE, String.valueOf(DEFAULT_DISK_QUEUE));
        this.defaults.put(DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(DEFAULT_DISK_QUEUE_MAX_IN_MEMORY_SIZE));
        this.defaults.put(NUM_TPS_FOR_ETC, String.valueOf(DEFAULT_NUM_TPS_FOR_ETC));
    }
    
    private void initPropertyNames(){
        propertyNames.addAll(defaults.keySet());
        propertyNames.addAll(arguments.keySet());
        propertyNames.addAll(properties.keySet());
        propertyNames.addAll(System.getProperties().stringPropertyNames());
    }
    
    private void validateDirectoryPaths(){
        validateDirectoryPath(DISK_QUEUE_TEMP_DIR,getProperty(DISK_QUEUE_TEMP_DIR));
        
        String exportFileDir = getProperty(EXPORT_FILE_DIR);
        validateDirectoryPath(EXPORT_FILE_DIR,exportFileDir);
        validateFilePath(EXPORT_FILE_NAME, getProperty(EXPORT_FILE_NAME), exportFileDir);
        validateFilePath(ERROR_FILE_NAME, getProperty(ERROR_FILE_NAME), exportFileDir);
    }
    
    private void validateFilePath(String propertyName, String filePath, String defaultDirectory){
        if(filePath != null){
            String parentDir = new File(filePath).getParent();
            if(parentDir != null ){
                validateDirectoryPath(propertyName,parentDir);
            }else if(defaultDirectory == null){
                validateDirectoryPath(propertyName,System.getProperty("user.dir"));
            }
        }
    }
    
    private void initLoaderCls() throws CorbException{
        String urisLoader = getProperty(URIS_LOADER);
        if (urisLoader != null) {
            try {
                loaderCls = getUrisLoaderCls(urisLoader);
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
                throw new CorbException("Unable to instantiate UrisLoader class: " + urisLoader, ex);
            }
        }
    }
    
    private void initTaskCls(String taskClassType, String propertyName) throws CorbException{
        String value = getProperty(propertyName);
        if(value != null){
            try{
                Class<? extends Task> taskCls = getTaskCls(propertyName, value);
                taskClsMap.put(taskClassType, taskCls);
            }catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
                throw new CorbException("Unable to instantiate Task class " +value+" for "+ taskClassType, ex);
            }
        }
    }
        
    public Map<String,String> getArguments(){
        return new HashMap<String,String>(this.arguments);
    }
    
    public Map<String,String> getProperties(){
        return new HashMap<String,String>(this.properties);
    }
    
    public Map<String,String> getDefaults(){
        return new HashMap<String,String>(this.defaults);
    }
    
    public String getProperty(String propertyName) {
        return getProperty(propertyName,true);
    }
           
    public String getProperty(String propertyName, boolean emptyAsNull) {       
        String retVal = arguments.get(propertyName);
        if(retVal == null){ //Note: Don't check blanks in arguments. 
            retVal = System.getProperty(propertyName);
            if(retVal == null || (isBlank(retVal) && emptyAsNull)){
                retVal = this.properties.get(propertyName);
                if(retVal == null || (isBlank(retVal) && emptyAsNull)){
                    retVal = this.defaults.get(propertyName);
                }
            }
        }       
        return trim(retVal);
    }
    
    public String getProperty(String propertyName, String defaultValue){
        String value = getProperty(propertyName);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Retrieves an int value.
     *
     * @param key The key name.
     * @return The requested value ({@code -1} if not found or could not parse
     * value as int).
     */
    public int getIntProperty(String key){
        return getIntProperty(key, -1);
    }
    
    public int getIntProperty(String key, int defaultValue) {
        int intVal = defaultValue;
        String value = getProperty(key);
        if (isNotEmpty(value)) {
            try {
                intVal = Integer.parseInt(value);
            } catch (Exception exc) {
                LOG.log(WARNING, MessageFormat.format("Unable to parse `{0}` value `{1}` as an int", key, value), exc);
            }
        }
        return intVal;
    }
    
    public boolean getBooleanProperty(String key){
        return stringToBoolean(getProperty(key));
    }
    
    public boolean getBooleanProperty(String key, boolean defaultValue){
        return stringToBoolean(getProperty(key), defaultValue);
    }
    
    public Set<String> getPropertyNames(){
        return this.propertyNames;
    }
        
    public void setProperty(String propertyName, String propertyValue){
        removeProperty(propertyName);
        if(propertyValue != null){
            this.properties.put(propertyName, propertyValue);
            propertyNames.add(propertyName);
        }
    }
    
    public void removeProperty(String propertyName){
        this.properties.remove(propertyName);
        this.arguments.remove(propertyName);
        if(System.getProperty(propertyName) != null){
            System.clearProperty(propertyName);
        }
        propertyNames.remove(propertyName);
    }
    
    /**
     * This queue size is not configurable. 
     * @return
     */
    public int getQueueSize() {
        return QUEUE_SIZE;
    }
    
    //accessor methods provided for easier access of properties. 
    
    //uris loader and task class accessors
    
    public Class<? extends UrisLoader> getUrisLoaderClass() {
        return this.loaderCls;
    }

    public void setUrisLoaderClass(Class<? extends UrisLoader> loaderCls) {
        this.loaderCls = loaderCls;
    }
    
    public Class<? extends Task> getTaskClass(String taskClassType){
        return taskClsMap.get(taskClassType);
    }
    
    public void setTaskClass(String taskClassType, Class<? extends Task> taskCls){
        taskClsMap.put(taskClassType, taskCls);
    }
    
    public Class<? extends Task> getInitTaskClass() {
        return getTaskClass(INIT_TASK_CLASS);
    }
    
    public void setInitTaskClass(Class<? extends Task> taskCls) {
        setTaskClass(INIT_TASK_CLASS,taskCls);
    }
    
    public Class<? extends Task> getProcessTaskClass() {
        return getTaskClass(PROCESS_TASK_CLASS);
    }
      
    public void setProcessTaskClass(Class<? extends Task> taskCls) {
        setTaskClass(PROCESS_TASK_CLASS,taskCls);
    }
    
    public void setPreBatchTaskClass(Class<? extends Task> taskCls) {
        setTaskClass(PRE_BATCH_TASK_CLASS,taskCls);
    }
    
    public Class<? extends Task> getPreBatchTaskClass() {
        return getTaskClass(PRE_BATCH_TASK_CLASS);
    }
    
    public void setPostBatchTaskClass(Class<? extends Task> taskCls) {
        setTaskClass(POST_BATCH_TASK_CLASS,taskCls);
    }
    
    public Class<? extends Task> getPostBatchTaskClass() {
        return getTaskClass(POST_BATCH_TASK_CLASS);
    }
    
    //module accessors
    
    public String getInitModule() {
        return getProperty(INIT_MODULE);
    }
    
    public void setInitModule(String module) {
        setProperty(INIT_MODULE, module);
    }
    
    public String getUrisModule() {
        return getProperty(URIS_MODULE);
    }
    
    public void setUrisModule(String module) {
        setProperty(URIS_MODULE, module);
    }
    
    public String getUrisFile() {
        return getProperty(URIS_FILE);
    }

    public void setUrisFile(String urisFile) {
        setProperty(URIS_FILE,urisFile);
    }
    
    public String getProcessModule() {
        String module = getProperty(PROCESS_MODULE);
        return module != null ? module : getProperty(XQUERY_MODULE);
    }
    
    public void setProcessModule(String module) {
        setProperty(PROCESS_MODULE,module);
    }
    
    public String getPreBatchModule() {
        String module = getProperty(PRE_BATCH_MODULE);
        return module != null ? module : getProperty(PRE_BATCH_XQUERY_MODULE);
    }
    
    public void setPreBatchModule(String module) {
        setProperty(PRE_BATCH_MODULE,module);
    }
    
    public String getPostBatchModule() {
        String module = getProperty(POST_BATCH_MODULE);
        return module != null ? module : getProperty(POST_BATCH_XQUERY_MODULE);
    }
    
    public void setPostBatchModule(String module) {
        setProperty(POST_BATCH_MODULE,module);
    }
      
    // variable accessors
    
    public String getModuleRoot() {
        String moduleRoot = getProperty(MODULE_ROOT);
        return moduleRoot != null ? moduleRoot : DEFAULT_MODULE_ROOT;
    }
    
    public void setModuleRoot(String moduleRoot) {
        if(isNotBlank(moduleRoot)) setProperty(MODULE_ROOT, moduleRoot);
    }
    
    public int getThreadCount() {
        return getIntProperty(THREAD_COUNT,DEFAULT_THREAD_COUNT);
    }
    
    public void setThreadCount(int count) {
        if(count > 0) setProperty(THREAD_COUNT, String.valueOf(count));
    }

    public int getBatchSize() {
        return getIntProperty(BATCH_SIZE,DEFAULT_BATCH_SIZE);
    }

    public void setBatchSize(int batchSize) {
        if(batchSize > 0) setProperty(BATCH_SIZE, String.valueOf(batchSize));
    }
    
    public boolean isFailOnError() {
        return getBooleanProperty(FAIL_ON_ERROR,DEFAULT_FAIL_ON_ERROR);
    }
    
    public void setFailOnError(boolean failOnError) {
        setProperty(FAIL_ON_ERROR, String.valueOf(failOnError));
    }
    
    public String getExportFileDir() {
        return getProperty(EXPORT_FILE_DIR);
    }

    public void setExportFileDir(String exportFileDir) {
        validateDirectoryPath(EXPORT_FILE_DIR, exportFileDir);
        setProperty(EXPORT_FILE_DIR,exportFileDir);
    }
    
    public String getExportFileName(){
        return getProperty(EXPORT_FILE_NAME);
    }
    
    public void setExportFileName(String exportFileName){
        validateFilePath(EXPORT_FILE_NAME, exportFileName, getProperty(EXPORT_FILE_DIR));
        setProperty(EXPORT_FILE_NAME,exportFileName);
    }
    
    public String getErrorFileName(){
        return getProperty(ERROR_FILE_NAME);
    }
    
    public void setErrorFileName(String errorFileName){
        validateFilePath(ERROR_FILE_NAME, errorFileName, getProperty(EXPORT_FILE_DIR));
        setProperty(ERROR_FILE_NAME,errorFileName);
    }

    public boolean shouldUseDiskQueue() {
        return getBooleanProperty(DISK_QUEUE,DEFAULT_DISK_QUEUE);
    }
    
    public void setUseDiskQueue(boolean useDiskQueue) {
        setProperty(DISK_QUEUE, String.valueOf(useDiskQueue));
    }
    
    public int getDiskQueueMaxInMemorySize() {
        return getIntProperty(DISK_QUEUE_MAX_IN_MEMORY_SIZE,DEFAULT_DISK_QUEUE_MAX_IN_MEMORY_SIZE);
    }

    public void setDiskQueueMaxInMemorySize(int size) {
        if(size >= DISK_QUEUE_MIN_IN_MEMORY_SIZE) setProperty(DISK_QUEUE_MAX_IN_MEMORY_SIZE, String.valueOf(size));
    }
    
    public String getDiskQueueTempDir() {
        return getProperty(DISK_QUEUE_TEMP_DIR);
    }
    
    public void setDiskQueueTempDir(String directory) {
        validateDirectoryPath(DISK_QUEUE_TEMP_DIR,directory);
        setProperty(DISK_QUEUE_TEMP_DIR, directory);
    }
    
    public int getNumTpsForETC() {
        return getIntProperty(NUM_TPS_FOR_ETC,DEFAULT_NUM_TPS_FOR_ETC);
    }
    
    public void setNumTpsForETC(int numTpsForETC) {
        if (numTpsForETC > 0) setProperty(NUM_TPS_FOR_ETC,String.valueOf(numTpsForETC));
    }

    public String getLogLevel() {
        // TODO LogLevel make configurable
        return "INFO";
    }
    
    public String getLogHandler() {
        // TODO LogHandler make configurable
        return "CONSOLE";
    }
    
    @Deprecated
    public String getCollection() {
        return getProperty(COLLECTION_NAME,"");
    }
    
    @Deprecated
    public void setCollection(String collection) {
         setProperty(COLLECTION_NAME,collection);
    }
        
    @Deprecated
    public boolean isDoInstall() {
        return getBooleanProperty(INSTALL,false);
    }
    
    @Deprecated
    public void setInstall(boolean install){
        setProperty(INSTALL,String.valueOf(install));
    }
    
    @Deprecated
    public String getXDBC_ROOT() {
        return getProperty(XDBC_ROOT, SLASH);
    }

    @Deprecated
    public void setXDBC_ROOT(String xdbc_root) {
        setProperty(XDBC_ROOT, xdbc_root);
    }
    
    @Deprecated
    public String getModulesDatabase() {
        String modulesDatabase = getProperty(MODULES_DATABASE);
        return modulesDatabase != null? modulesDatabase : DEFAULT_MODULES_DATABASE;
    }
    
    @Deprecated
    public void setModulesDatabase(String modulesDatabase) {
        setProperty(MODULES_DATABASE, modulesDatabase);
    }
    
    @Deprecated
    protected void normalizeLegacyProperties() {
        Map<String, String> legacyProperties = new HashMap<>(3);
        legacyProperties.put(XQUERY_MODULE,PROCESS_MODULE);
        legacyProperties.put(PRE_BATCH_XQUERY_MODULE,PRE_BATCH_MODULE);
        legacyProperties.put(POST_BATCH_XQUERY_MODULE,POST_BATCH_MODULE);
        
        HashMap<String,String> normalized = getNormalizedProperties(properties);
        normalized.putAll(getNormalizedProperties(System.getProperties()));
        for(Map.Entry<String, String> entry: normalized.entrySet()){
            setProperty(entry.getKey(),entry.getValue());
        }
    }
    
    @Deprecated
    private HashMap<String,String> getNormalizedProperties(Map props) {
        HashMap<String,String> normalizedProperties = new HashMap<String,String>();

        Map<String, String> legacyProperties = new HashMap<String,String>(3);
        legacyProperties.put(XQUERY_MODULE,PROCESS_MODULE);
        legacyProperties.put(PRE_BATCH_XQUERY_MODULE,PRE_BATCH_MODULE);
        legacyProperties.put(POST_BATCH_XQUERY_MODULE,POST_BATCH_MODULE);
        
        for (Map.Entry<String, String> entry : legacyProperties.entrySet()) {
            String legacyKey = entry.getKey();
            String legacyKeyPrefix = legacyKey + '.';
            for(Object key: props.keySet()){
                if(key != null && key.toString().startsWith(legacyKeyPrefix) && props.get(key) != null){
                    String normalizedKeyPrefix = entry.getValue() + '.';
                    String newKey = key.toString().replace(legacyKeyPrefix, normalizedKeyPrefix);
                    normalizedProperties.put(newKey.trim(), props.get(key).toString().trim());
                }
            }
        }
        return normalizedProperties;
    }
}
