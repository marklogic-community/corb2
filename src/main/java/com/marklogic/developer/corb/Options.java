/*
  * * Copyright (c) 2004-2016 MarkLogic Corporation
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

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class Options {

    public static final String BATCH_SIZE = "BATCH-SIZE";
    public static final String BATCH_URI_DELIM = "BATCH-URI-DELIM";
    public static final String COLLECTION_NAME = "COLLECTION-NAME";
    public static final String COMMAND = "COMMAND";
    public static final String COMMAND_FILE = "COMMAND-FILE";
    public static final String COMMAND_FILE_POLL_INTERVAL = "COMMAND-FILE-POLL-INTERVAL";
    public static final String DECRYPTER = "DECRYPTER";
    public static final String DISK_QUEUE = "DISK-QUEUE";
    public static final String DISK_QUEUE_TEMP_DIR = "URIS-QUEUE-TEMP-DIR";
    public static final String DISK_QUEUE_MAX_IN_MEMORY_SIZE = "URIS-QUEUE-MAX-IN-MEMORY-SIZE";
    public static final String ERROR_FILE_NAME = "ERROR-FILE-NAME";
    public static final String EXIT_CODE_NO_URIS = "EXIT-CODE-NO-URIS";
    public static final String EXPORT_FILE_AS_ZIP = "EXPORT_FILE_AS_ZIP"; 
    public static final String EXPORT_FILE_BOTTOM_CONTENT = "EXPORT-FILE-BOTTOM-CONTENT";
    public static final String EXPORT_FILE_DIR = "EXPORT-FILE-DIR";
    public static final String EXPORT_FILE_HEADER_LINE_COUNT = "EXPORT-FILE-HEADER-LINE-COUNT";
    public static final String EXPORT_FILE_NAME = "EXPORT-FILE-NAME";
    public static final String EXPORT_FILE_PART_EXT = "EXPORT-FILE-PART-EXT";
    public static final String EXPORT_FILE_SORT = "EXPORT-FILE-SORT";
    public static final String EXPORT_FILE_SORT_COMPARATOR = "EXPORT-FILE-SORT-COMPARATOR";
    public static final String EXPORT_FILE_TOP_CONTENT = "EXPORT-FILE-TOP-CONTENT";
    public static final String EXPORT_FILE_URI_TO_PATH = "EXPORT-FILE-URI-TO-PATH";
    public static final String FAIL_ON_ERROR = "FAIL-ON-ERROR";
    public static final String INIT_MODULE = "INIT-MODULE";
    public static final String INIT_TASK = "INIT-TASK";
    public static final String INSTALL = "INSTALL";
    public static final String JASYPT_PROPERTIES_FILE = "JASYPT-PROPERTIES-FILE";
    public static final String MAX_OPTS_FROM_MODULE = "MAX_OPTS_FROM_MODULE"; 
    public static final String MODULES_DATABASE = "MODULES-DATABASE";
    public static final String MODULE_ROOT = "MODULE-ROOT";
    public static final String OPTIONS_FILE = "OPTIONS-FILE";
    public static final String POST_BATCH_MODULE = "POST-BATCH-MODULE";
    public static final String POST_BATCH_TASK = "POST-BATCH-TASK";
    public static final String POST_BATCH_XQUERY_MODULE = "POST-BATCH-XQUERY-MODULE"; //Deprecated in favor of POST_BATCH_MODULE
    public static final String PRE_BATCH_MODULE = "PRE-BATCH-MODULE";
    public static final String PRE_BATCH_TASK = "PRE-BATCH-TASK";
    public static final String PRE_BATCH_XQUERY_MODULE = "PRE-BATCH-XQUERY-MODULE"; //Deprecated in favor of PRE_BATCH_MODULE
    public static final String PRIVATE_KEY_ALGORITHM = "PRIVATE-KEY-ALGORITHM";
    public static final String PRIVATE_KEY_FILE = "PRIVATE-KEY-FILE";
    public static final String PROCESS_MODULE = "PROCESS-MODULE";
    public static final String PROCESS_TASK = "PROCESS-TASK";
    public static final String QUERY_RETRY_INTERVAL = "QUERY-RETRY-INTERVAL";
    public static final String QUERY_RETRY_LIMIT = "QUERY-RETRY-LIMIT";
    public static final String SSL_CIPHER_SUITES = "SSL-CIPHER-SUITES";
    public static final String SSL_CONFIG_CLASS = "SSL-CONFIG-CLASS";
    public static final String SSL_ENABLED_PROTOCOLS = "SSL-ENABLED-PROTOCOLS";
    public static final String SSL_KEYSTORE = "SSL-KEYSTORE";
    public static final String SSL_KEY_PASSWORD = "SSL-KEY-PASSWORD";
    public static final String SSL_KEYSTORE_PASSWORD = "SSL-KEYSTORE-PASSWORD";
    public static final String SSL_KEYSTORE_TYPE = "SSL-KEYSTORE-TYPE";
    public static final String SSL_PROPERTIES_FILE = "SSL-PROPERTIES-FILE";
    public static final String THREAD_COUNT = "THREAD-COUNT";
    public static final String URIS_BATCH_REF = "URIS_BATCH_REF"; 
    public static final String URIS_FILE = "URIS-FILE";
    public static final String URIS_LOADER = "URIS-LOADER";
    public static final String URIS_MODULE = "URIS-MODULE";
    public static final String URIS_REPLACE_PATTERN = "URIS-REPLACE-PATTERN";
    public static final String XCC_CONNECTION_RETRY_LIMIT = "XCC-CONNECTION-RETRY-LIMIT";
    public static final String XCC_CONNECTION_RETRY_INTERVAL = "XCC-CONNECTION-RETRY-INTERVAL";
    public static final String XCC_CONNECTION_URI = "XCC-CONNECTION-URI";
    public static final String XCC_DBNAME = "XCC-DBNAME";
    public static final String XCC_HOSTNAME = "XCC-HOSTNAME";
    public static final String XCC_PASSWORD = "XCC-PASSWORD";
    public static final String XCC_PORT = "XCC-PORT";
    public static final String XCC_USERNAME = "XCC-USERNAME";
    public static final String XML_FILE = "XML-FILE";
    public static final String XML_NODE = "XML-NODE";
    public static final String XQUERY_MODULE = "XQUERY-MODULE"; //Deprecated in favor of PROCESS_MODULE
}
