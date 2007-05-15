/*
 * Copyright (c)2005-2007 Mark Logic Corporation
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
 * 
 */
public class TransformOptions {

    public static final int SLEEP_TIME_MS = 500;

    public static final long PROGRESS_INTERVAL_MS = 60 * SLEEP_TIME_MS;

    public static final String NAME = TransformOptions.class.getName();

    private static final String SLASH = "/";

    private static final char SLASHCHAR = SLASH.toCharArray()[0];

    // TODO make the XDBC library path configurable
    public static final String XDBC_ROOT = SLASH;

    public static final String COLLECTION_TYPE = "COLLECTION";
    public static final String DIRECTORY_TYPE = "DIRECTORY";
    public static final String QUERY_TYPE = "QUERY";

    static String MODULE_ROOT = SLASH
            + TransformOptions.class.getPackage().getName().replace('.',
                    SLASHCHAR) + SLASH;

    static String URIS_MODULE = "get-uris.xqy";

    private int threadCount = 1;

    /**
     * @return
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * @param count
     */
    public void setThreadCount(int count) {
        this.threadCount = count;
    }

    /**
     * @return
     */
    public String getLogLevel() {
        // TODO LogLevel make configurable
        return "INFO";
    }

    /**
     * @return
     */
    public String getLogHandler() {
        // TODO LogHandler make configurable
        return "CONSOLE";
    }

    /**
     * @return
     */
    public String getModulesDatabase() {
        // TODO make ModulesDatabase configurable
        return "Modules";
    }

}
