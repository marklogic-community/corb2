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
package com.marklogic.developer;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogRecord;
import java.util.logging.Handler;

/**
 * Logging Handler to facilitate testing and verification that log events have occurred.
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TestHandler extends Handler {

    private final List<LogRecord> logRecords = new ArrayList<LogRecord>();
    
    @Override
    public void publish(LogRecord logRecord){
        logRecords.add(logRecord);
    }
    
    @Override
    public void flush(){
    }

    @Override
    public void close() {
    }

    public List<LogRecord> getLogRecords() {
        return new ArrayList<LogRecord>(logRecords);
    }

    public void clear() {
        logRecords.clear();
    }
}
