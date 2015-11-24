/*
 */
package com.marklogic.developer;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogRecord;
import java.util.logging.Handler;

/**
 * Logging Handler to facilitate testing and verification that log events have occurred.
 * @author mhansen
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
