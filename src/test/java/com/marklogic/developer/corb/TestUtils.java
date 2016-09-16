/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.List;
import java.util.Scanner;
import java.util.logging.LogRecord;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class TestUtils {

    public static void clearSystemProperties() {
        System.clearProperty("DECRYPTER");
        System.clearProperty("URIS-MODULE");
        System.clearProperty("OPTIONS-FILE");
        System.clearProperty("XCC-CONNECTION-URI");
        System.clearProperty("COLLECTION-NAME");
        System.clearProperty("XQUERY-MODULE");
        System.clearProperty("PROCESS-MODULE");
        System.clearProperty("THREAD-COUNT");
        System.clearProperty("MODULE-ROOT");
        System.clearProperty("MODULES-DATABASE");
        System.clearProperty("INSTALL");
        System.clearProperty("INIT-MODULE");
        System.clearProperty("INIT-TASK");
        System.clearProperty("BATCH-SIZE");
        System.clearProperty("PRIVATE-KEY-FILE");
        System.clearProperty("PROCESS-TASK");
        System.clearProperty("PRE-BATCH-MODULE");
        System.clearProperty("PRE-BATCH-XQUERY-MODULE");
        System.clearProperty("PRE-BATCH-TASK");
        System.clearProperty("POST-BATCH-MODULE");
        System.clearProperty("POST-BATCH-XQUERY-MODULE");
        System.clearProperty("POST-BATCH-TASK");
        System.clearProperty("EXPORT-FILE-DIR");
        System.clearProperty("EXPORT-FILE-NAME");
        System.clearProperty("URIS-FILE");
        // TODO consider looking for any properties starting with, to ensure they all get cleared
        System.clearProperty("XQUERY-MODULE.foo");
        System.clearProperty("PROCESS-MODULE.foo");
        System.clearProperty("PRE-BATCH-MODULE.foo");
        System.clearProperty("PRE-BATCH-XQUERY-MODULE.foo");
        System.clearProperty("POST-BATCH-MODULE.foo");
        System.clearProperty("POST-BATCH-XQUERY-MODULE.foo");
        System.clearProperty("EXPORT_FILE_AS_ZIP");
        System.clearProperty("EXPORT-FILE-BOTTOM-CONTENT)");
        System.clearProperty("EXPORT-FILE-PART-EXT");
        System.clearProperty("ERROR-FILE-NAME");
        System.clearProperty("FAIL-ON-ERROR");
        System.clearProperty("URIS-QUEUE-MAX-IN-MEMORY-SIZE");
        System.clearProperty("URIS-QUEUE-TEMP-DIR");
        System.clearProperty("URIS-MODULE.count");
    }

    public static String readFile(String filePath) throws FileNotFoundException {
        return readFile(new File(filePath));
    }

    public static String readFile(File file) throws FileNotFoundException {
        String result;
        try ( // \A == The beginning of the input
                Scanner scanner = new Scanner(file, "UTF-8")) {
            result = scanner.useDelimiter("\\A").next();
        }
        return result;
    }

    public static void clearFile(File file) {
        try (PrintWriter pw  = new PrintWriter(file)){          
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static File createTempDirectory() throws IOException {
        return Files.createTempDirectory("temp", new FileAttribute<?>[0]).toFile();  
    }

    public static boolean containsLogRecord(List<LogRecord> logRecords, LogRecord logRecord) {
        return logRecords.stream()
                .anyMatch((record) -> (record.getLevel().equals(logRecord.getLevel()) && 
                        record.getMessage().equals(logRecord.getMessage())));
    }
}
