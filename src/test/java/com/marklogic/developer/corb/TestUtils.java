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
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class TestUtils {
    
    private static final Logger LOG = Logger.getLogger(TestUtils.class.getName());
    private TestUtils() {
        //No need for an instance
    }

    /**
     * clear System properties that are CoRB options
     */
    public static void clearSystemProperties() {
        Set<String> systemProperties = System.getProperties().stringPropertyNames();
        Class<Options> optionsClass = Options.class;
        Arrays.stream(optionsClass.getFields())
                .map(field -> {
                    try {
                        field.setAccessible(true);
                        //obtain the CoRB option name
                        return (String) field.get(optionsClass);
                    } catch (IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .forEach(option -> {
                    //remove the standard CoRB option
                    System.clearProperty(option);
                    //remove any custom input properties (i.e. URIS-MODULE.foo)
                    systemProperties.stream()
                            .filter(property -> property.startsWith(option + "."))
                            .forEach(System::clearProperty);
                });
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
        try (PrintWriter pw = new PrintWriter(file)) {
            //Instantiating new PRintWriter wipes the file
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    public static File createTempDirectory() throws IOException {
        return Files.createTempDirectory("temp", new FileAttribute<?>[0]).toFile();
    }

    public static boolean containsLogRecord(List<LogRecord> logRecords, LogRecord logRecord) {
        return logRecords.stream()
                .anyMatch(record -> record.getLevel().equals(logRecord.getLevel())
                        && record.getMessage().equals(logRecord.getMessage()));
    }
}
