/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import com.marklogic.developer.TestHandler;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Session;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
                             .filter(property -> property.startsWith(option + '.'))
                             .forEach(System::clearProperty);
                 });
        System.clearProperty(Options.EXPORT_FILE_AS_ZIP_LEGACY);
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
            //Instantiating new PrintWriter wipes the file
        } catch (FileNotFoundException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    public static File createTempDirectory() throws IOException {
        return Files.createTempDirectory("temp").toFile();
    }

    public static boolean containsLogRecord(List<LogRecord> logRecords, LogRecord logRecord) {
        return logRecords.stream()
                .anyMatch(record -> record.getLevel().equals(logRecord.getLevel())
                        && record.getMessage().equals(logRecord.getMessage()));
    }

    public static void assertEqualsNormalizeNewline(String expected, String actual) {
        assertEquals(normalizeNewline(expected), normalizeNewline(actual));
    }

    public static String normalizeNewline(String input) {
        return input.replaceAll("\\R", "\n");
    }

    // -------------------------------------------------------------------------
    // Log assertion helpers
    // -------------------------------------------------------------------------

    /**
     * Assert that the handler contains a log record with the given level and message.
     * Replaces the common two-line pattern:
     * <pre>
     *   List&lt;LogRecord&gt; records = handler.getLogRecords();
     *   assertTrue(TestUtils.containsLogRecord(records, new LogRecord(level, message)));
     * </pre>
     */
    public static void assertContainsLogRecord(TestHandler handler, Level level, String message) {
        assertTrue(containsLogRecord(handler.getLogRecords(), new LogRecord(level, message)),
                "Expected log record [" + level + "] \"" + message + "\" not found");
    }

    /**
     * Assert that the handler does NOT contain a log record with the given level and message.
     */
    public static void assertNotContainsLogRecord(TestHandler handler, Level level, String message) {
        assertFalse(containsLogRecord(handler.getLogRecords(), new LogRecord(level, message)),
                "Unexpected log record [" + level + "] \"" + message + "\" was found");
    }

    /**
     * Assert the level and message of the first log record in the handler.
     * Replaces the common two-line pattern:
     * <pre>
     *   List&lt;LogRecord&gt; records = handler.getLogRecords();
     *   assertEquals(level, records.get(0).getLevel());
     *   assertEquals(message, records.get(0).getMessage());
     * </pre>
     */
    public static void assertFirstLogRecord(TestHandler handler, Level level, String message) {
        List<LogRecord> records = handler.getLogRecords();
        assertFalse(records.isEmpty(), "Expected at least one log record but handler is empty");
        assertEquals(level, records.get(0).getLevel());
        assertEquals(message, records.get(0).getMessage());
    }

    // -------------------------------------------------------------------------
    // ContentSourcePool mock factory
    // -------------------------------------------------------------------------

    /**
     * Creates a {@link ContentSourcePool} mock pre-wired with a {@link ContentSource} mock
     * and a {@link Session} mock, using:
     * <pre>
     *   when(csp.get()).thenReturn(contentSource);
     *   when(contentSource.newSession()).thenReturn(session);
     * </pre>
     * Replaces the common 5-line boilerplate repeated across many test classes.
     * Use {@link #getSessionFromPool(ContentSourcePool)} to retrieve the session for further stubbing.
     */
    public static ContentSourcePool mockContentSourcePool() {
        ContentSourcePool csp = mock(ContentSourcePool.class);
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        try {
            when(csp.get()).thenReturn(cs);
        } catch (CorbException e) {
            throw new RuntimeException(e);
        }
        when(cs.newSession()).thenReturn(session);
        return csp;
    }

    /**
     * Returns the {@link Session} mock wired to the given {@link ContentSourcePool} mock
     * (created via {@link #mockContentSourcePool()}), allowing further stubbing of session calls.
     */
    public static Session getSessionFromPool(ContentSourcePool csp) {
        try {
            ContentSource cs = csp.get();
            if (cs == null) {
                throw new RuntimeException("ContentSourcePool mock returned null ContentSource");
            }
            return cs.newSession();
        } catch (Exception e) {
            throw new RuntimeException("Could not retrieve session from pool mock", e);
        }
    }

    // -------------------------------------------------------------------------
    // stdout / stderr capture helpers
    // -------------------------------------------------------------------------

    private static ByteArrayOutputStream capturedOut;
    private static ByteArrayOutputStream capturedErr;
    private static PrintStream originalOut;
    private static PrintStream originalErr;

    /**
     * Redirects {@code System.out} to an in-memory buffer. Call {@link #getStdout()} to read
     * the captured output and {@link #restoreStdStreams()} in {@code @AfterEach} to restore.
     */
    public static void captureStdout() {
        capturedOut = new ByteArrayOutputStream();
        originalOut = System.out;
        try {
            System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Redirects {@code System.err} to an in-memory buffer. Call {@link #getStderr()} to read
     * the captured output and {@link #restoreStdStreams()} in {@code @AfterEach} to restore.
     */
    public static void captureStderr() {
        capturedErr = new ByteArrayOutputStream();
        originalErr = System.err;
        try {
            System.setErr(new PrintStream(capturedErr, true, StandardCharsets.UTF_8.name()));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns the content written to the captured stdout buffer as a UTF-8 string. */
    public static String getStdout() {
        try {
            return capturedOut == null ? "" : capturedOut.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns the content written to the captured stderr buffer as a UTF-8 string. */
    public static String getStderr() {
        try {
            return capturedErr == null ? "" : capturedErr.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Restores {@code System.out} and {@code System.err} to their original streams.
     * Call this in {@code @AfterEach} after {@link #captureStdout()} / {@link #captureStderr()}.
     */
    public static void restoreStdStreams() {
        if (originalOut != null) {
            System.setOut(originalOut);
            originalOut = null;
            capturedOut = null;
        }
        if (originalErr != null) {
            System.setErr(originalErr);
            originalErr = null;
            capturedErr = null;
        }
    }
}
