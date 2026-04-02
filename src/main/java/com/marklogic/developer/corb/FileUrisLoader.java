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

import com.marklogic.developer.corb.util.IOUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.logging.Logger;

/**
 * A URI loader implementation that reads URIs line-by-line from a text file.
 * <p>
 * This class extends {@link AbstractUrisLoader} and provides functionality to:
 * </p>
 * <ul>
 *   <li>Read URIs from a plain text file with one URI per line</li>
 *   <li>Automatically skip blank lines</li>
 *   <li>Apply URI replacement patterns via regex substitution</li>
 *   <li>Count total URIs for progress tracking</li>
 *   <li>Support batch reference tracking based on the file name</li>
 * </ul>
 * <p>
 * <b>File Format:</b><br>
 * The input file should contain one URI per line. Blank lines are automatically skipped.
 * Leading and trailing whitespace is trimmed from each line.
 * </p>
 * <p>
 * <b>URI Replacement:</b><br>
 * URIs can be transformed using regex replacement patterns configured via the
 * {@link #parseUriReplacePatterns()} method inherited from {@link AbstractUrisLoader}.
 * Replacements are applied sequentially in the order they are defined.
 * </p>
 * <p>
 * <b>Performance Characteristics:</b><br>
 * The loader performs two passes over the file:
 * </p>
 * <ol>
 *   <li>First pass: Count total lines using {@link LineNumberReader} for progress tracking</li>
 *   <li>Second pass: Read URIs sequentially using {@link BufferedReader}</li>
 * </ol>
 * <p>
 * This two-pass approach enables accurate progress reporting but requires reading the entire
 * file twice.
 * </p>
 * <p>
 * <b>Configuration:</b><br>
 * The file path is specified via:
 * </p>
 * <ul>
 *   <li>{@link Manager#getOptions()}.getUrisFile() - preferred method</li>
 *   <li>{@code getLoaderPath()} - fallback if URIS_FILE is not specified</li>
 * </ul>
 * <p>
 * <b>Resource Management:</b><br>
 * The loader maintains open file readers that must be properly closed. The {@link #close()}
 * method ensures all resources are released.
 * </p>
 *
 * @author MarkLogic Corporation
 * @since 2.0.4
 * @see AbstractUrisLoader
 * @see FileUrisXMLLoader
 * @see FileUrisDirectoryLoader
 */
public class FileUrisLoader extends AbstractUrisLoader {

    /**
     * The underlying file reader used to access the URIs file.
     * <p>
     * This reader is opened in the {@link #open()} method and closed in the {@link #close()} method.
     * It provides character-based file I/O for reading the URIs file.
     * </p>
     */
    protected FileReader fileReader;

    /**
     * Buffered reader wrapping the file reader for efficient line-by-line reading.
     * <p>
     * Buffering improves I/O performance by reading larger chunks of data from the file
     * and caching them in memory. This is particularly important when processing files
     * with many URIs, as it minimizes disk I/O operations.
     * </p>
     */
    protected BufferedReader bufferedReader;

    /**
     * Cache for the next line to be returned by {@link #next()}.
     * <p>
     * This field implements a look-ahead mechanism that allows {@link #hasNext()} to be
     * called multiple times without advancing the reader position. When {@code hasNext()}
     * reads a line, it stores it here. The {@code next()} method retrieves this cached
     * value and clears it for the next iteration.
     * </p>
     * <p>
     * A value of {@code null} indicates that no line has been read ahead, or that the
     * cached line has been consumed by {@code next()}.
     * </p>
     */
    protected String nextLine;

    /**
     * Logger instance for this class.
     */
    protected static final Logger LOG = Logger.getLogger(FileUrisLoader.class.getName());

    /**
     * Standard exception message used when an I/O error occurs while reading the URIs file.
     */
    private static final String EXCEPTION_MSG_PROBLEM_READING_URIS_FILE = "Problem while reading the uris file";

    /**
     * Opens and initializes the URI loader by reading the URIs file.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     *   <li>Parses URI replacement patterns from configuration</li>
     *   <li>Determines the file path from URIS_FILE option or loader path</li>
     *   <li>Sets the batch reference to the file name (if enabled)</li>
     *   <li>Counts the total number of lines in the file using {@link LineNumberReader}</li>
     *   <li>Opens the file for reading with {@link BufferedReader}</li>
     * </ol>
     * <p>
     * <b>Line Counting:</b><br>
     * The method uses {@link LineNumberReader#skip(long)} with {@link Long#MAX_VALUE} to quickly
     * advance to the end of the file for accurate line counting. This provides progress tracking
     * but requires reading through the entire file once before processing begins.
     * </p>
     *
     * @throws CorbException if the file cannot be opened, read, or if an I/O error occurs
     */
    @Override
    public void open() throws CorbException {

        parseUriReplacePatterns();

        String fileName = getOptions().getUrisFile();
        if (isBlank(fileName)) {
            fileName = getLoaderPath();
        }
        if (shouldSetBatchRef()) {
            batchRef = fileName;
        }
        try (LineNumberReader lnr = new LineNumberReader(new FileReader(fileName))) {
            lnr.skip(Long.MAX_VALUE);
            setTotalCount(lnr.getLineNumber() + 1L);
            //these are closed in the close() method
            fileReader = new FileReader(fileName);
            bufferedReader = new BufferedReader(fileReader);
        } catch (Exception exc) {
            throw new CorbException("Problem loading data from uris file " + getOptions().getUrisFile(), exc);
        }
    }

    /**
     * Reads the next non-blank line from the file, recursively skipping blank lines.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Reads a line from the buffered reader</li>
     *   <li>Trims leading and trailing whitespace</li>
     *   <li>If the line is blank after trimming, recursively reads the next line</li>
     *   <li>Returns the first non-blank line encountered, or null if end-of-file is reached</li>
     * </ol>
     * <p>
     * <b>Note:</b> This recursive implementation may cause stack overflow if the file contains
     * an extremely large number of consecutive blank lines, though this is unlikely in practice.
     * </p>
     *
     * @return the next non-blank line, trimmed of whitespace; or {@code null} if end-of-file
     * @throws IOException if an I/O error occurs while reading from the file
     */
    private String readNextLine() throws IOException {
        String line = trim(bufferedReader.readLine());
        if (line != null && isBlank(line)) {
            line = readNextLine();
        }
        return line;
    }

    /**
     * Checks whether more URIs are available in the file.
     * <p>
     * This method implements a look-ahead mechanism:
     * </p>
     * <ul>
     *   <li>If {@link #nextLine} is null, it reads and caches the next line</li>
     *   <li>Returns {@code true} if a line was successfully read and cached</li>
     *   <li>Returns {@code false} if end-of-file has been reached</li>
     * </ul>
     * <p>
     * The look-ahead ensures that {@code hasNext()} can be called multiple times without
     * advancing the reader position, satisfying the standard Iterator contract.
     * </p>
     *
     * @return {@code true} if more URIs are available; {@code false} if end-of-file
     * @throws CorbException if an I/O error occurs while reading from the file
     */
    @Override
    public boolean hasNext() throws CorbException {
        if (nextLine == null) {
            try {
                nextLine = readNextLine();
            } catch (Exception exc) {
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_URIS_FILE, exc);
            }
        }
        return nextLine != null;
    }

    /**
     * Returns the next URI from the file with replacement patterns applied.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Retrieves the cached {@link #nextLine} if available, otherwise reads a new line</li>
     *   <li>Clears the {@link #nextLine} cache after retrieval</li>
     *   <li>Applies all configured URI replacement patterns sequentially using regex substitution</li>
     *   <li>Returns the transformed URI</li>
     * </ol>
     * <p>
     * <b>Replacement Processing:</b><br>
     * Replacements are applied from the {@code replacements} array (inherited from
     * {@link AbstractUrisLoader}), which contains alternating pattern-replacement pairs.
     * For each pair at indices [i, i+1], the pattern at index i is replaced with the
     * value at index i+1 using {@link String#replaceAll(String, String)}.
     * </p>
     *
     * @return the next URI with replacements applied; or {@code null} if end-of-file
     * @throws CorbException if an I/O error occurs while reading from the file
     */
    @Override
    public String next() throws CorbException {
        String line;
        if (nextLine != null) {
            line = nextLine;
            nextLine = null;
        } else {
            try {
                line = readNextLine();
            } catch (Exception exc) {
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_URIS_FILE, exc);
            }
        }
        for (int i = 0; line != null && i < replacements.length - 1; i += 2) {
            line = line.replaceAll(replacements[i], replacements[i + 1]);
        }
        return line;
    }

    /**
     * Closes the loader and releases all associated resources.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Calls the parent class {@link #close()} method</li>
     *   <li>Closes the buffered reader using {@link IOUtils#closeQuietly(java.io.Closeable)}</li>
     *   <li>Closes the file reader using {@link IOUtils#closeQuietly(java.io.Closeable)}</li>
     *   <li>Calls {@link #cleanup()} to null out references</li>
     * </ol>
     * <p>
     * The {@code closeQuietly} methods ensure that exceptions during closure are logged
     * but don't prevent other cleanup operations from completing.
     * </p>
     */
    @Override
    public void close() {
        super.close();
        IOUtils.closeQuietly(fileReader);
        IOUtils.closeQuietly(bufferedReader);
        cleanup();
    }

    /**
     * Performs cleanup by nulling out references to reader objects and cached data.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Calls the parent class {@link #cleanup()} method</li>
     *   <li>Nulls out the {@link #nextLine} cache</li>
     *   <li>Nulls out the {@link #bufferedReader} reference</li>
     *   <li>Nulls out the {@link #fileReader} reference</li>
     * </ol>
     * <p>
     * Nulling out these references helps with garbage collection and prevents accidental
     * reuse of closed resources.
     * </p>
     */
    @Override
    protected void cleanup() {
        super.cleanup();
        nextLine = null;
        bufferedReader = null;
        fileReader = null;
    }
}
