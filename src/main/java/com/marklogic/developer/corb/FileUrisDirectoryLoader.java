/*
  * * Copyright (c) 2004-2023 MarkLogic Corporation
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

import com.marklogic.developer.corb.util.XmlUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * A URI loader implementation that recursively walks a directory tree to discover files
 * for processing.
 * <p>
 * This class extends {@link AbstractFileUrisLoader} and provides functionality to:
 * </p>
 * <ul>
 *   <li>Recursively traverse a directory structure to discover all files</li>
 *   <li>Filter out hidden files and directories</li>
 *   <li>Generate loader document representations for each discovered file</li>
 *   <li>Support parallel processing for efficient file counting</li>
 *   <li>Automatically set batch references based on the directory path</li>
 * </ul>
 * <p>
 * <b>Directory Traversal:</b><br>
 * The loader uses {@link Files#walk(Path, FileVisitOption...)} to perform a depth-first recursive traversal
 * of the directory tree. All non-hidden files in the directory and its subdirectories
 * are included in the result set.
 * </p>
 * <p>
 * <b>File Filtering:</b><br>
 * By default, the loader filters out:
 * </p>
 * <ul>
 *   <li>Hidden files (as determined by {@link File#isHidden()})</li>
 *   <li>Directories</li>
 * </ul>
 * <p>
 * Subclasses can override {@link #accept(Path)} to provide custom filtering logic.
 * </p>
 * <p>
 * <b>Configuration:</b><br>
 * The directory path is specified via the {@value com.marklogic.developer.corb.Options#LOADER_PATH}
 * property, which must point to an existing, readable directory.
 * </p>
 * <p>
 * <b>Performance:</b><br>
 * File counting uses parallel streams for improved performance on large directory trees.
 * The file traversal itself is sequential to maintain consistent ordering.
 * </p>
 * <p>
 * <b>Resource Management:</b><br>
 * The loader manages a {@link Stream} resource that must be properly closed. The {@link #close()}
 * method ensures the stream is closed to prevent resource leaks.
 * </p>
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 * @see AbstractFileUrisLoader
 * @see FileUrisStreamingXMLLoader
 */
public class FileUrisDirectoryLoader extends AbstractFileUrisLoader {

    /**
     * Error message constant used when an exception occurs while reading files
     * from the directory tree.
     * This message is used to wrap underlying IOExceptions that occur during
     * directory traversal or file access operations.
     */
    protected static final String EXCEPTION_MSG_PROBLEM_READING_FILE = "Problem while reading the file";

    /**
     * Iterator over the filtered paths discovered during directory traversal.
     * Each path represents a file that has passed the {@link #accept(Path)} filter
     * and is ready for processing. Initialized during {@link #open()} and used to
     * sequentially retrieve files via {@link #next()}.
     */
    private Iterator<Path> fileIterator;

    /**
     * Stream of paths created by walking the directory tree.
     * This stream is filtered to include only acceptable files and must be properly
     * closed to release underlying resources such as file handles and directory streams.
     * Closed automatically during {@link #close()}.
     */
    private Stream<Path> fileStream;

    /**
     * Opens and initializes the directory loader by validating the directory and creating
     * the file iterator.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     *   <li>Retrieves the directory path from {@value com.marklogic.developer.corb.Options#LOADER_PATH}</li>
     *   <li>Validates that the path exists, is a directory, and is readable</li>
     *   <li>Sets the batch reference to the canonical directory path (if enabled)</li>
     *   <li>Creates a file stream by walking the directory tree recursively</li>
     *   <li>Filters the stream to include only acceptable files (via {@link #accept(Path)})</li>
     *   <li>Counts the total number of matching files for progress tracking</li>
     * </ol>
     *
     * @throws CorbException if the directory path is not specified, doesn't exist, is not a directory,
     *         is not readable, or if an I/O error occurs while reading the directory
     */
    @Override
    public void open() throws CorbException {

        String dirName = getLoaderPath();

        Path dir = Paths.get(dirName);
        File file = dir.toFile();
        if (!(file.exists() && file.isDirectory() && Files.isReadable(dir))) {
            throw new CorbException(MessageFormat.format("{0}: {1} must be specified and an accessible directory", Options.LOADER_PATH, dirName));
        }

        try {
            if (shouldSetBatchRef()) {
                batchRef = file.getCanonicalPath();
            }
            fileStream = Files.walk(dir);
            fileIterator = fileStream.filter(this::accept).iterator();
            setTotalCount(fileCount(dir));
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_FILE, ex);
        }
    }

    /**
     * Counts the total number of acceptable files in the directory tree using parallel processing.
     * <p>
     * This method creates a separate stream to walk the directory tree and counts files that
     * pass the {@link #accept(Path)} filter. The counting is performed in parallel for improved
     * performance on large directory trees.
     * </p>
     * <p>
     * <b>Note:</b> The stream is automatically closed using try-with-resources to ensure
     * proper resource cleanup.
     * </p>
     *
     * @param dir the root directory path to count files from
     * @return the number of files that pass the acceptance filter
     * @throws IOException if an I/O error occurs while walking the directory
     * @throws ArithmeticException if the file count exceeds {@link Integer#MAX_VALUE}
     */
    protected int fileCount(Path dir) throws IOException {
        try (Stream<Path> stream = Files.walk(dir)) {
            return Math.toIntExact(stream.parallel()
                    .filter(this::accept)
                    .count());
        }
    }

    /**
     * Determines whether a path should be included in the loader's result set.
     * <p>
     * This method applies filtering criteria to exclude:
     * </p>
     * <ul>
     *   <li>Hidden files (as determined by {@link File#isHidden()})</li>
     *   <li>Directories</li>
     * </ul>
     * <p>
     * <b>Extensibility:</b><br>
     * Subclasses can override this method to implement custom filtering logic, such as:
     * </p>
     * <ul>
     *   <li>File extension patterns (e.g., only .xml files)</li>
     *   <li>File size constraints</li>
     *   <li>Name pattern matching</li>
     *   <li>Date/time filters</li>
     * </ul>
     *
     * @param path the {@link Path} to evaluate
     * @return {@code true} if the path represents a non-hidden file; {@code false} if it
     *         is a hidden file or a directory
     */
    protected boolean accept(Path path) {
        File file = path.toFile();
        //TODO custom property with pattern to filter out unwanted files, or just let the process module do the filtering?
        return !(file.isHidden() || file.isDirectory());
    }

    /**
     * Checks whether more files are available in the directory tree.
     * <p>
     * This method returns {@code true} if the file iterator has been initialized and
     * has more elements to process.
     * </p>
     *
     * @return {@code true} if more files are available; {@code false} otherwise
     * @throws CorbException if an error occurs checking iterator status (though unlikely in this implementation)
     */
    @Override
    public boolean hasNext() throws CorbException {
        return fileIterator != null && fileIterator.hasNext();
    }

    /**
     * Returns the next file in the directory tree as an XML loader document string.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Retrieves the next {@link Path} from the file iterator</li>
     *   <li>Converts it to a {@link File} object</li>
     *   <li>Generates a loader document using {@link #toLoaderDoc(File)}</li>
     *   <li>Converts the document to an XML string representation</li>
     * </ol>
     * <p>
     * The loader document contains metadata about the file, such as its path and other
     * attributes needed for processing.
     * </p>
     *
     * @return the next file represented as an XML loader document string
     * @throws CorbException if an error occurs retrieving or converting the file
     */
    @Override
    public String next() throws CorbException {
        Path path = fileIterator.next();
        File file = path.toFile();
        return XmlUtils.documentToString(toLoaderDoc(file));
    }

    /**
     * Closes the loader and releases all associated resources.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Calls the parent class {@link #close()} method</li>
     *   <li>Closes the file stream (if not null) to release underlying resources</li>
     * </ol>
     * <p>
     * <b>Important:</b> Failing to call this method may result in resource leaks, particularly
     * file handles and directory streams that are not automatically released until garbage collection.
     * </p>
     */
    @Override
    public void close() {
        super.close();
        if (fileStream != null) {
            fileStream.close();
        }
    }
}
