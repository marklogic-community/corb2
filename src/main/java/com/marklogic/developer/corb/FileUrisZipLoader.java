/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.developer.corb.util.XmlUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A URI loader implementation that extracts and processes entries from ZIP archive files.
 * <p>
 * This class extends {@link AbstractFileUrisLoader} and provides functionality to:
 * </p>
 * <ul>
 *   <li>Read and iterate through entries in a ZIP archive</li>
 *   <li>Filter out directory entries, processing only files</li>
 *   <li>Extract file content as base64-encoded data</li>
 *   <li>Generate loader document envelopes with metadata for each entry</li>
 *   <li>Capture ZIP-specific metadata (filename, last modified time, comments)</li>
 *   <li>Support parallel counting of entries for progress tracking</li>
 * </ul>
 * <p>
 * <b>Processing Model:</b><br>
 * The loader processes ZIP archives using the following workflow:
 * </p>
 * <ol>
 *   <li>Opens the ZIP file and creates a {@link ZipFile} reader</li>
 *   <li>Filters the entries to include only files (excludes directories)</li>
 *   <li>Counts total file entries using parallel stream processing</li>
 *   <li>Iterates through file entries, extracting content and metadata</li>
 *   <li>Returns each entry as a loader document XML string</li>
 * </ol>
 * <p>
 * <b>Content Handling:</b><br>
 * Each ZIP entry's content is:
 * </p>
 * <ul>
 *   <li>Read as a binary stream</li>
 *   <li>Base64 encoded automatically</li>
 *   <li>Wrapped in a loader document envelope with metadata</li>
 *   <li>Serialized to an XML string for processing</li>
 * </ul>
 * This encoding ensures binary content can be safely transmitted as XML.

 * <p>
 * <b>Metadata Extraction:</b><br>
 * For each ZIP entry, the following metadata is captured:
 * </p>
 * <ul>
 *   <li><b>source:</b> The ZIP file name</li>
 *   <li><b>filename:</b> The entry's full path within the ZIP</li>
 *   <li><b>lastModified:</b> The entry's last modified timestamp (ISO 8601 format)</li>
 *   <li><b>comment:</b> The entry's comment (if present)</li>
 * </ul>
 * <p>
 * <b>Performance Characteristics:</b>
 * </p>
 * <ul>
 *   <li><b>Memory:</b> Reads one entry at a time, keeping only current entry in memory</li>
 *   <li><b>Counting:</b> Uses parallel stream for efficient total entry count</li>
 *   <li><b>Filtering:</b> Automatically excludes directory entries</li>
 * </ul>
 * <p>
 * <b>Configuration Properties:</b>
 * </p>
 * <ul>
 *   <li>{@value Options#ZIP_FILE} - Path to the ZIP archive file to process</li>
 * </ul>
 * <p>
 * <b>Use Cases:</b><br>
 * This loader is ideal for:
 * </p>
 * <ul>
 *   <li>Bulk importing archived files into MarkLogic</li>
 *   <li>Processing backup archives</li>
 *   <li>Ingesting compressed data exports</li>
 *   <li>Handling collections of binary or text files</li>
 * </ul>
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 * @see AbstractFileUrisLoader
 * @see FileUrisDirectoryLoader
 */
public class FileUrisZipLoader extends AbstractFileUrisLoader {

    /**
     * Metadata element name for the ZIP entry comment field.
     * <p>
     * ZIP entries can have optional comment strings associated with them. When present,
     * this metadata field contains the comment text and is included in the loader document
     * envelope. This allows processing modules to access entry-specific annotations or
     * descriptions that were added when the ZIP archive was created.
     * </p>
     * <p>
     * <b>Example:</b> If a ZIP entry has the comment "Customer data backup from 2023-01-15",
     * this will be included as {@code <comment>Customer data backup from 2023-01-15</comment>}
     * in the metadata section of the loader document.
     * </p>
     */
    public static final String META_COMMENT = "comment";

    /**
     * Logger instance for this class.
     */
    protected static final Logger LOG = Logger.getLogger(FileUrisZipLoader.class.getName());

    /**
     * Standard exception message used when an I/O error occurs while reading the ZIP file.
     * <p>
     * This message is used consistently throughout the class when throwing {@link CorbException}
     * for ZIP file-related errors, such as:
     * </p>
     * <ul>
     *   <li>Failure to open the ZIP file</li>
     *   <li>Corruption or invalid ZIP format</li>
     *   <li>I/O errors reading ZIP entries or their content</li>
     * </ul>
     */
    public static final String EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE = "Problem reading zip file";

    /**
     * The ZIP file reader for accessing archive entries and their content.
     * <p>
     * This object is initialized in the {@link #open()} method and provides:
     * </p>
     * <ul>
     *   <li>Access to the ZIP file's entry list via {@link ZipFile#stream()}</li>
     *   <li>Input streams for reading individual entry content</li>
     *   <li>Metadata such as the ZIP file name</li>
     * </ul>
     * <p>
     * The ZipFile remains open throughout the loading process to allow streaming access
     * to entries, and is closed in the {@link #close()} method to release file handles.
     * </p>
     * <p>
     * Set to {@code null} initially and remains {@code null} if the loader has not been
     * opened or if opening failed.
     * </p>
     */
    protected ZipFile zipFile = null;

    /**
     * Iterator over the filtered file entries in the ZIP archive.
     * <p>
     * This iterator is created in the {@link #open()} method and provides sequential access
     * to {@link ZipEntry} objects representing files within the ZIP archive. The iterator:
     * </p>
     * <ul>
     *   <li>Includes only file entries (directories are filtered out)</li>
     *   <li>Maintains the order of entries as stored in the ZIP archive</li>
     *   <li>Supports standard {@link Iterator} operations (hasNext, next)</li>
     * </ul>
     * <p>
     * The wildcard generic type {@code <? extends ZipEntry>} allows for potential subclasses
     * of ZipEntry, though in practice the standard {@link ZipEntry} class is used.
     * </p>
     */
    protected Iterator<? extends ZipEntry> files;

    /**
     * Opens and initializes the ZIP loader by reading the archive and filtering entries.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     *   <li>Retrieves the ZIP file path from {@value Options#ZIP_FILE} property</li>
     *   <li>Opens the ZIP file using {@link ZipFile}</li>
     *   <li>Sets the batch reference to the ZIP file name (if enabled)</li>
     *   <li>Creates a stream of ZIP entries and filters to include only files (not directories)</li>
     *   <li>Counts the total number of file entries using parallel processing</li>
     *   <li>Creates an iterator over the filtered file entries</li>
     * </ol>
     * <p>
     * <b>Filtering:</b> Directory entries (where {@link ZipEntry#isDirectory()} returns {@code true})
     * are automatically excluded from processing.
     * </p>
     *
     * @throws CorbException if the ZIP file cannot be opened, read, or if an I/O error occurs
     */
    @Override
    public void open() throws CorbException {

        String zipFilename = getLoaderPath(Options.ZIP_FILE);

        try {
            File file = FileUtils.getFile(zipFilename);
            zipFile = new ZipFile(file);
            if (shouldSetBatchRef()) {
                batchRef = zipFile.getName();
            }
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE, ex);
        }
        Predicate<ZipEntry> isFile = ze -> !ze.isDirectory();
        files = zipFile.stream().filter(isFile).iterator();
        setTotalCount(Math.toIntExact(zipFile.stream().parallel().filter(isFile).count()));
    }

    /**
     * Checks whether more ZIP file entries are available for processing.
     * <p>
     * This method returns {@code true} if the iterator has more file entries to process.
     * Directory entries are not included in the iteration.
     * </p>
     *
     * @return {@code true} if more file entries are available; {@code false} otherwise
     * @throws CorbException if an error occurs checking entry availability
     */
    @Override
    public boolean hasNext() throws CorbException {
        return files.hasNext();
    }

    /**
     * Returns the next ZIP entry as a loader document XML string.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Retrieves the next {@link ZipEntry} from the iterator</li>
     *   <li>Extracts metadata from the entry (filename, last modified, comment, etc.)</li>
     *   <li>Opens a buffered input stream for the entry's content</li>
     *   <li>Creates a loader document with metadata and base64-encoded content</li>
     *   <li>Serializes the loader document to an XML string</li>
     * </ol>
     * <p>
     * The entry's content is automatically base64 encoded to ensure binary data
     * can be safely represented in XML.
     * </p>
     *
     * @return the ZIP entry as a loader document XML string
     * @throws CorbException if an I/O error occurs reading the entry or creating the loader document
     */
    @Override
    public String next() throws CorbException {
        ZipEntry zipEntry = files.next();
        Map<String, String> metadata = getMetadata(zipEntry);
        try (InputStream stream = new BufferedInputStream(zipFile.getInputStream(zipEntry))) {
            return XmlUtils.documentToString(toLoaderDoc(metadata, stream));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE, ex);
        }
    }

    /**
     * This operation is not supported for ZIP file processing.
     * <p>
     * The {@link FileUrisZipLoader} works with {@link ZipEntry} objects rather than
     * {@link File} objects. Use {@link #getMetadata(ZipEntry)} instead.
     * </p>
     *
     * @param file the file (not used)
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    protected Map<String, String> getMetadata(File file) {
        throw new UnsupportedOperationException("Invalid operation for FileUrisZipLoader. Invoke with a ZipEntry.");
    }

    /**
     * Extracts metadata from a ZIP entry for inclusion in the loader document.
     * <p>
     * The following metadata fields are extracted:
     * </p>
     * <ul>
     *   <li><b>{@link #META_SOURCE}:</b> The name of the ZIP file</li>
     *   <li><b>{@link #META_FILENAME}:</b> The full path/name of the entry within the ZIP</li>
     *   <li><b>{@link #META_LAST_MODIFIED}:</b> The entry's last modified timestamp in ISO 8601
     *       format (if available)</li>
     *   <li><b>{@link #META_COMMENT}:</b> The entry's comment (if present and not empty)</li>
     * </ul>
     * <p>
     * This metadata is included in the loader document envelope and can be accessed by
     * process modules for conditional logic or auditing purposes.
     * </p>
     *
     * @param zipEntry the ZIP entry to extract metadata from
     * @return a map of metadata key-value pairs
     */
    protected Map<String, String> getMetadata(ZipEntry zipEntry) {
        Map<String, String> metadata = new HashMap<>();
        if (zipFile != null) {
            metadata.put(META_SOURCE, zipFile.getName());
        }
        metadata.put(META_FILENAME, zipEntry.getName());

        FileTime lastModifiedTime = zipEntry.getLastModifiedTime();
        if (lastModifiedTime != null) {
            metadata.put(META_LAST_MODIFIED, toISODateTime(lastModifiedTime));
        }
        String comment = zipEntry.getComment();
        if (StringUtils.isNotEmpty(comment)) {
            metadata.put(META_COMMENT, comment);
        }
        return metadata;
    }

    /**
     * Closes the loader and releases the ZIP file resource.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Calls the parent class {@link #close()} method</li>
     *   <li>Closes the {@link ZipFile} quietly (logging but not throwing exceptions)</li>
     * </ol>
     * <p>
     * <b>Important:</b> Failing to close the ZIP file can result in file handle leaks,
     * preventing deletion or modification of the ZIP file.
     * </p>
     */
    @Override
    public void close() {
        super.close();
        IOUtils.closeQuietly(zipFile);
    }
}
