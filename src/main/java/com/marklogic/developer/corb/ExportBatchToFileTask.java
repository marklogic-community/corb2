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

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.util.StringUtils.*;

import com.marklogic.xcc.ResultSequence;

import java.io.*;

/**
 * A specialized task for exporting batch results to files with support for batch-based naming
 * and synchronized file writing operations.
 * <p>
 * This class extends {@link ExportToFileTask} and provides enhanced functionality for batch processing:
 * </p>
 * <ul>
 *   <li>Automatic file naming based on batch references</li>
 *   <li>Support for part file extensions via {@value com.marklogic.developer.corb.Options#EXPORT_FILE_PART_EXT}</li>
 *   <li>Synchronized writing to prevent concurrent write conflicts</li>
 *   <li>File append mode to consolidate batch results</li>
 * </ul>
 * <p>
 * <b>File Naming Strategy:</b><br>
 * The export file name is determined in the following order:
 * </p>
 * <ol>
 *   <li>Explicitly specified via {@value com.marklogic.developer.corb.Options#EXPORT_FILE_NAME} property</li>
 *   <li>Derived from the batch reference URI ({@value com.marklogic.developer.corb.Options#URIS_BATCH_REF})</li>
 *   <li>If neither is available, a {@link NullPointerException} is thrown</li>
 * </ol>
 * <p>
 * <b>Thread Safety:</b><br>
 * File write operations are synchronized to ensure thread-safe appending when multiple tasks
 * write to the same export file. This is critical in batch processing scenarios where multiple
 * threads may process different batches that contribute to the same output file.
 * </p>
 * <p>
 * <b>Configuration Properties:</b>
 * </p>
 * <ul>
 *   <li>{@value com.marklogic.developer.corb.Options#EXPORT_FILE_NAME} - Explicit export file name</li>
 *   <li>{@value com.marklogic.developer.corb.Options#URIS_BATCH_REF} - Batch reference for deriving file name</li>
 *   <li>{@value com.marklogic.developer.corb.Options#EXPORT_FILE_PART_EXT} - Optional part file extension</li>
 * </ul>
 *
 * @author MarkLogic Corporation
 * @since 2.0.0
 * @see ExportToFileTask
 */
public class ExportBatchToFileTask extends ExportToFileTask {

	private static final Object SYNC_OBJ = new Object();

	/**
	 * Returns the export file name by delegating to {@link #getExportBatchFileName()}.
	 * This method overrides the parent implementation to provide batch-specific file naming.
	 *
	 * @return the export file name for this batch task
	 */
	@Override
	protected String getFileName() {
		return getExportBatchFileName();
	}

	/**
	 * Determines the export batch file name using a fallback strategy.
	 * <p>
	 * The file name resolution order:
     * </p>
	 * <ol>
	 *   <li>Check the {@value com.marklogic.developer.corb.Options#EXPORT_FILE_NAME} property</li>
	 *   <li>If not set, extract from {@value com.marklogic.developer.corb.Options#URIS_BATCH_REF} property
	 *       by taking the substring after the last '/' character</li>
	 *   <li>If neither property provides a value, throw {@link NullPointerException}</li>
	 * </ol>
	 *
	 * @return the resolved export batch file name
	 * @throws NullPointerException if neither EXPORT_FILE_NAME nor URIS_BATCH_REF properties
	 *         are configured or provide a valid file name
	 */
	protected String getExportBatchFileName() {
        String fileName = getProperty(EXPORT_FILE_NAME);
        if (isEmpty(fileName)) {
            String batchRef = trim(getProperty(Manager.URIS_BATCH_REF));
            if (isNotEmpty(batchRef)) {
                fileName = batchRef.substring(batchRef.lastIndexOf('/') + 1);
            }
        }
        if (isEmpty(fileName)) {
            throw new NullPointerException("Missing " + EXPORT_FILE_NAME + " or " + URIS_BATCH_REF + " property");
        }
        return fileName;
    }

	/**
	 * Returns the file name with an optional part extension appended.
	 * <p>
	 * If the {@value com.marklogic.developer.corb.Options#EXPORT_FILE_PART_EXT} property is configured
	 * and the base file name is not empty, this method appends the part extension to the file name.
	 * The extension is automatically prefixed with a '.' if it doesn't already start with one.
	 * </p>
	 * <p>
	 * <b>Example:</b><br>
	 * If file name is "batch-001" and EXPORT_FILE_PART_EXT is "tmp",
	 * this returns "batch-001.tmp"
	 * </p>
	 *
	 * @return the file name with the part extension appended, or the original file name
	 *         if no part extension is configured or the file name is empty
	 */
	protected String getPartFileName() {
        String fileName = getFileName();
		return getPartFileName(fileName);
	}

    /**
     * Returns the specified file name with an optional part extension appended.
     * <p>
     * If the {@value com.marklogic.developer.corb.Options#EXPORT_FILE_PART_EXT} property is configured
     * and the provided file name is not empty, this method appends the part extension to the file name.
     * The extension is automatically prefixed with a '.' if it doesn't already start with one.
     * </p>
     * <p>
     * <b>Example:</b><br>
     * If fileName is "batch-001" and EXPORT_FILE_PART_EXT is "tmp",
     * this returns "batch-001.tmp"
     * </p>
     *
     * @param fileName the base file name to append the part extension to
     * @return the file name with the part extension appended, or the original file name
     *         if no part extension is configured or the file name is empty
     */
    protected String getPartFileName(String fileName) {
        if (isNotEmpty(fileName)) {
            //If not specified, then it won't append a file extension
            String partExt = getProperty(EXPORT_FILE_PART_EXT);
            if (isNotEmpty(partExt)) {
                if (!partExt.startsWith(".")) {
                    partExt = '.' + partExt;
                }
                fileName += partExt;
            }
        }
        return fileName;
    }

    /**
     * Gets the part file extension for export files.
     * <p>
     * The extension is configured via {@value com.marklogic.developer.corb.Options#EXPORT_FILE_PART_EXT}.
     * If not configured, defaults to ".part". Ensures the extension starts with a dot.
     * </p>
     *
     * @return the part file extension (including leading dot)
     */
    protected String getPartExt() {
        String partExt = getProperty(EXPORT_FILE_PART_EXT);
        if (isEmpty(partExt)) {
            partExt = ".part";
        } else if (!partExt.startsWith(".")) {
            partExt = '.' + partExt;
        }
        return partExt;
    }

	/**
	 * Returns the export file with the part file name (including any part extension).
	 * This method overrides the parent implementation to use the part file name instead
	 * of the base file name.
	 *
	 * @return the export {@link File} object based on the part file name
	 */
	@Override
    protected File getExportFile() {
        return getExportFile(getPartFileName());
    }

    /**
     * Writes the result sequence to the export file in a thread-safe manner.
     * <p>
     * This method overrides the parent implementation to provide:
     * </p>
     * <ul>
     *   <li><b>Synchronized writing:</b> Uses the {@link #SYNC_OBJ} monitor to ensure that
     *       only one thread writes to the file at a time</li>
     *   <li><b>Append mode:</b> Opens the file in append mode (true) to add results to the end
     *       of existing content rather than overwriting</li>
     *   <li><b>Buffered output:</b> Wraps the file output stream in a {@link BufferedOutputStream}
     *       for improved write performance</li>
     * </ul>
     * <p>
     * The synchronization ensures that when multiple batch tasks write to the same export file,
     * their outputs don't interleave or corrupt each other.
     * </p>
     *
     * @param seq the {@link ResultSequence} containing the data to write
     * @param exportFile the {@link File} to write the results to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    protected void writeToFile(ResultSequence seq, File exportFile) throws IOException {
        if (seq == null || !seq.hasNext()) {
            return;
        }
        synchronized (SYNC_OBJ) {
            try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(exportFile, true))){
                write(seq, writer);
            }
        }
    }

}
