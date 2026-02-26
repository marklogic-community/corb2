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

import static com.marklogic.developer.corb.Options.EXPORT_FILE_HEADER_LINE_COUNT;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_TOP_CONTENT;
import com.marklogic.developer.corb.util.FileUtils;

import java.io.*;

/**
 * Pre-batch task for initializing and preparing export files before batch processing begins.
 * <p>
 * PreBatchUpdateFileTask extends {@link ExportBatchToFileTask} to provide initialization
 * capabilities for export files before any batch processing occurs. This task typically runs
 * as the {@code PRE-BATCH-TASK} and performs:
 * </p>
 * <ul>
 * <li>Deletion of any existing export file (clean slate)</li>
 * <li>Writing header/top content to the file</li>
 * <li>Executing the PRE-BATCH-MODULE (if configured)</li>
 * <li>Tracking the number of header lines for later use</li>
 * </ul>
 * <p>
 * The header line count is particularly important for post-processing operations like sorting,
 * where header lines need to be preserved and not sorted with the data. The count is stored
 * in the properties as {@link Options#EXPORT_FILE_HEADER_LINE_COUNT} and can be used by
 * {@link PostBatchUpdateFileTask}.
 * </p>
 * <p>
 * Configuration options:
 * </p>
 * <ul>
 * <li>{@link Options#EXPORT_FILE_TOP_CONTENT} - Content to write at the top of the file (header)</li>
 * <li>{@link Manager#URIS_BATCH_REF} - Batch reference that can be substituted in top content</li>
 * <li>{@link Options#EXPORT_FILE_HEADER_LINE_COUNT} - Set by this task to track header line count</li>
 * </ul>
 * <p>
 * The top content can include the placeholder {@code @URIS_BATCH_REF} which will be replaced
 * with the actual batch reference value returned by the URIS-MODULE. This is useful for
 * including dynamic information in file headers.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * # Static header
 * EXPORT-FILE-TOP-CONTENT=Name,Age,Email
 *
 * # Header with batch reference
 * EXPORT-FILE-TOP-CONTENT=Report for batch: @URIS_BATCH_REF
 * </pre>
 *
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see ExportBatchToFileTask
 * @see PostBatchUpdateFileTask
 * @see Options#EXPORT_FILE_TOP_CONTENT
 * @see Manager#URIS_BATCH_REF
 */
public class PreBatchUpdateFileTask extends ExportBatchToFileTask {

	/**
	 * Retrieves and processes the top/header content to write to the export file.
	 * <p>
	 * This method:
     * </p>
	 * <ol>
	 * <li>Gets the top content template from {@link Options#EXPORT_FILE_TOP_CONTENT}</li>
	 * <li>Gets the batch reference from {@link Manager#URIS_BATCH_REF}</li>
	 * <li>Replaces any {@code @URIS_BATCH_REF} placeholder with the actual batch reference</li>
	 * </ol>
	 * <p>
	 * This allows for dynamic headers that include information about the current batch,
	 * such as a timestamp, query identifier, or other context returned by the URIS-MODULE.
	 * </p>
	 *
	 * @return the processed top content string, or null if not configured
	 */
	protected String getTopContent() {
		String topContent = getProperty(EXPORT_FILE_TOP_CONTENT);
		String batchRef = getProperty(Manager.URIS_BATCH_REF);
		if (topContent != null && batchRef != null) {
			topContent = topContent.replace('@' + Manager.URIS_BATCH_REF, batchRef);
		}
		return topContent;
	}

	/**
	 * Deletes the export file if it exists.
	 * <p>
	 * This ensures a clean start for the new export by removing any leftover
	 * files from previous runs. This is important for preventing data corruption
	 * or appending to old data.
	 * </p>
	 *
	 * @throws IOException if an error occurs while deleting the file
	 */
	private void deleteFileIfExists() throws IOException {
		File batchFile = getExportFile();
        FileUtils.deleteFile(batchFile);
	}

	/**
	 * Writes the top/header content to the export file.
	 * <p>
	 * The content is retrieved via {@link #getTopContent()} and written
	 * using {@link #writeToExportFile(String)}. If no top content is configured,
	 * this method does nothing.
	 * </p>
	 *
	 * @throws IOException if an I/O error occurs while writing
	 */
	protected void writeTopContent() throws IOException {
		String topContent = getTopContent();
		writeToExportFile(topContent);
	}

	/**
	 * Counts the header lines and stores the count in properties.
	 * <p>
	 * This method:
     * </p>
	 * <ol>
	 * <li>Counts the number of lines in the export file (after writing top content and PRE-BATCH-MODULE output)</li>
	 * <li>Stores the count as {@link Options#EXPORT_FILE_HEADER_LINE_COUNT} in the properties</li>
	 * </ol>
	 * <p>
	 * The header line count is used by {@link PostBatchUpdateFileTask} during sorting
	 * operations to preserve header lines and prevent them from being sorted with data lines.
	 * </p>
	 *
	 * @throws IOException if an error occurs while counting lines
	 */
	private void addLineCountToProps() throws IOException{
		int ct = FileUtils.getLineCount(getExportFile());
		if (this.properties != null && ct > 0) {
			this.properties.setProperty(EXPORT_FILE_HEADER_LINE_COUNT, String.valueOf(ct));
		}
	}

    /**
     * Indicates whether a PROCESS-MODULE is required for this task.
     * <p>
     * Returns false because this is a pre-batch task that initializes the export file,
     * not a task that processes individual URIs.
     * </p>
     *
     * @return false (PROCESS-MODULE is not required)
     */
    @Override
    protected boolean shouldHaveProcessModule(){
        return false;
    }

	/**
	 * Executes the pre-batch file initialization operations.
	 * <p>
	 * This method performs the following operations in sequence:
     * </p>
	 * <ol>
	 * <li>Delete any existing export file (clean start)</li>
	 * <li>Write top/header content (if configured)</li>
	 * <li>Invoke the PRE-BATCH-MODULE (if configured)</li>
	 * <li>Count and store the header line count for later use</li>
	 * <li>Clean up temporary resources</li>
	 * </ol>
	 * <p>
	 * All write operations are performed before any batch processing begins,
	 * ensuring the export file is properly initialized with headers and any
	 * preliminary data.
	 * </p>
	 *
	 * @return an empty string array (task result)
	 * @throws Exception if an error occurs during initialization
	 */
	@Override
	public String[] call() throws Exception {
		try {
			deleteFileIfExists();
			writeTopContent();
			invokeModule();
			addLineCountToProps();
			return new String[0];
		} finally {
			cleanup();
		}
	}

}
