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

import com.marklogic.developer.corb.util.NumberUtils;
import com.marklogic.xcc.ResultSequence;

import java.io.*;
import java.nio.file.Files;
import java.util.logging.Logger;

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
     * Logger instance for logging task execution, errors, retries, and diagnostic information.
     */
    private static final Logger LOG = Logger.getLogger(ExportBatchToFileTask.class.getName());

    private int currentFileIndex = 0;
	private long currentFileLineCount = 0;
	private long currentFileSize = 0;

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
		if (isNotEmpty(fileName)) {
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
		synchronized (SYNC_OBJ) {
			long maxLines = getMaxLines();
			long maxSize = getMaxSize();

			File currentFile = getCurrentSplitFile();
			try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(currentFile, true))){
				writeWithSplitting(seq, writer, maxLines, maxSize);
			}
		}
	}

	/**
	 * Writes the ResultSequence to the output stream, splitting files as needed
	 * based upon line count or file size thresholds.
	 *
	 * @param seq the ResultSequence to write
	 * @param writer the OutputStream to write to
	 * @param maxLines maximum lines per file (0 or negative to disable)
	 * @param maxSize maximum bytes per file (0 or negative to disable)
	 * @throws IOException if an I/O error occurs
	 */
	protected void writeWithSplitting(ResultSequence seq, OutputStream writer, long maxLines, long maxSize) throws IOException {
		while (seq != null && seq.hasNext()) {
			// Check if we need to rotate to a new file
			boolean needsRotation = false;
			if (maxLines > 0 && currentFileLineCount >= maxLines) {
				needsRotation = true;
			} else if (maxSize > 0 && currentFileSize >= maxSize) {
				needsRotation = true;
			}

			if (needsRotation) {
				writer.flush();
				writer.close();
				currentFileIndex++;
				currentFileLineCount = 0;
				currentFileSize = 0;
				File nextFile = getCurrentSplitFile();

                if (currentFileIndex > 1) {
                    int headerLineCount = getIntProperty(EXPORT_FILE_HEADER_LINE_COUNT);
                    if (headerLineCount > 0) {
                        File baseFile = getExportFile();
                        copyHeaderIntoFile(baseFile, headerLineCount, nextFile);
                    }
                }
				writer = new BufferedOutputStream(new FileOutputStream(nextFile, true));
			}

			byte[] valueBytes = getValueAsBytes(seq.next().getItem());
            writer.write(valueBytes);
			writer.write(NEWLINE);

			currentFileLineCount++;
			currentFileSize += valueBytes.length + NEWLINE.length;
		}
		writer.flush();
        writer.close();
	}

    /**
     * Copies header lines from the input file to the output file.
     * <p>
     * This is used during sorting to preserve header lines that should not be sorted
     * with the data lines.
     * </p>
     *
     * @param inputFile the file to read header lines from
     * @param headerLineCount the number of header lines to copy
     * @param outputFile the file to write header lines to
     * @throws IOException if an I/O error occurs
     */
    protected void copyHeaderIntoFile(File inputFile, int headerLineCount, File outputFile) throws IOException {

        try (BufferedReader reader = Files.newBufferedReader(inputFile.toPath());
             BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath()) ) {
            String line;
            int currentLine = 0;
            while ((line = reader.readLine()) != null && currentLine < headerLineCount) {
                writer.write(line);
                writer.newLine();
                currentLine++;
            }
            writer.flush();
        }
    }

	/**
	 * Gets the maximum number of lines per file from configuration.
	 *
	 * @return maximum lines per file, or -1 if not configured
	 */
	protected long getMaxLines() {
		String maxLinesStr = getProperty(EXPORT_FILE_SPLIT_MAX_LINES);
		if (isNotEmpty(maxLinesStr)) {
			try {
				return Long.parseLong(maxLinesStr.trim());
			} catch (NumberFormatException e) {
                LOG.warning("Invalid value for " + EXPORT_FILE_SPLIT_MAX_LINES + ": " + maxLinesStr);
			}
		}
		return -1;
	}

	/**
	 * Gets the maximum file size in bytes from configuration.
	 *
	 * @return maximum file size in bytes, or -1 if not configured
	 */
	protected long getMaxSize() {
		String maxSizeStr = getProperty(EXPORT_FILE_SPLIT_MAX_SIZE);
		if (isNotEmpty(maxSizeStr)) {
			try {
				return NumberUtils.parseSize(maxSizeStr);
			} catch (NumberFormatException e) {
                LOG.warning("Invalid value for " + EXPORT_FILE_SPLIT_MAX_SIZE + ": " + maxSizeStr);
			}
		}
		return -1;
	}

	/**
	 * Gets the current split file based on the file index.
	 * If index is 0, returns the base filename. Otherwise, appends the index
	 * before the file extension.
	 *
	 * @return the File object for the current split file
	 */
	protected File getCurrentSplitFile() {
		String baseFileName = getPartFileName();
		if (currentFileIndex == 0) {
			return getExportFile(baseFileName);
		}

		// Insert the index before the file extension
		String splitFileName = insertIndexIntoFileName(baseFileName, currentFileIndex);
		return getExportFile(splitFileName);
	}

	/**
	 * Inserts an index into a filename before the extension.
	 * For example: "output.txt" with index 1 becomes "output1.txt"
	 *
	 * @param fileName the base filename
	 * @param index the index to insert
	 * @return the filename with the index inserted
	 */
	protected String insertIndexIntoFileName(String fileName, int index) {
        String regex = "(?<path>.*\\\\|/)*((?<filename>.+?)(?<ext>\\.\\w+)?)$";
        String replacement = "${path}${filename}"+index+"${ext}";
        return fileName.replaceFirst(regex, replacement);
	}

}
