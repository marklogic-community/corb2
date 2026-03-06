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

import static com.marklogic.developer.corb.Options.EXPORT_FILE_URI_TO_PATH;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trimToEmpty;

import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.ResultSequence;

import java.io.*;
import java.nio.file.Files;

/**
 * A task implementation for exporting query results from MarkLogic to files on the local file system.
 * <p>
 * This class extends {@link AbstractTask} and provides comprehensive file export capabilities:
 * </p>
 * <ul>
 *   <li>Automatic file naming based on input URIs</li>
 *   <li>Configurable export directory structure</li>
 *   <li>Support for preserving or flattening URI path structure</li>
 *   <li>Line-by-line writing of result sequences</li>
 *   <li>Automatic creation of parent directories</li>
 * </ul>
 * <p>
 * <b>File Naming:</b><br>
 * By default, the export file name is derived from the first input URI. Leading forward slashes
 * are removed, and the URI path can optionally be preserved or flattened based on the
 * {@value com.marklogic.developer.corb.Options#EXPORT_FILE_URI_TO_PATH} property.
 * </p>
 * <p>
 * <b>Export Directory:</b><br>
 * Files are written to the configured export directory (via the {@code exportDir} field inherited
 * from {@link AbstractTask}). If not configured, files are written to the current working directory.
 * Parent directories are created automatically if they don't exist.
 * </p>
 * <p>
 * <b>Output Format:</b><br>
 * Each item in the result sequence is written as a separate line, with items separated by
 * newline characters. The content is written using buffered output streams for efficiency.
 * </p>
 * <p>
 * <b>Configuration Properties:</b>
 * </p>
 * <ul>
 *   <li>{@value com.marklogic.developer.corb.Options#EXPORT_FILE_URI_TO_PATH} - Controls whether
 *       to preserve URI path structure (true) or use only the file name (false)</li>
 *   <li>{@value com.marklogic.developer.corb.Options#EXPORT_FILE_REQUIRE_PROCESS_MODULE} - Controls
 *       whether a process module is required (default: true)</li>
 *   <li>{@value com.marklogic.developer.corb.Options#PROCESS_MODULE} - Specifies the XQuery or
 *       JavaScript module to execute for generating export content</li>
 * </ul>
 * <p>
 * <b>Thread Safety:</b><br>
 * This base class does not provide synchronization for file writes. Subclasses may implement
 * synchronization if concurrent writes to the same file are expected (see {@link ExportBatchToFileTask}).
 * </p>
 *
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @since 1.0
 * @see ExportBatchToFileTask
 * @see AbstractTask
 */
public class ExportToFileTask extends AbstractTask {

	/**
	 * Returns the export file name by delegating to {@link #getExportFileName()}.
	 * This method can be overridden by subclasses to provide custom file naming strategies.
	 *
	 * @return the export file name
	 */
	protected String getFileName() {
		return getExportFileName();
	}

    /**
     * Derives the export file name from the first input URI with optional path structure preservation.
     * <p>
     * The file name resolution process:
     * </p>
     * <ol>
     *   <li>Takes the first URI from {@code inputUris[0]}</li>
     *   <li>Removes the leading '/' character if present</li>
     *   <li>If {@value com.marklogic.developer.corb.Options#EXPORT_FILE_URI_TO_PATH} is "false",
     *       extracts only the file name portion (substring after the last '/')</li>
     *   <li>Otherwise, preserves the full URI path structure</li>
     * </ol>
     * <p>
     * <b>Examples:</b><br>
     * For URI "/content/docs/example.xml":
     * </p>
     * <ul>
     *   <li>With EXPORT_FILE_URI_TO_PATH="true" or unset: "content/docs/example.xml"</li>
     *   <li>With EXPORT_FILE_URI_TO_PATH="false": "example.xml"</li>
     * </ul>
     *
     * @return the derived export file name
     */
    protected String getExportFileName() {
        String filename = inputUris[0].charAt(0) == '/' ? inputUris[0].substring(1) : inputUris[0];
        String uriInPath = getProperty(EXPORT_FILE_URI_TO_PATH);
        int lastIdx = filename.lastIndexOf('/');
        if ("false".equalsIgnoreCase(uriInPath) && lastIdx > 0 && filename.length() > (lastIdx + 1)) {
            filename = filename.substring(lastIdx + 1);
        }
        return filename;
    }

	/**
	 * Writes a result sequence to the specified export file.
	 * <p>
	 * This method creates a buffered output stream for the file and writes the entire
	 * result sequence. The file is created if it doesn't exist, or overwritten if it does.
	 * The stream is automatically closed after writing (using try-with-resources).
	 * </p>
	 *
	 * @param seq the {@link ResultSequence} to write; may be null or empty
	 * @param exportFile the {@link File} to write to; parent directories will be created if needed
	 * @throws IOException if an I/O error occurs during writing
	 */
	protected void writeToFile(ResultSequence seq, File exportFile) throws IOException {
        if (seq == null || !seq.hasNext()) {
            return;
        }
        try (OutputStream writer = new BufferedOutputStream(Files.newOutputStream(exportFile.toPath()))) {
            write(seq, writer);
        }
    }

    /**
     * Helper method to append content to a specific file
     * @param content the content to write
     * @param file the file to append content
     * @throws IOException if an I/O error occurs
     */
    protected void appendToFile(String content, File file) throws IOException {
        String trimmedContent = trimToEmpty(content);
        if (isNotEmpty(trimmedContent)) {
            try (BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file, true))) {
                writer.write(trimmedContent.getBytes());
                writer.write(NEWLINE);
            }
        }
    }

    /**
     * Writes each item in a result sequence to an output stream, one item per line.
     * <p>
     * This method iterates through the result sequence and:
     * </p>
     * <ol>
     *   <li>Converts each result item to bytes using {@link #getValueAsBytes(com.marklogic.xcc.types.XdmItem)}</li>
     *   <li>Writes the bytes to the output stream</li>
     *   <li>Appends a newline character after each item</li>
     *   <li>Flushes the stream after all items are written</li>
     * </ol>
     *
     * @param seq the {@link ResultSequence} containing items to write; should not be null
     * @param writer the {@link OutputStream} to write to; should not be null
     * @throws IOException if an I/O error occurs during writing
     */
    protected void write(ResultSequence seq, OutputStream writer) throws IOException {
        while (seq.hasNext()) {
            writer.write(getValueAsBytes(seq.next().getItem()));
            writer.write(NEWLINE);
        }
        writer.flush();
    }

    /**
     * Returns a File object for the export file using the default file name.
     * This method delegates to {@link #getExportFile(String)} with the file name
     * obtained from {@link #getFileName()}.
     *
     * @return a {@link File} object representing the export file
     */
    protected File getExportFile() {
        return getExportFile(getFileName());
    }

    /**
     * Returns a File object for the export file with the specified file name,
     * resolved relative to the configured export directory.
     * <p>
     * The file resolution process:
     * </p>
     * <ol>
     *   <li>If {@code exportDir} is configured, the file is created relative to that directory</li>
     *   <li>If {@code exportDir} is not configured, the file is created in the current working directory</li>
     *   <li>Parent directories are automatically created if they don't exist</li>
     * </ol>
     * <p>
     * <b>Example:</b><br>
     * If exportDir="/output" and fileName="content/docs/example.xml",
     * this returns a File for "/output/content/docs/example.xml" with directories
     * "/output/content/docs" created if they don't exist.
     * </p>
     *
     * @param fileName the file name (may include path separators); should not be null
     * @return a {@link File} object representing the export file with parent directories created
     */
    protected File getExportFile(String fileName) {
        File exportFile = new File(exportDir, fileName);
        exportFile.getAbsoluteFile().getParentFile().mkdirs();
        return exportFile;
    }

	/**
	 * Processes the result sequence from the module execution by writing it to the export file.
	 * <p>
	 * This method overrides {@link AbstractTask#processResult(ResultSequence)} to provide
	 * file export functionality. It delegates to {@link #writeToFile(ResultSequence, File)} to perform
	 * the actual file writing.
	 * </p>
	 *
	 * @param seq the {@link ResultSequence} returned from the module execution
	 * @return the string {@code "true"} if writing succeeds
	 * @throws CorbException if an I/O error occurs during writing, wrapping the {@link IOException}
	 */
	@Override
	protected String processResult(ResultSequence seq) throws CorbException {
        if (seq == null || !seq.hasNext()) {
            return TRUE;
        }
		try {
			writeToFile(seq, getExportFile());
			return TRUE;
		} catch (IOException exc) {
			throw new CorbException(exc.getMessage(), exc);
		}
	}

    /**
     * Indicates whether this task requires a process module to be configured.
     * <p>
     * This method returns {@code true} by default, meaning that a process module
     * (XQuery or JavaScript) must be specified for the task to execute. Subclasses
     * can override this method to allow execution without a process module.
     * </p>
     *
     * @return {@code true} if a process module is required; {@code false} otherwise
     */
    protected boolean shouldHaveProcessModule(){
        return true;
    }

    /**
     * Invokes the configured process module after validating that a module is specified if required.
     * <p>
     * This method overrides {@link AbstractTask#invokeModule()} to add validation logic:
     * </p>
     * <ul>
     *   <li>If {@link #shouldHaveProcessModule()} returns {@code true}</li>
     *   <li>AND neither {@code adhocQuery} nor {@code moduleUri} is specified</li>
     *   <li>AND {@value com.marklogic.developer.corb.Options#EXPORT_FILE_REQUIRE_PROCESS_MODULE}
     *       property is not "false"</li>
     *   <li>THEN a {@link CorbException} is thrown</li>
     * </ul>
     * <p>
     * The {@value com.marklogic.developer.corb.Options#EXPORT_FILE_REQUIRE_PROCESS_MODULE} property
     * can be set to "false" to bypass the module requirement check.
     * </p>
     *
     * @return an array of strings returned from the module execution
     * @throws CorbException if a process module is required but not specified, or if
     *         an error occurs during module execution
     */
    @Override
    protected String[] invokeModule() throws CorbException {
        if (shouldHaveProcessModule() &&
            StringUtils.isEmpty(adhocQuery) && StringUtils.isEmpty(moduleUri)
            && StringUtils.stringToBoolean(getProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE), true)) {
            throw new CorbException(Options.PROCESS_MODULE + " must be specified.");
        }
        return super.invokeModule();
    }
}

