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

import org.xml.sax.SAXParseException;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.List;

/**
 * Validates XML items against an XML Schema and writes validation errors to individual files.
 * <p>
 * SchemaValidateToFileTask extends {@link SchemaValidateBatchToFileTask} but changes the output
 * behavior to write validation errors to separate files for each URI, rather than appending all
 * errors to a single batch file. This task:
 * </p>
 * <ul>
 * <li>Retrieves XML items from the PROCESS-MODULE result sequence</li>
 * <li>Validates each item against the configured XML Schema</li>
 * <li>Collects validation errors (SAXParseExceptions)</li>
 * <li>Writes errors to individual files (one per URI/document)</li>
 * <li>Logs successful validations (no errors) without creating files</li>
 * </ul>
 * <p>
 * Key differences from {@link SchemaValidateBatchToFileTask}:
 * </p>
 * <ul>
 * <li>Creates separate output files per URI instead of one batch file</li>
 * <li>Overwrites files instead of appending (no EXPORT-FILE-PART extension)</li>
 * <li>Does not require synchronized writing (each thread writes to its own file)</li>
 * </ul>
 * <p>
 * This task is useful when you need to maintain separate validation reports for each document,
 * making it easier to identify and fix individual documents with errors.
 * </p>
 * <p>
 * Configuration options:
 * </p>
 * <ul>
 * <li>{@link Options#XML_SCHEMA} - Path to the XML Schema (.xsd) file (required)</li>
 * <li>{@link Options#EXPORT_FILE_DIR} - Directory for output files (required)</li>
 * <li>{@link Options#EXPORT_FILE_NAME} - Not used; filenames are based on URIs</li>
 * </ul>
 * <p>
 * Output file naming:
 * Files are created in the {@link Options#EXPORT_FILE_DIR} directory with names based on the
 * URI being validated. The URI is converted to a filesystem-safe filename.
 * </p>
 * <p>
 * Output format (same as parent class):
 * </p>
 * <pre>{@code
 * <document uri="/path/to/doc.xml">
 *   <error lineNumber="10" columnNumber="5" systemId="/path/to/doc.xml">
 *     cvc-complex-type.2.4.a: Invalid content was found...
 *   </error>
 *   <error lineNumber="15" columnNumber="12">
 *     cvc-datatype-valid.1.2.1: 'invalid' is not a valid value...
 *   </error>
 * </document>
 * }</pre>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * PROCESS-MODULE=getXmlDocuments.xqy
 * PROCESS-TASK=com.marklogic.developer.corb.SchemaValidateToFileTask
 * XML-SCHEMA=/path/to/schema.xsd
 * EXPORT-FILE-DIR=/path/to/validation-reports
 * </pre>
 *
 * @author MarkLogic Corporation
 * @see SchemaValidateBatchToFileTask
 * @see ExportToFileTask
 * @see Options#XML_SCHEMA
 * @see Options#EXPORT_FILE_DIR
  * @since 2.4.1
 */
public class SchemaValidateToFileTask extends SchemaValidateBatchToFileTask {

    /**
     * Gets the filename for the current export file.
     * <p>
     * Overrides the parent class method to return individual filenames per URI
     * instead of a batch filename. This ensures each validated document gets its
     * own validation report file.
     * </p>
     * <p>
     * The filename is derived from the URI being processed and converted to a
     * filesystem-safe format.
     * </p>
     *
     * @return the individual export filename (not the batch filename)
     * @see ExportBatchToFileTask#getExportFileName()
     */
    @Override
    protected String getFileName() {
        //this extends SchemaValidateBatchToFileTask, which produces the Batch filename instead of individual files per URI,
        //so, need to change to a single Export file per document
        return getExportFileName();
    }

    /**
     * Gets the export file for writing validation errors.
     * <p>
     * Overrides the parent class method to avoid appending the EXPORT-FILE-PART
     * temporary extension. Since we're writing individual files (not appending to
     * a batch file), there's no need for temporary extensions.
     * </p>
     * <p>
     * This method directly returns a File based on {@link #getFileName()} without
     * any temporary extensions.
     * </p>
     *
     * @return the File object for the export file (without part extension)
     * @see ExportBatchToFileTask#getExportFile()
     */
    @Override
    protected File getExportFile() {
        // this extends SchemaValidateBatchToFileTask, which extends ExportBatchToFileTask and would get a File that could
        // have the EXPORT_FILE_PART extension. We just want the regular filename.
        return getExportFile(getFileName());
    }

    /**
     * Writes schema validation errors to the output file.
     * <p>
     * Overrides the parent class method to change the file writing behavior:
     * </p>
     * <ul>
     * <li>Opens the file in WRITE mode (not APPEND) since each file is new</li>
     * <li>Does not synchronize writes since each thread writes to its own file</li>
     * <li>Creates a new file for each URI/document being validated</li>
     * </ul>
     * <p>
     * The parent class appends to a shared batch file and requires synchronization.
     * This implementation writes to individual files, so synchronization is unnecessary
     * and would only hurt performance.
     * </p>
     *
     * @param exceptions list of validation errors to write
     * @param outputFile the file to write errors to (unique per URI)
     * @throws IOException if file I/O fails
     * @throws XMLStreamException if XML writing fails
     * @see SchemaValidateBatchToFileTask#writeSchemaValidationReport(List, File)
     */
    @Override
    protected void writeSchemaValidationReport(List<SAXParseException> exceptions, File outputFile) throws IOException, XMLStreamException {
        //Since these validation reports are per doc, the FileWriter will not append, and no need for synchronized writes
        try (Writer writer = Files.newBufferedWriter(outputFile.toPath())) {
            writeSchemaValidationReport(exceptions, writer);
        }
    }
}
