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

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.XdmItem;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.util.XmlUtils.schemaValidate;
import static java.util.logging.Level.INFO;

/**
 * Validates XML items against an XML Schema and writes validation errors to a batch file.
 * <p>
 * SchemaValidateBatchToFileTask extends {@link ExportBatchToFileTask} to provide XML Schema
 * validation capabilities. This task:
 * </p>
 * <ul>
 * <li>Retrieves XML items from the PROCESS-MODULE result sequence</li>
 * <li>Validates each item against the configured XML Schema</li>
 * <li>Collects validation errors (SAXParseExceptions)</li>
 * <li>Writes errors to a single batch file in XML format</li>
 * <li>Logs successful validations (no errors) without writing to the file</li>
 * </ul>
 * <p>
 * This task is useful for validating large collections of XML documents against a schema
 * and producing a consolidated error report. Only documents with validation errors are
 * written to the output file.
 * </p>
 * <p>
 * Configuration options:
 * </p>
 * <ul>
 * <li>{@link Options#XML_SCHEMA} - Path to the XML Schema (.xsd) file (required)</li>
 * <li>{@link Options#EXPORT_FILE_NAME} - Output file for validation errors (required)</li>
 * <li>{@link Options#EXPORT_FILE_DIR} - Directory for the output file (optional)</li>
 * </ul>
 * <p>
 * Output format:
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
 * Each {@code <document>} element represents a URI (or batch of URIs) that failed
 * validation. The {@code <error>} elements contain details about each validation
 * error including line/column numbers and error messages.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <pre>
 * PROCESS-MODULE=getXmlDocuments.xqy
 * PROCESS-TASK=com.marklogic.developer.corb.SchemaValidateBatchToFileTask
 * XML-SCHEMA=/path/to/schema.xsd
 * EXPORT-FILE-NAME=validation-errors.xml
 * </pre>
 * <p>
 * Thread safety: The {@link #writeSchemaValidationReport(List, Writer)} method is
 * synchronized to prevent interleaved writes from multiple threads.
 * </p>
 *
 * @author MarkLogic Corporation
 * @see ExportBatchToFileTask
 * @see com.marklogic.developer.corb.util.XmlUtils#schemaValidate(Source, File, java.util.Properties)
 * @see Options#XML_SCHEMA
 */
public class SchemaValidateBatchToFileTask extends ExportBatchToFileTask {

    private static final Object SYNC_OBJ = new Object();
    private static final Logger LOG = Logger.getLogger(SchemaValidateBatchToFileTask.class.getName());

    /**
     * Processes the result sequence by validating each item and writing errors to the export file.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Gets the configured XML Schema file</li>
     * <li>Gets the export file for writing validation errors</li>
     * <li>Iterates through all items in the result sequence</li>
     * <li>Validates each item against the schema</li>
     * <li>Writes any validation errors to the export file</li>
     * </ol>
     * <p>
     * All validation errors for the current batch are appended to the same export file.
     * Documents that validate successfully do not produce output (but are logged).
     * </p>
     *
     * @param seq the result sequence containing XML items to validate
     * @return "true" if processing completed successfully
     * @throws CorbException if validation or file writing fails
     */
    @Override
    protected String processResult(ResultSequence seq) throws CorbException {
        File schemaFile = getSchemaFile();
        File outputFile =  getExportFile();

        while (seq.hasNext()) {
            XdmItem item = seq.next().getItem();
            Source source = itemAsSource(item);
            try {
                validateAndWriteReport(source, schemaFile, outputFile);
            } catch (IOException | SAXException | XMLStreamException ex) {
                throw new CorbException("Unable to validate or write report for URIs: " + urisAsString(inputUris), ex);
            }
        }
        return TRUE;
    }

    /**
     * Validates an XML source against a schema and writes any errors to the output file.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Performs XML Schema validation using {@link com.marklogic.developer.corb.util.XmlUtils#schemaValidate}</li>
     * <li>Collects any SAXParseExceptions (validation errors)</li>
     * <li>Writes the errors to the output file in XML format</li>
     * </ol>
     * <p>
     * If validation succeeds (no errors), nothing is written to the output file,
     * but a success message is logged.
     * </p>
     *
     * @param source the XML source to validate
     * @param schemaFile the XML Schema file to validate against
     * @param outputFile the file to write validation errors to
     * @throws IOException if file I/O fails
     * @throws SAXException if XML parsing fails
     * @throws XMLStreamException if XML writing fails
     */
    protected void validateAndWriteReport(Source source, File schemaFile, File outputFile) throws IOException, SAXException, XMLStreamException {
        List<SAXParseException> exceptions = schemaValidate(source, schemaFile, properties);
        writeSchemaValidationReport(exceptions, outputFile);
    }

    /**
     * Writes schema validation errors to the output file.
     * <p>
     * This is a convenience method that opens a buffered writer for the output file
     * and delegates to {@link #writeSchemaValidationReport(List, Writer)}.
     * </p>
     * <p>
     * The file is opened in APPEND mode so multiple batches can write to the same file.
     * UTF-8 encoding is used for the output.
     * </p>
     *
     * @param exceptions list of validation errors
     * @param outputFile the file to write errors to
     * @throws IOException if file I/O fails
     * @throws XMLStreamException if XML writing fails
     */
    protected void writeSchemaValidationReport(List<SAXParseException> exceptions, File outputFile) throws IOException, XMLStreamException {
        try (Writer writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            writeSchemaValidationReport(exceptions, writer);
        }
    }

    /**
     * Serializes SAXParseExceptions to XML for the input URIs that have been validated.
     * <p>
     * The output format is:
     * </p>
     * <pre>{@code
     * <document uri="/path/to/doc.xml">
     *   <error lineNumber="10" columnNumber="5" ...>error message</error>
     *   ...
     * </document>
     * }</pre>
     * <p>
     * If the exceptions list is empty (validation succeeded), nothing is written and
     * a success message is logged.
     * </p>
     * <p>
     * This method is synchronized to prevent interleaved writes from multiple threads
     * when running with multiple workers.
     * </p>
     *
     * @param exceptions list of validation errors to serialize
     * @param writer the writer to output XML to
     * @throws XMLStreamException if XML writing fails
     */
    protected void writeSchemaValidationReport(List<SAXParseException> exceptions, Writer writer) throws XMLStreamException {
        if (exceptions.isEmpty()) {
            LOG.log(INFO, urisAsString(inputUris) + " is Schema valid, no errors to report");
            return;
        }

        XMLOutputFactory output = XMLOutputFactory.newInstance();
        synchronized (SYNC_OBJ) {
            XMLStreamWriter xmlWriter = output.createXMLStreamWriter(writer);
            xmlWriter.writeStartElement("document");
            xmlWriter.writeAttribute("uri", urisAsString(inputUris));
            for (SAXParseException exception : exceptions) {
                writeParseException(exception, xmlWriter);
            }
            xmlWriter.writeEndElement();
            xmlWriter.writeCharacters("\n");
            xmlWriter.close();
        }
    }

    /**
     * Serializes a single SAXParseException to XML.
     * <p>
     * The output format is:
     * </p>
     * <pre>{@code
     * <error lineNumber="10" columnNumber="5" systemId="/path/to/doc.xml" publicId="...">
     *   error message text
     * </error>
     * }</pre>
     * <p>
     * Attributes are only written if they have non-null values.
     * </p>
     *
     * @param exception the exception to serialize
     * @param xmlWriter the XML writer to output to
     * @throws XMLStreamException if XML writing fails
     */
    protected void writeParseException(SAXParseException exception, XMLStreamWriter xmlWriter) throws XMLStreamException {
        xmlWriter.writeStartElement("error");
        writeAttribute(xmlWriter, "publicId", exception.getPublicId());
        writeAttribute(xmlWriter, "systemId", exception.getSystemId());
        writeAttribute(xmlWriter, "lineNumber", Integer.toString(exception.getLineNumber()));
        writeAttribute(xmlWriter, "columnNumber", Integer.toString(exception.getColumnNumber()));
        xmlWriter.writeCharacters(exception.getMessage());
        xmlWriter.writeEndElement();
    }

    /**
     * Writes an XML attribute if both name and value are non-null.
     * <p>
     * This method prevents NullPointerExceptions when serializing exception attributes
     * that may not be set (e.g., publicId, systemId).
     * </p>
     *
     * @param writer the XML writer
     * @param name the attribute name
     * @param value the attribute value
     * @throws XMLStreamException if XML writing fails
     */
    private void writeAttribute(XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        if (name != null && value != null) {
            writer.writeAttribute(name, value);
        }
    }

    /**
     * Obtains the XML Schema file configured via {@link Options#XML_SCHEMA}.
     * <p>
     * The schema file path can be:
     * </p>
     * <ul>
     * <li>An absolute path</li>
     * <li>A relative path (from the current working directory)</li>
     * <li>A classpath resource</li>
     * </ul>
     *
     * @return the File object for the XML Schema
     */
    protected File getSchemaFile() {
        String schemaFilename = getProperty(Options.XML_SCHEMA);
        return FileUtils.getFile(schemaFilename);
    }

    /**
     * Converts an XdmItem to a Source for XML validation.
     * <p>
     * This method:
     * </p>
     * <ol>
     * <li>Extracts the item content as a byte array</li>
     * <li>Creates an InputStream from the bytes</li>
     * <li>Wraps the stream in a StreamSource</li>
     * <li>Sets the system ID to the URI being validated (for error reporting)</li>
     * </ol>
     * <p>
     * The system ID is important for schema validation as it allows the validator
     * to resolve relative schema locations and provides context in error messages.
     * </p>
     *
     * @param item the XdmItem to convert
     * @return a Source suitable for schema validation
     */
    protected Source itemAsSource(XdmItem item) {
        byte[] itemData = getValueAsBytes(item);
        InputStream inputStream = new ByteArrayInputStream(itemData);
        Source source = new StreamSource(inputStream);
        source.setSystemId(urisAsString(inputUris));
        return source;
    }

}
