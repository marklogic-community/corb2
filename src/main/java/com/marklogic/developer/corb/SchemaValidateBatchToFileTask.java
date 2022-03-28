/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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
 * Validate items in a ResultSequence and write any validation errors to a single file for the batch
 */
public class SchemaValidateBatchToFileTask extends ExportBatchToFileTask {

    private static final Object SYNC_OBJ = new Object();
    private static final Logger LOG = Logger.getLogger(SchemaValidateBatchToFileTask.class.getName());

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
     * Perform XML Schema validation and write any validation errors to the outputFile
     * @param source
     * @param schemaFile
     * @param outputFile
     * @throws IOException
     * @throws SAXException
     * @throws XMLStreamException
     */
    protected void validateAndWriteReport(Source source, File schemaFile, File outputFile) throws IOException, SAXException, XMLStreamException {
        List<SAXParseException> exceptions = schemaValidate(source, schemaFile, properties);
        writeSchemaValidationReport(exceptions, outputFile);
    }

    protected void writeSchemaValidationReport(List<SAXParseException> exceptions, File outputFile) throws IOException, XMLStreamException {
        try (Writer writer = Files.newBufferedWriter(outputFile.toPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            writeSchemaValidationReport(exceptions, writer);
        }
    }

    /**
     * Serialize any SAXParseExceptions to XML for the inputUris that have been validated.
     * @param exceptions
     * @param writer
     * @throws XMLStreamException
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
     * Serialize a SAXParseException to XML using the XMLStreamWriter provided
     * @param exception
     * @param xmlWriter
     * @throws XMLStreamException
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
     * Write an attribute, if the provided name and value are not null, avoiding potential Null Pointer Exceptions
     * @param writer
     * @param name
     * @param value
     * @throws XMLStreamException
     */
    private void writeAttribute(XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        if (name != null && value != null) {
            writer.writeAttribute(name, value);
        }
    }

    /**
     * Obtain a File for the configured {@value com.marklogic.developer.corb.Options#XML_SCHEMA}
     * @return
     */
    protected File getSchemaFile() {
        String schemaFilename = getProperty(Options.XML_SCHEMA);
        return FileUtils.getFile(schemaFilename);
    }

    /**
     * Produce a Source from XdmItem provided
     * @param item
     * @return
     */
    protected Source itemAsSource(XdmItem item) {
        byte[] itemData = getValueAsBytes(item);
        InputStream inputStream = new ByteArrayInputStream(itemData);
        Source source = new StreamSource(inputStream);
        source.setSystemId(urisAsString(inputUris));
        return source;
    }

}
