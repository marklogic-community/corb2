/*
 * * Copyright (c) 2004-2022 MarkLogic Corporation
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
package com.marklogic.developer.corb.util;

import com.marklogic.developer.corb.CorbException;

import com.marklogic.developer.corb.Options;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;
/**
 * @since 2.4.0
 */
public final class XmlUtils {

    private static final Logger LOG = Logger.getLogger(XmlUtils.class.getName());

    private XmlUtils() {
    }

    public static String documentToString(Document doc)    {
        return nodeToString(doc, doc);
    }

    public static String nodeToString(Node node) {
        if (node instanceof Document) {
            return documentToString((Document) node);
        } else {
            return nodeToString(node.getOwnerDocument(), node);
        }
    }

    public static String nodeToString(Document doc, Node node) {
        DOMImplementationLS domImplementation = (DOMImplementationLS) doc.getImplementation();
        LSSerializer lsSerializer = domImplementation.createLSSerializer();
        lsSerializer.getDomConfig().setParameter("xml-declaration", false);
        LSOutput lsOutput =  domImplementation.createLSOutput();
        lsOutput.setEncoding(StandardCharsets.UTF_8.name());

        Writer stringWriter = new StringWriter();
        lsOutput.setCharacterStream(stringWriter);
        lsSerializer.write(node, lsOutput);
        return stringWriter.toString();
    }

    public static List<SAXParseException> schemaValidate(File xmlFile, File schemaFile, Properties options) throws CorbException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try (Reader fileReader = new FileReader(xmlFile)) {
            Source source = new StAXSource(xmlInputFactory.createXMLStreamReader(fileReader));
            return schemaValidate(source, schemaFile, options);
        } catch (IOException | SAXException | XMLStreamException ex) {
            LOG.log(Level.SEVERE, "Unable to schema validate XML file", ex);
            throw new CorbException(ex.getMessage(), ex);
        }
    }

    public static List<SAXParseException> schemaValidate(Source source, File schemaFile, Properties options) throws SAXException, IOException {
        SchemaFactory schemaFactory = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI);
        boolean honourAllSchemaLocations = StringUtils.stringToBoolean(Options.findOption(options, Options.XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS), true);
        if (honourAllSchemaLocations){
            schemaFactory.setFeature("http://apache.org/xml/features/honour-all-schemaLocations", true);
        }
        Schema schema = schemaFactory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        final List<SAXParseException> exceptions = new LinkedList<>();
        //collect all validation errors with a custom handler
        validator.setErrorHandler(new ErrorHandler() {
            @Override
            public void warning(SAXParseException exception) {
                exceptions.add(exception);
            }

            @Override
            public void fatalError(SAXParseException exception) {
                exceptions.add(exception);
            }

            @Override
            public void error(SAXParseException exception) {
                exceptions.add(exception);
            }
        });
        validator.validate(source);
        return exceptions;
    }

    public static InputStream toInputStream(Node node) {
        return new ByteArrayInputStream(nodeToString(node.getOwnerDocument(), node).getBytes());
    }
}
