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
package com.marklogic.developer.corb.util;

import com.marklogic.developer.corb.CorbException;

import com.marklogic.developer.corb.Options;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.*;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;
/**
 * Utility class for XML operations.
 * Provides methods for XML document manipulation, serialization, validation,
 * and conversion. This class handles DOM operations, schema validation,
 * and XML-to-string conversions.
 *
 * @since 2.4.0
 */
public final class XmlUtils {

    private static final Logger LOG = Logger.getLogger(XmlUtils.class.getName());

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private XmlUtils() {
    }

    /**
     * Converts an XML Document to its string representation.
     * The output does not include an XML declaration.
     *
     * @param doc the XML document to convert
     * @return the string representation of the document
     */
    public static String documentToString(Document doc)    {
        return nodeToString(doc, doc);
    }

    /**
     * Converts an XML Node to its string representation.
     * If the node is a Document, delegates to {@link #documentToString(Document)}.
     * Otherwise, uses the node's owner document for serialization.
     *
     * @param node the XML node to convert
     * @return the string representation of the node
     */
    public static String nodeToString(Node node) {
        if (node instanceof Document) {
            return documentToString((Document) node);
        } else {
            return nodeToString(node.getOwnerDocument(), node);
        }
    }

    /**
     * Converts an XML Node to its string representation using a specified Document.
     * Uses DOM Level 3 Load and Save (LS) API for serialization.
     * The output is UTF-8 encoded and does not include an XML declaration.
     *
     * @param doc the document used for DOM implementation and serialization settings
     * @param node the XML node to convert
     * @return the string representation of the node
     */
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

    /**
     * Validates an XML file against an XML Schema Definition (XSD) file.
     * Uses StAX for parsing and collects all validation errors.
     *
     * @param xmlFile the XML file to validate
     * @param schemaFile the XSD schema file to validate against
     * @param options configuration properties for validation behavior
     * @return a list of SAXParseException objects representing validation errors (empty if valid)
     * @throws CorbException if an I/O or parsing error occurs during validation
     */
    public static List<SAXParseException> schemaValidate(File xmlFile, File schemaFile, Properties options) throws CorbException {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        xmlInputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        try (Reader fileReader = new InputStreamReader(Files.newInputStream(xmlFile.toPath()), StandardCharsets.UTF_8)) {
            Source source = new StAXSource(xmlInputFactory.createXMLStreamReader(fileReader));
            return schemaValidate(source, schemaFile, options);
        } catch (IOException | SAXException | XMLStreamException ex) {
            LOG.log(Level.SEVERE, "Unable to schema validate XML file", ex);
            throw new CorbException(ex.getMessage(), ex);
        }
    }

    /**
     * Validates an XML source against an XML Schema Definition (XSD) file.
     * Collects all validation errors (warnings, errors, and fatal errors) using a custom error handler.
     * Optionally honors all schema locations based on configuration.
     *
     * @param source the XML source to validate
     * @param schemaFile the XSD schema file to validate against
     * @param options configuration properties including XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS
     * @return a list of SAXParseException objects representing validation errors (empty if valid)
     * @throws SAXException if a schema parsing error occurs
     * @throws IOException if an I/O error occurs during validation
     */
    public static List<SAXParseException> schemaValidate(Source source, File schemaFile, Properties options) throws SAXException, IOException {
        SchemaFactory schemaFactory = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI);
        try {
            schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        } catch (SAXNotRecognizedException | SAXNotSupportedException e) {
            LOG.log(Level.WARNING, "Unable to set secure XML schema factory features", e);
        }
        boolean honourAllSchemaLocations = StringUtils.stringToBoolean(Options.findOption(options, Options.XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS), true);
        if (honourAllSchemaLocations){
            schemaFactory.setFeature("http://apache.org/xml/features/honour-all-schemaLocations", true);
        }
        Schema schema = schemaFactory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        try {
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            validator.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        } catch (SAXNotRecognizedException | SAXNotSupportedException e) {
            LOG.log(Level.WARNING, "Unable to set secure XML validator features", e);
        }
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

    /**
     * Converts an XML Node to an InputStream.
     * The node is first serialized to a string, then converted to bytes using the default charset.
     *
     * @param node the XML node to convert
     * @return an InputStream containing the serialized node data
     */
    public static InputStream toInputStream(Node node) {
        return new ByteArrayInputStream(nodeToString(node.getOwnerDocument(), node).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a new instance of DocumentBuilderFactory with secure features enabled.
     * The factory is configured to prevent XML External Entity (XXE) attacks by disabling
     * DTD processing and external entity resolution.
     *
     * @return a securely configured DocumentBuilderFactory instance
     */
    public static DocumentBuilderFactory newSecureDocumentBuilderFactoryInstance() {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        documentBuilderFactory.setXIncludeAware(false);
        documentBuilderFactory.setExpandEntityReferences(false);
        try {
            documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        } catch (ParserConfigurationException e) {
            LOG.log(Level.WARNING, "Unable to set secure XML parser features", e);
        }
        try {
            documentBuilderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Unable to set secure XML parser features", e);
        }
        try {
            documentBuilderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Unable to set secure XML parser features", e);
        }
        try {
            documentBuilderFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Unable to set secure XML parser features", e);
        }
        documentBuilderFactory.setXIncludeAware(false);
        documentBuilderFactory.setExpandEntityReferences(false);
        return documentBuilderFactory;
    }

    /**
     * Creates a new instance of TransformerFactory with secure features enabled.
     * The factory is configured to prevent XML External Entity (XXE) attacks by enabling
     * secure processing and restricting access to external DTDs and stylesheets.
     *
     * @return a securely configured TransformerFactory instance
     */
    public static TransformerFactory newSecureTransformerFactoryInstance() {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        try {
            transformerFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            transformerFactory.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_DTD, "");
            transformerFactory.setAttribute(javax.xml.XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "file,jar:zip");
        } catch (IllegalArgumentException | TransformerConfigurationException e) {
            LOG.log(Level.WARNING, "Unable to set secure XML transformer features", e);
        }
        return transformerFactory;
    }

    /**
     * Creates a new instance of XMLInputFactory with secure features enabled.
     * The factory is configured to prevent XML External Entity (XXE) attacks by disabling
     * DTD support and external entity resolution.
     *
     * @return a securely configured XMLInputFactory instance
     */
    public static XMLInputFactory newSecureXMLInputFactoryInstance() {
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try {
            xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
            xmlInputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        } catch (IllegalArgumentException e) {
            // Ignore if the underlying XMLInputFactory does not support these properties
        }
        return xmlInputFactory;
    }

    /**
     * Creates a new instance of XMLOutputFactory with secure features enabled.
     * The factory is configured to prevent XML External Entity (XXE) attacks by disabling
     * access to external DTDs and stylesheets.
     *
     * @return a securely configured XMLOutputFactory instance
     */
    public static XMLOutputFactory newSecureXMLOutputFactoruInstance() {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        try {
            xmlOutputFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            xmlOutputFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "file,jar:zip");
        } catch (IllegalArgumentException e) {
            LOG.log(Level.WARNING, "Unable to set secure XML output factory features", e);
        }
        return xmlOutputFactory;
    }

    /**
     * Creates a new instance of XPathFactory with secure features enabled.
     * The factory is configured to prevent XML External Entity (XXE) attacks by enabling
     * secure processing.
     *
     * @return a securely configured XPathFactory instance
     */
    public static XPathFactory newSecureXPathFactoryInstance() {
        XPathFactory factory = XPathFactory.newInstance();
        try {
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        } catch (XPathFactoryConfigurationException e) {
            LOG.warning("Failed to set secure processing feature on XPathFactory: " + e.getMessage());
        }
        return factory;
    }
}
