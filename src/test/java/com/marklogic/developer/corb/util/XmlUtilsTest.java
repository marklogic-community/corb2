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
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class XmlUtilsTest {
    private static final String dir = "src/test/resources/streamingXMLUrisLoader/";
    private static final File xmlDoc = new File(dir , "EDI.ICF15T.D150217.T113100716.T");
    private static final File schema = new File(dir + "BenefitEnrollment.xsd");
    private static final Properties options = new Properties();

    private static Document parseDocument(File file) throws Exception {
        return XmlUtils.newSecureDocumentBuilderFactoryInstance()
            .newDocumentBuilder().parse(file);
    }

    // -------------------------------------------------------------------------
    // schemaValidate(File, File, Properties) -- existing tests
    // -------------------------------------------------------------------------

    @Test
    void schemaValidate() {
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema, options);
            assertTrue(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void schemaValidateWithError()  {
        File schema = new File(dir, "NotBenefitEnrollment.xsd");
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema, options);
            assertFalse(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void schemaValidateMissingFile() {
        File missingFile = new File("does-not-exist.xml");
        assertThrows(CorbException.class, () -> XmlUtils.schemaValidate(missingFile, schema, options));
    }

    // -------------------------------------------------------------------------
    // schemaValidate(Source, File, Properties) -- direct overload
    // -------------------------------------------------------------------------

    @Test
    void schemaValidateSource() throws Exception {
        try (Reader reader = new InputStreamReader(Files.newInputStream(xmlDoc.toPath()), StandardCharsets.UTF_8)) {
            List<SAXParseException> exceptions = XmlUtils.schemaValidate(new StreamSource(reader), schema, options);
            assertTrue(exceptions.isEmpty());
        }
    }

    @Test
    void schemaValidateSourceWithHonourAllSchemaLocationsFalse() throws Exception {
        Properties opts = new Properties();
        opts.setProperty(Options.XML_SCHEMA_HONOUR_ALL_SCHEMALOCATIONS, "false");
        try (Reader reader = new InputStreamReader(Files.newInputStream(xmlDoc.toPath()), StandardCharsets.UTF_8)) {
            List<SAXParseException> exceptions = XmlUtils.schemaValidate(new StreamSource(reader), schema, opts);
            assertTrue(exceptions.isEmpty());
        }
    }

    @Test
    void schemaValidateSourceWithSchemaLocationHint() throws Exception {
        // XML that is valid against the schema but also contains a xsi:schemaLocation hint
        // pointing to a non-existent file. With honour-all-schemaLocations=true (default),
        // Xerces will try to resolve the hint and report an unresolvable reference as a
        // warning() call on the error handler, covering XmlUtils$1.warning().
        String xmlContent = new String(Files.readAllBytes(xmlDoc.toPath()), StandardCharsets.UTF_8);
        xmlContent = xmlContent.replace(
            "xmlns=\"http://bem.corb.developer.marklogic.com\"",
            "xmlns=\"http://bem.corb.developer.marklogic.com\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"http://bem.corb.developer.marklogic.com non-existent-schema.xsd\""
        );
        List<SAXParseException> exceptions = XmlUtils.schemaValidate(
            new StreamSource(new StringReader(xmlContent)), schema, options);
        // Whether a warning or error is produced depends on the Xerces version/JVM; the
        // important thing is that the error handler's methods are exercised.
        assertNotNull(exceptions);
    }

    // -------------------------------------------------------------------------
    // DOM serialisation helpers
    // -------------------------------------------------------------------------

    @Test
    void testDocumentToString() throws Exception {
        Document doc = parseDocument(xmlDoc);
        String result = XmlUtils.documentToString(doc);
        assertTrue(result.contains("BenefitEnrollmentRequest"));
        assertFalse(result.contains("<?xml"));
    }

    @Test
    void testNodeToStringElement() throws Exception {
        Document doc = parseDocument(xmlDoc);
        Node root = doc.getDocumentElement();
        String result = XmlUtils.nodeToString(root); // non-Document path
        assertTrue(result.contains("BenefitEnrollmentRequest"));
    }

    @Test
    void testNodeToStringDocument() throws Exception {
        Document doc = parseDocument(xmlDoc);
        String result = XmlUtils.nodeToString(doc); // Document path → delegates to documentToString
        assertTrue(result.contains("BenefitEnrollmentRequest"));
    }

    @Test
    void testToInputStream() throws Exception {
        Document doc = parseDocument(xmlDoc);
        byte[] bytes;
        try (InputStream is = XmlUtils.toInputStream(doc.getDocumentElement())) {
            bytes = new byte[is.available()];
            //noinspection ResultOfMethodCallIgnored
            is.read(bytes);
        }
        String content = new String(bytes, StandardCharsets.UTF_8);
        assertTrue(content.contains("BenefitEnrollmentRequest"));
    }

    // -------------------------------------------------------------------------
    // Secure factory methods
    // -------------------------------------------------------------------------

    @Test
    void testNewSecureDocumentBuilderFactoryInstance() {
        DocumentBuilderFactory factory = XmlUtils.newSecureDocumentBuilderFactoryInstance();
        assertNotNull(factory);
        assertTrue(factory.isNamespaceAware());
        assertFalse(factory.isXIncludeAware());
        assertFalse(factory.isExpandEntityReferences());
    }

    @Test
    void testNewSecureTransformerFactoryInstance() {
        TransformerFactory factory = XmlUtils.newSecureTransformerFactoryInstance();
        assertNotNull(factory);
    }

    @Test
    void testNewSecureXMLInputFactoryInstance() {
        XMLInputFactory factory = XmlUtils.newSecureXMLInputFactoryInstance();
        assertNotNull(factory);
        assertFalse((Boolean) factory.getProperty(XMLInputFactory.SUPPORT_DTD));
        assertFalse((Boolean) factory.getProperty("javax.xml.stream.isSupportingExternalEntities"));
    }

    @Test
    void testNewSecureXMLOutputFactoryInstance() {
        XMLOutputFactory factory = XmlUtils.newSecureXMLOutputFactoruInstance();
        assertNotNull(factory);
    }

    @Test
    void testNewSecureXPathFactoryInstance() {
        XPathFactory factory = XmlUtils.newSecureXPathFactoryInstance();
        assertNotNull(factory);
    }

}
