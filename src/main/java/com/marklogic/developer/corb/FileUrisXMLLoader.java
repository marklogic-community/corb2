/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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

/**
 *
 * @author Praveen Venkata
 */
import static com.marklogic.developer.corb.Options.XML_FILE;
import static com.marklogic.developer.corb.Options.XML_NODE;
import com.marklogic.developer.corb.util.FileUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.stax.StAXSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Split an XML file {@value #XML_FILE} into multiple documents using the
 * XPath expression from the {@value #XML_NODE} property and 
 * sends the serialized XML string to the process module in the URIS parameter.
 * 
 * For extremely large XML files, consider the {@link com.marklogic.developer.corb.FileUrisStreamingXMLLoader}
 * 
 * @since 2.3.1
 */
public class FileUrisXMLLoader extends AbstractUrisLoader {

    private static final Logger LOG = Logger.getLogger(FileUrisXMLLoader.class.getName());
    protected static final String EXCEPTION_MSG_PROBLEM_READING_XML_FILE = "Problem while reading the XML file";
    protected String nextUri;
    protected Iterator<Node> nodeIterator;
    protected Document doc;

    private Map<Integer, Node> nodeMap;
    private TransformerFactory transformerFactory;
    private static final String YES = "yes";

    @Override
    public void open() throws CorbException {

        try {
            String fileName = getProperty(XML_FILE);
            String xpathRootNode = getProperty(XML_NODE);

            File xmlFile = new File(fileName);

            validate(xmlFile);

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(xmlFile);

            //Get Child nodes for parent node which is a wrapper node
            NodeList nodeList;

            if (xpathRootNode == null) {
                //default processing will select child elements
                nodeList = doc.getChildNodes().item(0).getChildNodes();
            } else {
                XPathFactory factory = XPathFactory.newInstance();
                //using this factory to create an XPath object:
                XPath xpath = factory.newXPath();

                // XPath Query for showing all nodes value
                XPathExpression expr = xpath.compile(xpathRootNode);
                Object result = expr.evaluate(doc, XPathConstants.NODESET);
                nodeList = (NodeList) result;
            }

            nodeMap = new ConcurrentHashMap<>(nodeList.getLength());

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (xpathRootNode == null && node.getNodeType() != Node.ELEMENT_NODE) {
                    continue; //default processing without an XPath selects only /*
                }
                nodeMap.put(i, node);
            }

            this.setTotalCount(nodeMap.size());
            nodeIterator = nodeMap.values().iterator();
            batchRef = xmlFile.getCanonicalPath(); //original XML file set for reference in processing modules

        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException exc) {
            throw new CorbException("Problem loading data from XML file ", exc);
        }
    }
    
    /**
     * Lazy-load a new instance of a TransformerFactory. Subsequent calls, 
     * re-use the existing TransformerFactory.
     * @return TransformerFactory
     */
    protected TransformerFactory getTransformerFactory() {
        //Creating a transformerFactory is expensive, only do it once
        if (transformerFactory == null) {
            transformerFactory = TransformerFactory.newInstance();
        }
        return transformerFactory;
    }
    /**
     * Instantiates a new Transformer object with output options to omit the XML declaration and indent enabled.
     * @return
     * @throws TransformerConfigurationException 
     */
    protected Transformer newTransformer() throws TransformerConfigurationException {
        Transformer transformer = getTransformerFactory().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, YES);
        transformer.setOutputProperty(OutputKeys.INDENT, YES);
        return transformer;
    }

    /**
     * Use an identity transform to serialize a Node to a string.
     * @param node
     * @return
     * @throws CorbException 
     */
    private String nodeToString(Node node) throws CorbException {
        StringWriter sw = new StringWriter();
        try {
            Transformer autobot = newTransformer();
            autobot.transform(new DOMSource(node), new StreamResult(sw));
        } catch (TransformerException te) {
            throw new CorbException("nodeToString Transformer Exception", te);
        }
        return sw.toString();
    }

    private String readNextNode() throws IOException, CorbException {
        if (nodeIterator.hasNext()) {
            Node nextNode = nodeIterator.next();
            short nextNodeType = nextNode.getNodeType();
            String line;
            if (nextNodeType == Node.ELEMENT_NODE || nextNodeType == Node.DOCUMENT_NODE) {
                line = trim(nodeToString(nextNode));
            } else {
                line = nextNode.getNodeValue();
            }
            if (isBlank(line)) {
                line = readNextNode();
            }
            return line;
        }
        return null;
    }

    @Override
    public boolean hasNext() throws CorbException {
        if (nextUri == null) {
            try {
                nextUri = readNextNode();
            } catch (IOException | CorbException exc) {
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, exc);
            }
        }
        return nextUri != null;
    }

    @Override
    public String next() throws CorbException {
        String node;
        if (nextUri != null) {
            node = nextUri;
            nextUri = null;
        } else {
            try {
                node = readNextNode();
            } catch (IOException | CorbException exc) {
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, exc);
            }
        }
        return node;
    }

    @Override
    public void close() {
        if (doc != null) {
            LOG.info("closing XML file reader");
            try {
                doc = null;
                if (nodeMap != null) {
                    nodeMap.clear();
                }
            } catch (Exception exc) {
                LOG.log(Level.SEVERE, "while closing XML file reader", exc);
            }
        }
        cleanup();
    }

    protected void validate(File xmlFile) throws CorbException {
        String schemaFilename = getProperty(Options.XML_SCHEMA);
        if (schemaFilename != null && !schemaFilename.isEmpty()) {
            SchemaFactory sf = SchemaFactory.newInstance(W3C_XML_SCHEMA_NS_URI);
            File schemaFile = FileUtils.getFile(schemaFilename);
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
            try (Reader fileReader = new FileReader(xmlFile)) {
                Source source = new StAXSource(xmlInputFactory.createXMLStreamReader(fileReader));
                Schema schema = sf.newSchema(schemaFile);
                Validator validator = schema.newValidator();
                try {
                    validator.validate(source);
                } catch (SAXException ex) {
                    LOG.log(Level.SEVERE, xmlFile.getCanonicalPath() + " is not schema valid", ex);
                    throw new CorbException(ex.getMessage());
                }
            } catch (IOException | SAXException | XMLStreamException ex) {
                LOG.log(Level.SEVERE, null, ex);
                throw new CorbException(ex.getMessage());
            }
        }
    }
}
