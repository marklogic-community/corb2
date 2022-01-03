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

import static com.marklogic.developer.corb.Options.METADATA;
import static com.marklogic.developer.corb.Options.METADATA_TO_PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.POST_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PRE_BATCH_MODULE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.XML_FILE;
import static com.marklogic.developer.corb.Options.XML_METADATA;
import static com.marklogic.developer.corb.Options.XML_NODE;
import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trim;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import com.marklogic.developer.corb.util.XmlUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Split an XML file {@value Options#XML_FILE} into multiple documents using the XPath
 * expression from the {@value Options#XML_NODE} property and send the serialized XML
 * string to the process module in the URIS parameter.
 *
 * For extremely large XML files, consider the
 * {@link com.marklogic.developer.corb.FileUrisStreamingXMLLoader}
 *
 * @author Praveen Venkata
 * @author Bhagat Bandlamudi
 * @since 2.3.1
 */
public class FileUrisXMLLoader extends AbstractFileUrisLoader {

    private static final Logger LOG = Logger.getLogger(FileUrisXMLLoader.class.getName());
    protected static final String EXCEPTION_MSG_PROBLEM_READING_XML_FILE = "Problem while reading the XML file";
    protected static final String EXCEPTION_MSG_PROBLEM_READING_XML_METADATA = "Problem while reading the XML metadata from file";
    protected String nextUri;
    protected Iterator<Node> nodeIterator;
    protected Document doc;
    protected File xmlFile;
    protected Node customMetadata;
    private Map<Integer, Node> nodeMap;

    @Override
    public void open() throws CorbException {
        String fileName = getLoaderPath(XML_FILE);
        xmlFile = FileUtils.getFile(fileName);
        schemaValidate(xmlFile);
        nodeIterator = readNodes(xmlFile.toPath());

        if (shouldSetBatchRef()) {
            try {
                batchRef = xmlFile.getCanonicalPath();
            } catch (IOException exc) {
                throw new CorbException("Problem loading data from XML file ", exc);
            }
        }

        setMetadataNodeToModule(customMetadata, xmlFile);
    }

    private Iterator<Node> readNodes(Path input) throws CorbException {
        String xpathRootNode = getProperty(XML_NODE);
        String xpathMetadataNode = getProperty(XML_METADATA);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        dbFactory.setNamespaceAware(true);
        try {
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(input.toFile());

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

                if (isNotEmpty(xpathMetadataNode)) {
                    XPathExpression metadataExpr = xpath.compile(xpathMetadataNode);
                    Object metadataResult = metadataExpr.evaluate(doc, XPathConstants.NODESET);
                    NodeList metadataNodeList = (NodeList) metadataResult;
                    if (metadataNodeList.getLength() > 0) {
                        customMetadata = metadataNodeList.item(0);
                    }
                }
            }

            nodeMap = new ConcurrentHashMap<>(nodeList.getLength());

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (xpathRootNode == null && node.getNodeType() != Node.ELEMENT_NODE) {
                    continue; //default processing without an XPath selects only /*
                }
                nodeMap.put(i, node);
            }

            setTotalCount(nodeMap.size());
        } catch (SAXException | IOException | XPathExpressionException | ParserConfigurationException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
        }
        return nodeMap.values().iterator();
    }

    private String readNextNode() throws CorbException {
        if (nodeIterator.hasNext()) {
            Node nextNode = nodeIterator.next();
            String line = nodeToString(nextNode);

            if (shouldUseEnvelope()) { //TODO: determine if default should be true(breaking change)
                Map<String, String> metadata;
                try {
                    metadata = getMetadata(xmlFile);
                    metadata.put(META_CONTENT_TYPE, "text/xml");
                    String xpath = getProperty(XML_NODE);
                    if (!isBlank(xpath)) {
                        metadata.put(XML_NODE, xpath);
                    }

                    Document loaderDoc;
                    if (shouldBase64Encode()) {
                        try (InputStream inputStream = XmlUtils.toInputStream(nextNode)) {
                            loaderDoc = toLoaderDoc(metadata, inputStream);
                        }
                    } else {
                        loaderDoc = toLoaderDoc(metadata, nextNode, false);
                    }
                    return XmlUtils.documentToString(loaderDoc);
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            } else {
                return line;
            }
        }
        return null;
    }

    private String nodeToString(Node node) throws CorbException {
        String line;
        short nextNodeType = node.getNodeType();
        if (nextNodeType == Node.ELEMENT_NODE || nextNodeType == Node.DOCUMENT_NODE) {
            //serialize the XML into a string
            line = trim(XmlUtils.nodeToString(node));
        } else {
            line = node.getNodeValue();
        }
        if (isBlank(line)) {
            line = readNextNode();
        }
        return line;
    }

    @Override
    protected boolean shouldBase64Encode() {
        String shouldEncode = getProperty(Options.LOADER_BASE64_ENCODE);
        return StringUtils.stringToBoolean(shouldEncode, false);
    }

    @Override
    public boolean hasNext() throws CorbException {
        if (nextUri == null) {
            nextUri = readNextNode();
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
            node = readNextNode();
        }
        return node;
    }

    @Override
    public void close() {
        super.close();
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
    }

    protected void schemaValidate(File xmlFile) throws CorbException {
        String schemaFilename = getProperty(Options.XML_SCHEMA);
        if (StringUtils.isNotEmpty(schemaFilename)) {
            File schemaFile = FileUtils.getFile(schemaFilename);
            List<SAXParseException> validationErrors = XmlUtils.schemaValidate(xmlFile, schemaFile, properties);
            if (!validationErrors.isEmpty()) {
                throw new CorbException("File is not schema valid", validationErrors.get(0) );
            }
        }
    }

    protected void setMetadataNodeToModule(Node metadataNode, File file) throws CorbException {
        if (metadataNode != null) {
            try {
                String content = parseContent(metadataNode, file);
                setMetadataProperties(content);
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, MessageFormat.format("IOException occurred processing {0}", (file != null ? file.getName() : "")), ex);
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_METADATA, ex);
            }
        }
    }

    private String parseContent(Node metadataNode, File file) throws IOException, CorbException {
        String content;
        if (shouldUseEnvelope()) {
            Map<String, String> metadataMap = (file != null) ? getMetadata(file) : new HashMap<>();
            Document document;
            if (shouldBase64Encode()) {
                content = XmlUtils.nodeToString(metadataNode);
                document = toLoaderDoc(metadataMap, new ByteArrayInputStream(content.getBytes()));
            } else {
                document = toLoaderDoc(metadataMap, metadataNode, false);
            }
            content = XmlUtils.documentToString(document);
        } else {
            content = XmlUtils.nodeToString(metadataNode);
        }
        return content;
    }

    private void setMetadataProperties(String content) {
        if (content != null) {
            properties.put(PRE_BATCH_MODULE + '.' + METADATA, content);
            properties.put(POST_BATCH_MODULE + '.' + METADATA, content);
            if (StringUtils.stringToBoolean(getProperty(METADATA_TO_PROCESS_MODULE), false)) {
                properties.put(PROCESS_MODULE + '.' + METADATA, content);
            }
        }
    }

}
