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
 * A DOM-based URI loader that splits XML files into multiple documents using XPath expressions.
 * <p>
 * This class extends {@link AbstractFileUrisLoader} and provides functionality to:
 * </p>
 * <ul>
 *   <li>Parse an entire XML file into a DOM tree in memory</li>
 *   <li>Extract nodes using configurable XPath expressions</li>
 *   <li>Optionally extract metadata from a separate XPath location</li>
 *   <li>Validate XML against an XSD schema before processing</li>
 *   <li>Generate loader document envelopes with metadata when configured</li>
 *   <li>Support base64 encoding of extracted content</li>
 * </ul>
 * <p>
 * <b>Processing Model:</b><br>
 * This loader uses a DOM (Document Object Model) parser that loads the entire XML file into memory:
 * <ol>
 *   <li>Parses the XML file into a DOM {@link Document}</li>
 *   <li>Evaluates the {@value Options#XML_NODE} XPath expression to select nodes</li>
 *   <li>Optionally evaluates {@value Options#XML_METADATA} XPath to extract metadata</li>
 *   <li>Stores matching nodes in a map for iteration</li>
 *   <li>Returns each node as a serialized XML string via the iterator interface</li>
 * </ol>
 * <p>
 * <b>XPath Configuration:</b><br>
 * The loader supports full XPath 1.0 expressions evaluated against the DOM:
 * </p>
 * <ul>
 *   <li>{@value Options#XML_NODE} - XPath for nodes to extract (default: child elements of root)</li>
 *   <li>{@value Options#XML_METADATA} - Optional XPath for a single metadata node</li>
 * </ul>
 * If no XPath is specified, defaults to extracting all child <b>elements</b> of the document root
 * (non-element nodes like text, comments are filtered out).
 * <p>
 * <b>Memory Considerations:</b><br>
 * <b>WARNING:</b> This loader loads the entire XML file into memory as a DOM tree, which can
 * consume significant memory for large files. For very large XML files (hundreds of MB or larger),
 * consider using {@link FileUrisStreamingXMLLoader} instead, which uses constant memory via streaming.
 * </p>
 * <p>
 * <b>Schema Validation:</b><br>
 * Optional XSD schema validation can be performed before processing by specifying
 * {@value Options#XML_SCHEMA}. If validation fails, a {@link CorbException} is thrown with
 * details of the first validation error.
 * </p>
 * <p>
 * <b>Metadata Handling:</b><br>
 * Extracted metadata is made available to batch modules:
 * </p>
 * <ul>
 *   <li>{@value Options#PRE_BATCH_MODULE} receives the metadata</li>
 *   <li>{@value Options#POST_BATCH_MODULE} receives the metadata</li>
 *   <li>{@value Options#PROCESS_MODULE} optionally receives metadata if
 *       {@value Options#METADATA_TO_PROCESS_MODULE} is enabled</li>
 * </ul>
 * <p>
 * <b>Configuration Properties:</b>
 * </p>
 * <ul>
 *   <li>{@value Options#XML_FILE} - Path to the XML file to process</li>
 *   <li>{@value Options#XML_NODE} - XPath expression for nodes to extract</li>
 *   <li>{@value Options#XML_METADATA} - Optional XPath for metadata node</li>
 *   <li>{@value Options#XML_SCHEMA} - Optional XSD file path for validation</li>
 *   <li>{@value Options#LOADER_BASE64_ENCODE} - Enable base64 encoding (default: false)</li>
 *   <li>{@value Options#METADATA_TO_PROCESS_MODULE} - Send metadata to process module (default: false)</li>
 * </ul>
 *
 * @author Praveen Venkata
 * @author Bhagat Bandlamudi
 * @since 2.3.1
 * @see FileUrisStreamingXMLLoader
 * @see AbstractFileUrisLoader
 */
public class FileUrisXMLLoader extends AbstractFileUrisLoader {

    /**
     * Logger instance for the FileUrisXMLLoader class.
     * Used to log informational messages, warnings, and errors during XML processing operations.
     */
    private static final Logger LOG = Logger.getLogger(FileUrisXMLLoader.class.getName());

    /**
     * Error message constant used when an exception occurs while reading or parsing the XML file.
     * This message is used to wrap underlying exceptions (SAXException, IOException, etc.)
     * that occur during DOM parsing or XPath evaluation.
     */
    protected static final String EXCEPTION_MSG_PROBLEM_READING_XML_FILE = "Problem while reading the XML file";

    /**
     * Error message constant used when an exception occurs while reading or processing
     * the metadata node extracted from the XML file.
     * This message is used to wrap exceptions that occur during metadata extraction
     * or serialization operations.
     */
    protected static final String EXCEPTION_MSG_PROBLEM_READING_XML_METADATA = "Problem while reading the XML metadata from file";

    /**
     * Cache for the next URI/node to be returned.
     * Enables the {@link #hasNext()} method to peek ahead without consuming the node,
     * supporting the Iterator contract.
     */
    protected String nextUri;

    /**
     * Iterator over the DOM nodes extracted from the XML file.
     * Each node represents one document to be processed.
     */
    protected Iterator<Node> nodeIterator;

    /**
     * The DOM document representation of the XML file.
     * Maintained in memory for the duration of processing.
     */
    protected Document doc;

    /**
     * The XML file being processed.
     */
    protected File xmlFile;

    /**
     * Optional custom metadata node extracted from the XML file.
     * Extracted using the {@value Options#XML_METADATA} XPath expression.
     */
    protected Node customMetadata;

    /**
     * Map storing the extracted nodes indexed by their position.
     * Uses {@link ConcurrentHashMap} for thread-safe access.
     */
    private Map<Integer, Node> nodeMap;

    /**
     * Opens and initializes the XML loader by parsing the file and extracting nodes.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     *   <li>Retrieves the XML file path from {@value Options#XML_FILE}</li>
     *   <li>Validates the XML file against an XSD schema (if configured)</li>
     *   <li>Parses the XML and extracts nodes using {@link #readNodes(Path)}</li>
     *   <li>Sets the batch reference to the canonical XML file path (if enabled)</li>
     *   <li>Makes metadata available to batch modules (if extracted)</li>
     * </ol>
     *
     * @throws CorbException if the XML file cannot be read, parsed, validated, or if
     *         an error occurs extracting nodes or setting metadata
     */
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

    /**
     * Parses the XML file, extracts nodes using XPath, and optionally extracts metadata.
     * <p>
     * This method performs the core DOM parsing and node extraction:
     * </p>
     * <ol>
     *   <li>Creates a namespace-aware {@link DocumentBuilder}</li>
     *   <li>Parses the XML file into a DOM {@link Document}</li>
     *   <li>If {@value Options#XML_NODE} XPath is specified:
     *       <ul>
     *         <li>Compiles and evaluates the XPath expression</li>
     *         <li>Extracts all matching nodes</li>
     *       </ul>
     *   </li>
     *   <li>If no XPath is specified:
     *       <ul>
     *         <li>Defaults to child nodes of the document root</li>
     *         <li>Filters to include only {@link Node#ELEMENT_NODE} types</li>
     *       </ul>
     *   </li>
     *   <li>If {@value Options#XML_METADATA} XPath is specified:
     *       <ul>
     *         <li>Evaluates the metadata XPath expression</li>
     *         <li>Stores the first matching node as {@link #customMetadata}</li>
     *       </ul>
     *   </li>
     *   <li>Stores all extracted nodes in {@link #nodeMap} indexed by position</li>
     *   <li>Sets the total count for progress tracking</li>
     * </ol>
     *
     * @param input the path to the XML file to parse
     * @return an iterator over the extracted DOM nodes
     * @throws CorbException if an error occurs during parsing, XPath evaluation, or node extraction
     */
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

    /**
     * Reads and serializes the next node from the iterator, optionally wrapping it in a loader envelope.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Checks if more nodes are available</li>
     *   <li>Retrieves the next node from the iterator</li>
     *   <li>Converts the node to a string representation</li>
     *   <li>If envelope mode is enabled:
     *       <ul>
     *         <li>Generates metadata including content type and XPath</li>
     *         <li>If base64 encoding is enabled, encodes the node content</li>
     *         <li>Wraps the node in a loader document with metadata</li>
     *         <li>Serializes the loader document to XML string</li>
     *       </ul>
     *   </li>
     *   <li>If envelope mode is disabled, returns the node as a plain XML string</li>
     * </ol>
     *
     * @return the next node as an XML string, optionally wrapped in a loader envelope;
     *         or {@code null} if no more nodes are available
     * @throws CorbException if an error occurs serializing the node
     */
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

    /**
     * Converts a DOM node to its string representation.
     * <p>
     * This method handles different node types:
     * </p>
     * <ul>
     *   <li>For {@link Node#ELEMENT_NODE} or {@link Node#DOCUMENT_NODE}: serializes the
     *       entire node and its children to an XML string</li>
     *   <li>For other node types (text, attribute, etc.): returns the node's text value</li>
     *   <li>If the result is blank, recursively reads the next node</li>
     * </ul>
     *
     * @param node the DOM node to convert to a string
     * @return the string representation of the node, trimmed of whitespace
     * @throws CorbException if an error occurs during serialization or recursive reading
     */
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

    /**
     * Determines whether extracted content should be base64 encoded.
     * <p>
     * This method checks the {@value Options#LOADER_BASE64_ENCODE} property.
     * Base64 encoding is useful for binary content or when the extracted XML might
     * contain characters that could cause issues in the loader envelope.
     * </p>
     *
     * @return {@code true} if base64 encoding is enabled; {@code false} otherwise (default)
     */
    @Override
    protected boolean shouldBase64Encode() {
        String shouldEncode = getProperty(Options.LOADER_BASE64_ENCODE);
        return StringUtils.stringToBoolean(shouldEncode, false);
    }

    /**
     * Checks whether more XML nodes are available for processing.
     * <p>
     * This method implements a look-ahead mechanism:
     * </p>
     * <ul>
     *   <li>If {@link #nextUri} is null, reads and caches the next node</li>
     *   <li>Returns {@code true} if a node was successfully read and cached</li>
     *   <li>Returns {@code false} if no more nodes are available</li>
     * </ul>
     * <p>
     * The look-ahead ensures {@code hasNext()} can be called multiple times without
     * advancing the iterator position.
     * </p>
     *
     * @return {@code true} if more nodes are available; {@code false} otherwise
     * @throws CorbException if an error occurs reading the next node
     */
    @Override
    public boolean hasNext() throws CorbException {
        if (nextUri == null) {
            nextUri = readNextNode();
        }
        return nextUri != null;
    }

    /**
     * Returns the next XML node as a serialized string.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Returns the cached {@link #nextUri} if available, clearing the cache</li>
     *   <li>Otherwise, reads the next node directly</li>
     * </ol>
     * <p>
     * The returned string may be plain XML or wrapped in a loader envelope depending
     * on configuration.
     * </p>
     *
     * @return the next node as an XML string, or {@code null} if no more nodes are available
     * @throws CorbException if an error occurs reading or serializing the node
     */
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

    /**
     * Closes the loader and releases the DOM document and node map from memory.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Calls the parent class {@link #close()} method</li>
     *   <li>Nulls out the {@link #doc} reference to allow garbage collection</li>
     *   <li>Clears the {@link #nodeMap} to release node references</li>
     * </ol>
     * <p>
     * Releasing these references is important for memory management, especially when
     * processing large XML files, as the DOM tree can consume significant memory.
     * </p>
     */
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

    /**
     * Validates the XML file against an XSD schema if one is configured.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Checks if {@value Options#XML_SCHEMA} property is specified</li>
     *   <li>If specified, loads the XSD schema file</li>
     *   <li>Validates the XML file against the schema</li>
     *   <li>If validation errors are found, throws {@link CorbException} with the first error</li>
     * </ol>
     * <p>
     * Schema validation occurs before any nodes are extracted, ensuring that only
     * schema-valid XML is processed.
     * </p>
     *
     * @param xmlFile the XML file to validate
     * @throws CorbException if the XML file is not schema-valid or if an error occurs during validation
     */
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

    /**
     * Processes the extracted metadata node and makes it available to batch modules.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Checks if a metadata node was extracted</li>
     *   <li>If present, parses the content (applying envelope/encoding if configured)</li>
     *   <li>Sets the metadata in properties for access by batch modules</li>
     * </ol>
     * <p>
     * The metadata is made available to:
     * </p>
     * <ul>
     *   <li>{@value Options#PRE_BATCH_MODULE} - always receives metadata</li>
     *   <li>{@value Options#POST_BATCH_MODULE} - always receives metadata</li>
     *   <li>{@value Options#PROCESS_MODULE} - receives metadata if
     *       {@value Options#METADATA_TO_PROCESS_MODULE} is enabled</li>
     * </ul>
     *
     * @param metadataNode the DOM node containing metadata; may be null
     * @param file the source XML file (used for generating metadata)
     * @throws CorbException if an error occurs parsing or setting the metadata
     */
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

    /**
     * Parses a metadata node into a string representation, optionally wrapped in a loader envelope.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>If envelope mode is enabled:
     *       <ul>
     *         <li>Generates metadata map from the source file (if available)</li>
     *         <li>If base64 encoding is enabled, serializes node and encodes it</li>
     *         <li>Otherwise, wraps the node directly in a loader document</li>
     *         <li>Serializes the loader document to XML string</li>
     *       </ul>
     *   </li>
     *   <li>If envelope mode is disabled, serializes the node directly to XML string</li>
     * </ol>
     *
     * @param metadataNode the DOM node containing metadata
     * @param file the source XML file used for generating metadata; may be null
     * @return the serialized metadata content as an XML string
     * @throws IOException if an I/O error occurs during processing
     * @throws CorbException if an error occurs creating the loader document
     */
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

    /**
     * Sets the metadata content in properties for access by batch and process modules.
     * <p>
     * The metadata is stored with the following keys:
     * </p>
     * <ul>
     *   <li>{@code PRE_BATCH_MODULE.METADATA} - Available to the pre-batch module</li>
     *   <li>{@code POST_BATCH_MODULE.METADATA} - Available to the post-batch module</li>
     *   <li>{@code PROCESS_MODULE.METADATA} - Available to the process module only if
     *       {@value Options#METADATA_TO_PROCESS_MODULE} is {@code true}</li>
     * </ul>
     *
     * @param content the serialized metadata content; may be null
     */
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
