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

import static com.marklogic.developer.corb.Options.XML_FILE;
import static com.marklogic.developer.corb.Options.XML_METADATA;
import static com.marklogic.developer.corb.Options.XML_NODE;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.security.InvalidParameterException;
import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;

import com.marklogic.developer.corb.util.XmlUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * A memory-efficient URI loader that splits large XML files into multiple documents using StAX streaming.
 * <p>
 * This class extends {@link FileUrisXMLLoader} and provides optimized functionality for processing
 * very large XML files that cannot be efficiently loaded into memory as a DOM tree:
 * </p>
 * <ul>
 *   <li>Uses StAX (Streaming API for XML) parser to minimize memory consumption</li>
 *   <li>Splits XML based on configurable XPath expressions via {@link StreamingXPath}</li>
 *   <li>Extracts matching elements to temporary files for individual processing</li>
 *   <li>Supports optional metadata extraction from a separate XPath location</li>
 *   <li>Optionally validates XML against an XSD schema before processing</li>
 *   <li>Generates loader document envelopes with metadata when configured</li>
 * </ul>
 * <p>
 * <b>Processing Model:</b><br>
 * The loader performs a single-pass streaming parse of the XML file:
 * </p>
 * <ol>
 *   <li>Parses the XML file using a StAX stream reader</li>
 *   <li>Maintains a context stack to track the current XPath location</li>
 *   <li>When an element matches the {@value Options#XML_NODE} XPath expression,
 *       extracts and saves it as a temporary file</li>
 *   <li>If {@value Options#XML_METADATA} is specified, extracts the first matching element
 *       as metadata to be included with all documents</li>
 *   <li>Returns each extracted file for processing via the iterator interface</li>
 * </ol>
 * <p>
 * <b>XPath Configuration:</b><br>
 * The XPath expressions are "streaming" XPaths that match against the current path
 * in the XML hierarchy:
 * </p>
 * <ul>
 *   <li>{@value Options#XML_NODE} - XPath for elements to extract (default: &#47;*&#47;* - children of root)</li>
 *   <li>{@value Options#XML_METADATA} - Optional XPath for a single metadata element to include</li>
 * </ul>
 * Note: These are simplified, namespace-insensitive XPath expressions evaluated during streaming.

 * <p>
 * <b>Temporary File Management:</b><br>
 * Extracted XML elements are written to temporary files:
 * </p>
 * <ul>
 *   <li>Files are created in a temporary directory (see {@value Options#XML_TEMP_DIR} or {@value Options#TEMP_DIR})</li>
 *   <li>Each file is named using the element's local name plus a unique suffix</li>
 *   <li>Files are automatically deleted after being read via {@link #next()}</li>
 *   <li>The temporary directory is deleted during {@link #cleanup()}</li>
 * </ul>
 * <p>
 * <b>Performance Characteristics:</b><br>
 * This loader is designed for very large XML files:
 * </p>
 * <ul>
 *   <li><b>Memory:</b> Uses constant memory regardless of file size (streaming)</li>
 *   <li><b>Processing:</b> Single-pass parsing, but creates temporary disk files</li>
 *   <li><b>Disk:</b> Requires disk space equal to the extracted content size</li>
 * </ul>
 * <p>
 * <b>Configuration Properties:</b>
 * </p>
 * <ul>
 *   <li>{@value Options#XML_FILE} - Path to the large XML file to process</li>
 *   <li>{@value Options#XML_NODE} - XPath expression for elements to extract</li>
 *   <li>{@value Options#XML_METADATA} - Optional XPath for metadata element</li>
 *   <li>{@value Options#XML_SCHEMA} - Optional XSD for validation</li>
 *   <li>{@value Options#XML_TEMP_DIR} or {@value Options#TEMP_DIR} - Temporary directory location</li>
 * </ul>
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @author Bhagat Bandlamudi
 * @since 2.4.0
 * @see FileUrisXMLLoader
 * @see StreamingXPath
 */
public class FileUrisStreamingXMLLoader extends FileUrisXMLLoader {

    /**
     * Logger instance for the FileUrisStreamingXMLLoader class.
     * Used to log informational messages, warnings, and errors during streaming XML
     * processing operations, including extraction errors and temporary directory usage.
     */
    protected static final Logger LOG = Logger.getLogger(FileUrisStreamingXMLLoader.class.getName());

    /**
     * String constant representing the value "yes" for XML transformer output properties.
     * Used to set properties like {@link OutputKeys#OMIT_XML_DECLARATION} and
     * {@link OutputKeys#INDENT} when configuring transformers for XML extraction.
     */
    private static final String YES = "yes";

    /**
     * String constant representing the forward slash "/" character.
     * Used as a delimiter when constructing namespace-insensitive XPath expressions
     * from the element context stack during streaming XML parsing.
     */
    private static final String SLASH = "/";

    /**
     * Temporary directory where extracted XML elements are written as individual files.
     * Created during {@link #open()} and deleted during {@link #cleanup()}.
     */
    private Path tempDir;

    /**
     * Stream for iterating over the temporary files created during XML extraction.
     * Must be closed to release resources.
     */
    private DirectoryStream<Path> directoryStream;

    /**
     * Iterator over the temporary files containing extracted XML elements.
     * Each file represents one document to be processed.
     */
    private Iterator<Path> files;

    /**
     * Empty array of file attributes used when creating temporary files and directories.
     * No special attributes are set on temporary files.
     */
    private final FileAttribute<?>[] fileAttributes = new FileAttribute<?>[0];

    /**
     * StreamingXPath matcher for identifying XML elements to extract.
     * Configured from the {@value Options#XML_NODE} property.
     */
    private StreamingXPath streamingXPath;

    /**
     * Optional StreamingXPath matcher for identifying the metadata element.
     * Configured from the {@value Options#XML_METADATA} property.
     * May be null if no metadata extraction is configured.
     */
    private StreamingXPath streamingMetaXPath;

    /**
     * Cached TransformerFactory instance for creating XML transformers.
     * Lazily initialized and reused to avoid expensive repeated instantiation.
     */
    private TransformerFactory transformerFactory;

    /**
     * Opens and initializes the streaming XML loader by parsing configuration and validating the XML file.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     *   <li>Retrieves the XML file path from {@value Options#XML_FILE}</li>
     *   <li>Parses the {@value Options#XML_NODE} XPath expression (default: "&#47;*&#47;*")</li>
     *   <li>Parses the optional {@value Options#XML_METADATA} XPath expression</li>
     *   <li>Validates the XML file against an XSD if {@value Options#XML_SCHEMA} is configured</li>
     *   <li>Sets the batch reference to the canonical XML file path (if enabled)</li>
     *   <li>Creates a temporary directory for extracted elements</li>
     *   <li>Performs the streaming parse to extract all matching elements to temp files</li>
     *   <li>Sets metadata for inclusion in process modules</li>
     * </ol>
     * <p>
     * <b>Default XPath:</b> If no XML_NODE is specified, defaults to &#47;*&#47;* which extracts
     * all child elements of the document root element.
     * </p>
     *
     * @throws CorbException if the XML file cannot be read, parsed, validated, or if
     *         temporary directory creation fails
     */
    @Override
    public void open() throws CorbException {
        String xmlFilename = getLoaderPath(XML_FILE);
        String xPath = getProperty(XML_NODE);
        String metaXPath = getProperty(XML_METADATA);
        // default processing will split on child elements of the document element
        xPath = StringUtils.isBlank(xPath) ? "/*/*" : xPath;
        streamingXPath = new StreamingXPath(xPath);
        streamingMetaXPath = isNotEmpty(metaXPath) ? new StreamingXPath(metaXPath): null;
        xmlFile = FileUtils.getFile(xmlFilename);
        schemaValidate(xmlFile);
        try {
            if (shouldSetBatchRef()) {
                //set the original XML filename, for reference in processing modules
                batchRef = xmlFile.getCanonicalPath();
            }
            tempDir = getTempDir();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, MessageFormat.format("IOException occurred processing {0}", xmlFilename), ex);
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
        }
        LOG.log(Level.INFO, MessageFormat.format("Using the temp directory {0}", tempDir));
        //extract all the child nodes to a temp directory and load the metadata along with it.
        files = readToTempDir(xmlFile.toPath());

        setMetadataNodeToModule(customMetadata, xmlFile);
    }

    /**
     * Checks whether more extracted XML documents are available for processing.
     *
     * @return {@code true} if more temporary files with extracted elements exist;
     *         {@code false} if all files have been processed
     * @throws CorbException if an error occurs checking file availability
     */
    @Override
    public boolean hasNext() throws CorbException {
        return files.hasNext();
    }

    /**
     * Returns the next extracted XML document as a string, optionally wrapped in a loader envelope.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Retrieves the next temporary file from the iterator</li>
     *   <li>If envelope mode is enabled:
     *       <ul>
     *         <li>Generates metadata including the source file path</li>
     *         <li>If base64 encoding is enabled, reads the file as binary and encodes it</li>
     *         <li>Otherwise, parses the XML and wraps it in a loader document</li>
     *         <li>Serializes the loader document to an XML string</li>
     *       </ul>
     *   </li>
     *   <li>If envelope mode is disabled, reads the file content as UTF-8 text</li>
     *   <li>Deletes the temporary file after reading</li>
     * </ol>
     *
     * @return the extracted XML document as a string, optionally wrapped in a loader envelope
     * @throws CorbException if an error occurs reading, parsing, or deleting the file
     */
    @Override
    public String next() throws CorbException {
        Path path = files.next();
        String content;
        try {
            if (shouldUseEnvelope()) {
                File file = path.toFile();
                Map<String, String> metadata = getMetadata(xmlFile);
                metadata.put(META_SOURCE, file.getCanonicalPath());
                Document document;
                if (shouldBase64Encode()) {
                    try (InputStream inputStream = new FileInputStream(file)) {
                        document = toLoaderDoc(metadata, inputStream);
                    }
                } else {
                    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                    Document originalDocument = docBuilder.parse(file);
                    document = toLoaderDoc(metadata, originalDocument.getDocumentElement(), false);
                }
                content = XmlUtils.documentToString(document);
            } else {
                content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            }
            Files.deleteIfExists(path);
        } catch (ParserConfigurationException | SAXException | IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
        }
        return content;
    }

    /**
     * Performs cleanup by deleting the temporary directory and all remaining temporary files.
     * <p>
     * This method calls the parent cleanup and then quietly deletes the temporary directory
     * and its contents. Deletion failures are logged but do not throw exceptions.
     * </p>
     */
    @Override
    public void cleanup() {
        super.cleanup();
        FileUtils.deleteQuietly(tempDir);
    }

    /**
     * Closes the loader and releases all associated resources.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Calls the parent class {@link #close()} method</li>
     *   <li>Closes the directory stream used for iterating temporary files</li>
     *   <li>Calls {@link #cleanup()} to delete the temporary directory</li>
     * </ol>
     */
    @Override
    public void close() {
        super.close();
        IOUtils.closeQuietly(directoryStream);
        cleanup();
    }

    /**
     * Performs a streaming parse of the large XML file, extracting matching elements to temporary files.
     * <p>
     * This method uses a StAX (Streaming API for XML) parser to process the XML file with
     * minimal memory consumption:
     * <ol>
     *   <li>Opens the XML file with a buffered reader</li>
     *   <li>Creates a StAX stream reader for parsing</li>
     *   <li>Maintains a context stack (Deque) tracking the current XPath location</li>
     *   <li>For each start element, checks if it matches extraction criteria</li>
     *   <li>Extracts matching elements to temporary files via {@link #extractElement(XMLStreamReader, Deque)}</li>
     *   <li>Updates end element context by popping from the context stack</li>
     *   <li>Opens a directory stream for iterating the extracted files</li>
     *   <li>Sets the total document count for progress tracking</li>
     * </ol>
     * </p>
     * <p>
     * <b>Return Codes from extractElement:</b>
     * <ul>
     *   <li>0 - No extraction performed, continue parsing</li>
     *   <li>1 - XML node extracted successfully (counts toward total)</li>
     *   <li>2 - Metadata extracted (does not count toward total)</li>
     * </ul>
     * </p>
     *
     * @param xmlFile the path to the XML file to parse and split
     * @return an iterator over the temporary files containing extracted elements
     * @throws CorbException if an XML parsing or I/O error occurs
     */
    private Iterator<Path> readToTempDir(Path xmlFile) throws CorbException {
        long extractedDocumentCount = 0;

        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try (Reader fileReader = Files.newBufferedReader(xmlFile)) {
            XMLStreamReader reader = xmlInputFactory.createXMLStreamReader(fileReader);

            Deque<String> context = new ArrayDeque<>();
            while (reader.hasNext()) {
                // if there is a problem extracting an element, don't count it
                if (reader.isStartElement()) {
                    int code = extractElement(reader, context);
                    // code=2 is for metadata, we can ignore it.
                    if ( code == 1) { //xml_node
                        extractedDocumentCount++;
                    } else if(code == 0){ // no extraction
                        reader.next();
                    }
                } else {
                    if (reader.isEndElement()) {
                        context.removeLast();
                    }
                    reader.next();
                }
            }
            reader.close();
            directoryStream = Files.newDirectoryStream(tempDir);
        } catch (XMLStreamException | IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
        }
        setTotalCount(extractedDocumentCount);
        return directoryStream.iterator();
    }

    /**
     * Obtains or creates a temporary directory for storing extracted XML elements.
     * <p>
     * The directory location is determined by the following precedence:
     * </p>
     * <ol>
     *   <li>If {@value Options#XML_TEMP_DIR} is specified, use that directory</li>
     *   <li>Otherwise, if {@value Options#TEMP_DIR} is specified, use that directory</li>
     *   <li>Otherwise, use the system default temporary directory</li>
     * </ol>
     * <p>
     * If a custom temporary directory is specified, it must exist, be a directory,
     * and be writable. A unique subdirectory is created within the specified or
     * default temp directory using the XML file name as a prefix.
     * </p>
     *
     * @return the path to the created temporary directory
     * @throws IOException if the temporary directory cannot be created
     * @throws InvalidParameterException if a specified temp directory doesn't exist,
     *         is not a directory, or is not writable
     */
    protected Path getTempDir() throws IOException {
        Path dir;
        String tempDirOption = getProperty(Options.XML_TEMP_DIR);
        if (StringUtils.isBlank(tempDirOption)) {
            tempDirOption = getProperty(Options.TEMP_DIR);
        }
        String prefix = xmlFile != null ? xmlFile.getName() : "temp";
        if (!StringUtils.isBlank(tempDirOption)) {
            File temporaryDirectory = new File(tempDirOption);
            if (!(temporaryDirectory.exists() && temporaryDirectory.isDirectory() && temporaryDirectory.canWrite())) {
                throw new InvalidParameterException(this.getClass().getSimpleName() + " temporary directory " + tempDirOption + " must exist and be writable");
            }
            dir = Files.createTempDirectory(temporaryDirectory.toPath(), prefix, fileAttributes);
        } else {
            dir = Files.createTempDirectory(prefix, fileAttributes);
        }
        return dir;
    }

    /**
     * Determines whether the current element matches extraction criteria and extracts it if so.
     * <p>
     * This method evaluates the current element's XPath against configured patterns:
     * </p>
     * <ol>
     *   <li>Adds the current element's local name to the context stack</li>
     *   <li>Constructs a namespace-insensitive XPath from the context (e.g., "/root/child/element")</li>
     *   <li>If the path matches {@link #streamingXPath} (from {@value Options#XML_NODE}):
     *       <ul>
     *         <li>Creates a temporary file</li>
     *         <li>Uses a Transformer to serialize the element subtree to the file</li>
     *         <li>Returns extraction code 1</li>
     *       </ul>
     *   </li>
     *   <li>Else if the path matches {@link #streamingMetaXPath} (from {@value Options#XML_METADATA})
     *       and metadata hasn't been extracted yet:
     *       <ul>
     *         <li>Transforms the element to a string</li>
     *         <li>Parses it as a DOM document</li>
     *         <li>Stores the root element as {@code customMetadata}</li>
     *         <li>Returns extraction code 2</li>
     *       </ul>
     *   </li>
     *   <li>Otherwise, returns extraction code 0 (no extraction)</li>
     * </ol>
     * <p>
     * <b>Important:</b> The context stack is popped (via {@code finally} block) after successful
     * extraction because the transformer consumes the element and its closing tag.
     * </p>
     *
     * @param reader the StAX stream reader positioned at a start element
     * @param context the deque maintaining the current XPath context (stack of element names)
     * @return extraction code: 0 = no extraction, 1 = element extracted, 2 = metadata extracted
     */
    protected int extractElement(XMLStreamReader reader, Deque<String> context) {
        int extractionCode = 0;

        String localName = reader.getLocalName(); //currently, namespace-insensitive
        context.addLast(localName);
        //construct a namespace-insensitive XPath for the context element
        String currentPath = SLASH + context.stream().collect(Collectors.joining(SLASH));

        if (streamingXPath.matches(currentPath)) {
            try {
                Path file = Files.createTempFile(tempDir, localName, ".xml", fileAttributes);

                Transformer autobot = newTransformer();
                autobot.transform(new StAXSource(reader), new StreamResult(file.toFile()));

                extractionCode = 1;
            } catch (IOException | TransformerException ex) {
                LOG.log(Level.SEVERE, EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
            } finally {
                context.removeLast();
            }
        } else if (customMetadata == null && streamingMetaXPath != null && streamingMetaXPath.matches(currentPath)) {
            try {
                StringWriter writer = new StringWriter();
                Transformer autobot = newTransformer();
                autobot.transform(new StAXSource(reader), new StreamResult(writer));
                String metaAsStr = writer.toString();

                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                Document originalDocument = docBuilder.parse(new ByteArrayInputStream(metaAsStr.getBytes()));
                customMetadata = originalDocument.getDocumentElement();

                extractionCode = 2;
            } catch (TransformerException | IOException | ParserConfigurationException | SAXException ex) {
                LOG.log(Level.SEVERE, EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
            } finally {
                context.removeLast();
            }
        }
        return extractionCode;
    }

    /**
     * Creates a new XML Transformer configured for extracting XML elements.
     * <p>
     * The transformer is configured with the following output properties:
     * </p>
     * <ul>
     *   <li>{@link OutputKeys#OMIT_XML_DECLARATION} = "yes" - Excludes the XML declaration</li>
     *   <li>{@link OutputKeys#INDENT} = "yes" - Enables pretty-printing with indentation</li>
     * </ul>
     * <p>
     * Omitting the XML declaration is useful when the extracted elements will be embedded
     * in another document or processed as fragments.
     * </p>
     *
     * @return a configured {@link Transformer} instance
     * @throws TransformerConfigurationException if the transformer cannot be created
     */
    protected Transformer newTransformer() throws TransformerConfigurationException {
        Transformer transformer = getTransformerFactory().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, YES);
        transformer.setOutputProperty(OutputKeys.INDENT, YES);
        return transformer;
    }

    /**
     * Obtains a TransformerFactory instance, lazily creating it on first access.
     * <p>
     * Creating a {@link TransformerFactory} is an expensive operation, so this method
     * caches the factory and reuses it for all subsequent transformer creation.
     * This lazy initialization pattern improves performance when multiple transformations
     * are needed during XML extraction.
     * </p>
     *
     * @return the cached or newly created {@link TransformerFactory} instance
     */
    protected TransformerFactory getTransformerFactory() {
        //Creating a transformerFactory is expensive, only do it once
        if (transformerFactory == null) {
            transformerFactory = TransformerFactory.newInstance();
        }
        return transformerFactory;
    }
}
