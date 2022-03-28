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
 * Split an XML file {@value Options#XML_FILE} into multiple documents using the
 * {@link StreamingXPath } expression from the {@value Options#XML_NODE} property and
 * sends the serialized XML string to the process module in the URIS parameter.
 * Uses a StAX parser in order to limit memory consumption and process very
 * large XML inputs.
 *
 * Optionally validate the XML file prior to processing. Specify the XSD with
 * the {@value Options#XML_SCHEMA}
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @author Bhagat Bandlamudi
 * @since 2.4.0
 */
public class FileUrisStreamingXMLLoader extends FileUrisXMLLoader {

    protected static final Logger LOG = Logger.getLogger(FileUrisStreamingXMLLoader.class.getName());
    private static final String YES = "yes";
    private static final String SLASH = "/";
    private Path tempDir;
    private DirectoryStream<Path> directoryStream;
    private Iterator<Path> files;
    private final FileAttribute<?>[] fileAttributes = new FileAttribute<?>[0];
    private StreamingXPath streamingXPath;
    private StreamingXPath streamingMetaXPath;
    private TransformerFactory transformerFactory;

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

    @Override
    public boolean hasNext() throws CorbException {
        return files.hasNext();
    }

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

    @Override
    public void cleanup() {
        super.cleanup();
        FileUtils.deleteQuietly(tempDir);
    }

    @Override
    public void close() {
        super.close();
        IOUtils.closeQuietly(directoryStream);
        cleanup();
    }

    /**
     * Read the large XML file and split into numerous smaller files, returning
     * an iterator for those files.
     *
     * @param xmlFile
     * @throws CorbException
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
     * Obtain the Path to a temporary directory that can be used to store files
     * for processing.
     *
     * @return Path to the temporary directory.
     * @throws IOException
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
     * Determine whether the context node should be extracted. If so, save as a
     * separate file.
     *
     * @param reader
     * @param context
     * @return int code indicating whether an element was successfully extracted
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
     * Instantiates a new Transformer object with output options to omit the XML
     * declaration and indent enabled.
     *
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
     * Lazy-load a new instance of a TransformerFactory. Subsequent calls,
     * re-use the existing TransformerFactory.
     *
     * @return TransformerFactory
     */
    protected TransformerFactory getTransformerFactory() {
        //Creating a transformerFactory is expensive, only do it once
        if (transformerFactory == null) {
            transformerFactory = TransformerFactory.newInstance();
        }
        return transformerFactory;
    }
}
