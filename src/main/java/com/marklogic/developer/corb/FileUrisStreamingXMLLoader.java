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

import static com.marklogic.developer.corb.Options.XML_FILE;
import static com.marklogic.developer.corb.Options.XML_NODE;
import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.StringUtils;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.security.InvalidParameterException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
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
 * @since 2.4.0
 */
public class FileUrisStreamingXMLLoader extends FileUrisXMLLoader {

    protected static final Logger LOG = Logger.getLogger(FileUrisStreamingXMLLoader.class.getName());
    private static final String SLASH = "/";
    private Path tempDir;
    private DirectoryStream<Path> directoryStream;
    private Iterator<Path> files;
    private final FileAttribute<?>[] fileAttributes = new FileAttribute<?>[0];
    private StreamingXPath streamingXPath;

    @Override
    public void open() throws CorbException {
        String xmlFilename = getProperty(XML_FILE);
        String xPath = getProperty(XML_NODE);
        // default processing will split on child elements of the document element
        xPath = StringUtils.isBlank(xPath) ? "/*/*" : xPath;
        streamingXPath = new StreamingXPath(xPath);
        xmlFile = FileUtils.getFile(xmlFilename);
        try {
            schemaValidate(xmlFile);
            if (shouldSetBatchRef()) {
                //set the original XML filename, for reference in processing modules
                batchRef = xmlFile.getCanonicalPath();
            }
            tempDir = getTempDir();
            files = readToTempDir(xmlFile.toPath());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
        }
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
                Map<String, String> metadata = getMetadata(file);
                metadata.put(META_SOURCE, xmlFile.getCanonicalPath());
                Node node;
                if (shouldBase64Encode()) {
                    try (InputStream inputStream = new FileInputStream(file)) {
                        node = toLoaderDoc(metadata, inputStream);
                    }
                } else {
                    DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                    Document document = docBuilder.parse(file);
                    node = toLoaderDoc(metadata, document.getDocumentElement(), false);
                }
                content = nodeToString(node);
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
        int extractedDocumentCount = 0;

        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try (Reader fileReader = Files.newBufferedReader(xmlFile)) {
            XMLStreamReader reader = xmlInputFactory.createXMLStreamReader(fileReader);

            Deque<String> context = new ArrayDeque<>();
            while (reader.hasNext()) {
                // if there is a problem extracting an element, don't count it
                if (reader.isStartElement() && extractElement(reader, context)) {
                    extractedDocumentCount++;
                }
                if (reader.isEndElement()) {
                    context.pop();
                }
                reader.next();
            }
            reader.close();
            directoryStream = Files.newDirectoryStream(tempDir);
        } catch (XMLStreamException | IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
        }
        setTotalCount(extractedDocumentCount);
        return directoryStream.iterator(); //tempDir.toFile().listFiles();
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
        String tempDirOption = getProperty(Options.TEMP_DIR);
        if (!StringUtils.isBlank(tempDirOption)) {
            File temporaryDirectory = new File(tempDirOption);
            if (!(temporaryDirectory.exists() && temporaryDirectory.isDirectory() && temporaryDirectory.canWrite())) {
                throw new InvalidParameterException(this.getClass().getSimpleName() + " temporary directory must exist and be writable");
            }
            dir = temporaryDirectory.toPath();
        } else {
            dir = Files.createTempDirectory(xmlFile.getName(), fileAttributes);
        }
        return dir;
    }

    /**
     * Determine whether the context node should be extracted. If so, save as a
     * separate file.
     *
     * @param reader
     * @param context
     * @return boolean indicating whether an element was successfully extracted
     */
    protected boolean extractElement(XMLStreamReader reader, Deque<String> context) {
        boolean elementWasExtracted = false;

        String localName = reader.getLocalName(); //currently, namespace-insensitive
        context.add(localName);
        //construct a namespace-insensitive XPath for the context element
        String currentPath = SLASH + context.stream().collect(Collectors.joining(SLASH));

        if (streamingXPath.matches(currentPath)) {
            try {
                Path file = Files.createTempFile(tempDir, localName, ".xml", fileAttributes);

                Transformer autobot = newTransformer();
                autobot.transform(new StAXSource(reader), new StreamResult(file.toFile()));

                elementWasExtracted = true;
            } catch (IOException | TransformerException ex) {
                LOG.log(Level.SEVERE, EXCEPTION_MSG_PROBLEM_READING_XML_FILE, ex);
                //TODO rethrow? should an exception reading one element halt the entire set, or just move on?
            } finally {
                context.pop();
            }
        }
        return elementWasExtracted;
    }

}
