/*
  * * Copyright (c) 2004-2017 MarkLogic Corporation
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
package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.StringUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public abstract class AbstractFileUrisLoader extends AbstractUrisLoader {

    public static final String META_CONTENT_TYPE = "contentType";
    public static final String META_FILENAME = "filename";
    public static final String META_LAST_MODIFIED = "lastModified";
    public static final String META_SOURCE = "source";
    
    //TODO: replacements?
    private final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
    private TransformerFactory transformerFactory;
    private static final String YES = "yes";

    protected Document toIngestDoc(File file) throws CorbException {
        try (InputStream inputStream = new FileInputStream(file)) {
            Map<String, String> metadata = getMetadata(file);
            return toIngestDoc(metadata, inputStream);
        } catch (IOException ex) {
            throw new CorbException("Error reading file metadata", ex);
        }
    }
    
    protected Document toIngestDoc(Map<String, String> metadata, InputStream inputStream) throws CorbException {
        try {
            return toIngestDoc(metadata, IOUtils.toBase64(inputStream), true);
        } catch (IOException ex) {
            throw new CorbException("Problem generating base64", ex);
        }
    }

    protected Document toIngestDoc(Map<String, String> metadata, String content, boolean isContentBase64Encoded) throws CorbException {
        try {
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

            Document doc = docBuilder.newDocument();
            Element docElement = doc.createElement("corb-loader"); //TODO better name? MLCP uses <com.marklogic.contentpump.metadata/> for their archive metadata files
            
            Element metadataElement = doc.createElement("metadata");
            docElement.appendChild(metadataElement);

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                Element metaElement = doc.createElement(entry.getKey());
                Text value = doc.createTextNode(entry.getValue());
                metaElement.appendChild(value);
                metadataElement.appendChild(metaElement);
            }

            Element contentElement = doc.createElement("content");
            //Attr nameAttr = doc.createAttribute("base64Encoded");
            //nameAttr.setValue(Boolean.toString(isContentBase64Encoded));
            contentElement.setAttribute("base64Encoded", Boolean.toString(isContentBase64Encoded));
            
            Text contentText = doc.createTextNode(content);
            contentElement.appendChild(contentText);
            
            docElement.appendChild(contentElement);
            doc.appendChild(docElement);

            return doc;
        } catch (ParserConfigurationException ex) {
            throw new CorbException("Error generating corb-ingest document", ex);
        }
    }

    protected Map<String, String> getMetadata(File file) throws IOException {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(META_FILENAME, file.getCanonicalPath());
        
        String lastModified = this.toISODateTime(file.lastModified());
        if (StringUtils.isNotBlank(lastModified)) {
            metadata.put(META_LAST_MODIFIED, lastModified);
        } 
        String contentType = Files.probeContentType(file.toPath());
        if (StringUtils.isNotEmpty(contentType)) {
            metadata.put(META_CONTENT_TYPE, contentType);
        }
        return metadata;
    }

    protected String toISODateTime(FileTime fileTime) {
        return toISODateTime(fileTime.toInstant());
    }

    protected String toISODateTime(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        return toISODateTime(instant);
    }

    protected String toISODateTime(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX")
                .withZone(ZoneOffset.UTC)
                .format(instant);
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
     * Use an identity transform to serialize a Node to a string.
     *
     * @param node
     * @return
     * @throws CorbException
     */
    protected String nodeToString(Node node) throws CorbException {
        StringWriter sw = new StringWriter();
        try {
            Transformer autobot = newTransformer();
            autobot.transform(new DOMSource(node), new StreamResult(sw));
        } catch (TransformerException te) {
            throw new CorbException("nodeToString Transformer Exception", te);
        }
        return sw.toString();
    }
}
