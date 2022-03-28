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
package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.StringUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 */
public abstract class AbstractFileUrisLoader extends AbstractUrisLoader {

    public static final String LOADER_DOC = "corb-loader";
    public static final String CONTENT = "content";
    public static final String BASE64_ENCODED = "base64Encoded";
    public static final String LOADER_METADATA = "metadata";
    public static final String META_CONTENT_TYPE = "contentType";
    public static final String META_FILENAME = "filename";
    public static final String META_PATH = "path";
    public static final String META_LAST_MODIFIED = "lastModified";
    public static final String META_SOURCE = "source";

    protected final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();

    protected Document toLoaderDoc(File file) throws CorbException {
        try (InputStream inputStream = new FileInputStream(file)) {
            Map<String, String> metadata = getMetadata(file);
            return AbstractFileUrisLoader.this.toLoaderDoc(metadata, inputStream);
        } catch (IOException ex) {
            throw new CorbException("Error reading file metadata", ex);
        }
    }

    protected Document toLoaderDoc(Map<String, String> metadata, InputStream inputStream) throws CorbException {
        try {
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            String content = IOUtils.toBase64(inputStream);
            Text contentText = doc.createTextNode(content);
            return AbstractFileUrisLoader.this.toLoaderDoc(metadata, contentText, true);
        } catch (ParserConfigurationException | IOException ex) {
            throw new CorbException("Problem generating base64", ex);
        }
    }

    protected Document toLoaderDoc(Map<String, String> metadata, Node content, boolean isContentBase64Encoded) throws CorbException {
        try {
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element docElement = doc.createElement(LOADER_DOC);

            Element metadataElement = doc.createElement(LOADER_METADATA);
            docElement.appendChild(metadataElement);

            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                Element metaElement = doc.createElement(entry.getKey());
                Text value = doc.createTextNode(entry.getValue());
                metaElement.appendChild(value);
                metadataElement.appendChild(metaElement);
            }

            Element contentElement = doc.createElement(CONTENT);
            contentElement.setAttribute(BASE64_ENCODED, Boolean.toString(isContentBase64Encoded));
            Node importedNode = doc.importNode(content, true);
            contentElement.appendChild(importedNode);

            docElement.appendChild(contentElement);
            doc.appendChild(docElement);

            return doc;
        } catch (ParserConfigurationException ex) {
            throw new CorbException("Error generating corb-ingest document", ex);
        }
    }

    protected Map<String, String> getMetadata(File file) throws IOException {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(META_FILENAME, file.getName());
        metadata.put(META_PATH, getMetaPath(file));

        String loaderPath = getLoaderPath();
        if (StringUtils.isNotBlank(loaderPath)) {
            metadata.put(META_SOURCE, loaderPath);
        }

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

    protected String getMetaPath(File file) throws IOException {
        String path = file.getCanonicalPath();
        String loaderPath = getLoaderPath();

        if (StringUtils.isNotBlank(loaderPath)) {
            String loaderPathCanonicalPath = Paths.get(loaderPath).toFile().getCanonicalPath();
            if (path.startsWith(loaderPathCanonicalPath)) {
                path = path.substring(loaderPathCanonicalPath.length() + 1);
            }
        }
        return path;
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

    protected boolean shouldUseEnvelope() {
        String useEnvelope = getProperty(Options.LOADER_USE_ENVELOPE);
        return StringUtils.stringToBoolean(useEnvelope, true);
    }

    protected boolean shouldBase64Encode() {
        String shouldEncode = getProperty(Options.LOADER_BASE64_ENCODE);
        return StringUtils.stringToBoolean(shouldEncode, true);
    }
}
