/*
  * * Copyright (c) 2004-2023 MarkLogic Corporation
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
 * Abstract base class for file-based URI loaders.
 * Provides common functionality for loading files and creating XML loader documents
 * with metadata and content. The loader documents contain file metadata (filename,
 * path, content type, last modified date) and optionally base64-encoded content.
 * Subclasses must implement specific file loading strategies.
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 */
public abstract class AbstractFileUrisLoader extends AbstractUrisLoader {

    /**
     * Root element name for the loader document XML structure.
     * <p>
     * The loader document wraps all metadata and content in a {@code <corb-loader>} element.
     * </p>
     */
    public static final String LOADER_DOC = "corb-loader";

    /**
     * Element name for the content section of the loader document.
     * <p>
     * The {@code <content>} element contains the actual file data, which may be
     * base64-encoded or in its original format.
     * </p>
     */
    public static final String CONTENT = "content";

    /**
     * Attribute name indicating whether content is base64-encoded.
     * <p>
     * This attribute is set on the {@code <content>} element with a value of
     * "true" or "false" to indicate the encoding state of the content.
     * </p>
     */
    public static final String BASE64_ENCODED = "base64Encoded";

    /**
     * Element name for the metadata section of the loader document.
     * <p>
     * The {@code <metadata>} element contains child elements for each metadata
     * field such as filename, path, content type, and last modified date.
     * </p>
     */
    public static final String LOADER_METADATA = "metadata";

    /**
     * Metadata element name for the content type (MIME type) of the file.
     * <p>
     * Example values: "text/plain", "application/xml", "image/jpeg"
     * </p>
     */
    public static final String META_CONTENT_TYPE = "contentType";

    /**
     * Metadata element name for the filename (without path).
     * <p>
     * Contains only the file's basename, not the full path.
     * Example: "document.xml" from "/path/to/document.xml"
     * </p>
     */
    public static final String META_FILENAME = "filename";

    /**
     * Metadata element name for the file path.
     * <p>
     * Contains the canonical file path, potentially relative to the loader path
     * if one is configured.
     * </p>
     */
    public static final String META_PATH = "path";

    /**
     * Metadata element name for the last modified timestamp.
     * <p>
     * The timestamp is formatted as an ISO 8601 date-time string in UTC timezone,
     * using the format "yyyy-MM-dd'T'HH:mmX".
     * </p>
     */
    public static final String META_LAST_MODIFIED = "lastModified";

    /**
     * Metadata element name for the source path (loader path).
     * <p>
     * Contains the base loader path from which files are being loaded,
     * if one is configured.
     * </p>
     */
    public static final String META_SOURCE = "source";

    /**
     * Document builder factory used for creating XML documents.
     * <p>
     * This factory is configured with namespace awareness enabled and is used
     * throughout the class to create DocumentBuilder instances for generating
     * loader documents.
     * </p>
     */
    protected final DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();

    /**
     * Constructor that initializes the document builder factory with namespace awareness enabled.
     */
    public AbstractFileUrisLoader() {
        docFactory.setNamespaceAware(true);
    }

    /**
     * Creates a loader document from a file.
     * Reads the file, extracts metadata, and creates an XML document containing
     * the metadata and base64-encoded content.
     *
     * @param file the file to create a loader document from
     * @return an XML Document containing file metadata and content
     * @throws CorbException if an error occurs reading the file or generating the document
     */
    protected Document toLoaderDoc(File file) throws CorbException {
        try (InputStream inputStream = new FileInputStream(file)) {
            Map<String, String> metadata = getMetadata(file);
            return AbstractFileUrisLoader.this.toLoaderDoc(metadata, inputStream);
        } catch (IOException ex) {
            throw new CorbException("Error reading file metadata", ex);
        }
    }

    /**
     * Creates a loader document from metadata and an input stream.
     * Reads the input stream, base64-encodes the content, and creates an XML document
     * containing the metadata and encoded content.
     *
     * @param metadata map of metadata key-value pairs
     * @param inputStream the input stream containing the file content
     * @return an XML Document containing metadata and base64-encoded content
     * @throws CorbException if an error occurs encoding the content or generating the document
     */
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

    /**
     * Creates a loader document from metadata and a content node.
     * Constructs an XML document with the structure:
     * <pre>
     * &lt;corb-loader&gt;
     *   &lt;metadata&gt;
     *     &lt;key&gt;value&lt;/key&gt;
     *     ...
     *   &lt;/metadata&gt;
     *   &lt;content base64Encoded="true|false"&gt;
     *     [content node]
     *   &lt;/content&gt;
     * &lt;/corb-loader&gt;
     * </pre>
     *
     * @param metadata map of metadata key-value pairs
     * @param content the DOM node containing the content
     * @param isContentBase64Encoded true if content is base64-encoded, false otherwise
     * @return an XML Document containing metadata and content
     * @throws CorbException if an error occurs generating the document
     */
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

    /**
     * Extracts metadata from a file.
     * Collects filename, path, source, last modified date, and content type.
     * The path is relative to the loader path if one is configured.
     *
     * @param file the file to extract metadata from
     * @return a map containing metadata key-value pairs
     * @throws IOException if an error occurs reading file attributes
     */
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

    /**
     * Gets the file path for metadata, relative to the loader path if configured.
     * If a loader path is set and the file path starts with it, the loader path
     * prefix is removed to create a relative path.
     *
     * @param file the file to get the path for
     * @return the canonical path, possibly relative to the loader path
     * @throws IOException if an error occurs resolving the canonical path
     */
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

    /**
     * Converts a FileTime to an ISO 8601 formatted date-time string.
     * Uses the format "yyyy-MM-dd'T'HH:mmX" in UTC timezone.
     *
     * @param fileTime the file time to convert
     * @return an ISO 8601 formatted date-time string
     */
    protected String toISODateTime(FileTime fileTime) {
        return toISODateTime(fileTime.toInstant());
    }

    /**
     * Converts a timestamp in milliseconds to an ISO 8601 formatted date-time string.
     * Uses the format "yyyy-MM-dd'T'HH:mmX" in UTC timezone.
     *
     * @param millis the timestamp in milliseconds since epoch
     * @return an ISO 8601 formatted date-time string
     */
    protected String toISODateTime(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        return toISODateTime(instant);
    }

    /**
     * Converts an Instant to an ISO 8601 formatted date-time string.
     * Uses the format "yyyy-MM-dd'T'HH:mmX" in UTC timezone.
     *
     * @param instant the instant to convert
     * @return an ISO 8601 formatted date-time string (e.g., "2023-12-15T14:30Z")
     */
    protected String toISODateTime(Instant instant) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmX")
                .withZone(ZoneOffset.UTC)
                .format(instant);
    }

    /**
     * Determines whether to use an envelope structure for loader documents.
     * Reads the LOADER_USE_ENVELOPE property, defaulting to true if not set.
     *
     * @return true if envelope structure should be used, false otherwise
     */
    protected boolean shouldUseEnvelope() {
        String useEnvelope = getProperty(Options.LOADER_USE_ENVELOPE);
        return StringUtils.stringToBoolean(useEnvelope, true);
    }

    /**
     * Determines whether to base64-encode file content.
     * Reads the LOADER_BASE64_ENCODE property, defaulting to true if not set.
     *
     * @return true if content should be base64-encoded, false otherwise
     */
    protected boolean shouldBase64Encode() {
        String shouldEncode = getProperty(Options.LOADER_BASE64_ENCODE);
        return StringUtils.stringToBoolean(shouldEncode, true);
    }
}
