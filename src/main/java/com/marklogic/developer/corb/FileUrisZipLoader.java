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

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.IOUtils;
import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.developer.corb.util.XmlUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 */
public class FileUrisZipLoader extends AbstractFileUrisLoader {

    public static final String META_COMMENT = "comment";
    protected static final Logger LOG = Logger.getLogger(FileUrisZipLoader.class.getName());
    public static final String EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE = "Problem reading zip file";

    protected ZipFile zipFile = null;
    protected Iterator<? extends ZipEntry> files;

    @Override
    public void open() throws CorbException {

        String zipFilename = getLoaderPath(Options.ZIP_FILE);

        try {
            File file = FileUtils.getFile(zipFilename);
            zipFile = new ZipFile(file);
            if (shouldSetBatchRef()) {
                batchRef = zipFile.getName();
            }
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE, ex);
        }
        Predicate<ZipEntry> isFile = ze -> !ze.isDirectory();
        files = zipFile.stream().filter(isFile).iterator();
        setTotalCount(Math.toIntExact(zipFile.stream().parallel().filter(isFile).count()));
    }

    @Override
    public boolean hasNext() throws CorbException {
        return files.hasNext();
    }

    @Override
    public String next() throws CorbException {
        ZipEntry zipEntry = files.next();
        Map<String, String> metadata = getMetadata(zipEntry);
        try (InputStream stream = new BufferedInputStream(zipFile.getInputStream(zipEntry))) {
            return XmlUtils.documentToString(toLoaderDoc(metadata, stream));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE, ex);
        }
    }

    @Override
    protected Map<String, String> getMetadata(File file) {
        throw new UnsupportedOperationException("Invalid operation for FileUrisZipLoader. Invoke with a ZipEntry.");
    }

    protected Map<String, String> getMetadata(ZipEntry zipEntry) {
        Map<String, String> metadata = new HashMap<>();
        if (zipFile != null) {
            metadata.put(META_SOURCE, zipFile.getName());
        }
        metadata.put(META_FILENAME, zipEntry.getName());

        FileTime lastModifiedTime = zipEntry.getLastModifiedTime();
        if (lastModifiedTime != null) {
            metadata.put(META_LAST_MODIFIED, toISODateTime(lastModifiedTime));
        }
        String comment = zipEntry.getComment();
        if (StringUtils.isNotEmpty(comment)) {
            metadata.put(META_COMMENT, comment);
        }
        return metadata;
    }

    @Override
    public void close() {
        super.close();
        IOUtils.closeQuietly(zipFile);
    }
}
