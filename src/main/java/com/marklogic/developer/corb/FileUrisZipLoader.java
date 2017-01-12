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
import java.io.BufferedInputStream;
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
 */
public class FileUrisZipLoader extends AbstractFileUrisLoader {
   
    public static final String META_COMMENT = "comment";
    public static final String META_CREATION_TIME = "creationTime";
    public static final String META_LAST_ACCESSED_TIME = "lastAccessedTime";
    protected static final Logger LOG = Logger.getLogger(FileUrisZipLoader.class.getName());
    public static final String EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE = "Problem reading zip file";

    protected ZipFile zipFile = null;
    protected Iterator<? extends ZipEntry> files;

    @Override
    public void open() throws CorbException {
        String zipFilename = getProperty(Options.ZIP_FILE);
        Predicate<ZipEntry> isFile = ze -> !ze.isDirectory();
        try {
            zipFile = new ZipFile(zipFilename);
            batchRef = zipFile.getName(); //TODO: evaluate if needed
            
            //TODO we could just extract these files to the temp dir, then process with FileUrisDirectoryLoader...
            files = zipFile.stream().filter(isFile).iterator();
            setTotalCount(Math.toIntExact(zipFile.stream().parallel().filter(isFile).count()));
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE, ex);
        }
    }

    @Override
    public boolean hasNext() throws CorbException {
        return files.hasNext();
    }

    @Override
    public String next() throws CorbException {
        ZipEntry zipEntry = files.next();
        Map<String, String> metadata = getMetadata(zipEntry);
        //TODO: add a metadata entry to record the index/position?
        //TODO save temp file, and use consistent toIngestDoc(File) method? would allow for sniffing contentType
        // - if file is used, remember to merge metadata from zipEntry and new file (and not stomp on timeStamps with temp file)
        try (InputStream stream = new BufferedInputStream(zipFile.getInputStream(zipEntry))) {
            return nodeToString(toIngestDoc(metadata, stream));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_ZIP_FILE, ex);
        }
    }

    protected Map<String, String> getMetadata(ZipEntry zipEntry) {
        Map<String, String> metadata = new HashMap<>();
        
        metadata.put(META_SOURCE, zipFile.getName());

        if (StringUtils.isNotEmpty(zipEntry.getName())) {
            metadata.put(META_FILENAME, zipEntry.getName());
        }
        FileTime creationTime = zipEntry.getCreationTime();
        if (creationTime != null) {
            metadata.put(META_CREATION_TIME, toISODateTime(creationTime));
        }
        FileTime lastAccessedTime = zipEntry.getLastAccessTime();
        if (lastAccessedTime != null) {
            metadata.put(META_LAST_ACCESSED_TIME, toISODateTime(lastAccessedTime));
        }
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
        IOUtils.closeQuietly(zipFile);
        cleanup();
    }
}
