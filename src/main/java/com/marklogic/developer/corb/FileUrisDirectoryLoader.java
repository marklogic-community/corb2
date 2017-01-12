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
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisDirectoryLoader extends AbstractFileUrisLoader {

    private static final Logger LOG = Logger.getLogger(FileUrisJSONLoader.class.getName());
    protected static final String EXCEPTION_MSG_PROBLEM_READING_FILE = "Problem while reading the file";
    private DirectoryStream<Path> directoryStream;
    private Iterator<Path> fileIterator;

    @Override
    public void open() throws CorbException {

        String dirName = getProperty(Options.FILE_LOADER_PATH);
        
        Path dir = Paths.get(dirName);
        if (!(Files.exists(dir) 
                && Files.isDirectory(dir) 
                && Files.isReadable(dir))) {
            throw new CorbException(MessageFormat.format("{0}: {1} must be specified and an accessible directory", Options.FILE_LOADER_PATH, dirName));
        }
        
        try {
            directoryStream = Files.newDirectoryStream(dir, (Path entry) -> {
                return this.accept(entry);
            });
            fileIterator = directoryStream.iterator();
            setTotalCount(fileCount(dir));
        } catch (IOException ex) {
            throw new CorbException("Problem loading data from xml file ", ex);
        }
    }

    protected int fileCount(Path dir) throws IOException {
        return Math.toIntExact(
                Files.walk(dir)
                .collect(Collectors.toList())
                .parallelStream()
                .filter(p -> this.accept(p))
                .count());
    }

    /**
     * Criteria to filter Path resources (files that are not hidden)
     *
     * @param path
     * @return
     */
    protected boolean accept(Path path) {
        File file = path.toFile();
        //TODO custom property with pattern to filter out unwanted files, or just let the process module do the filtering?
        return !(file.isHidden() || file.isDirectory());
    }

    @Override
    public boolean hasNext() throws CorbException {
        return fileIterator != null && fileIterator.hasNext();
    }

    @Override
    public String next() throws CorbException {
        Path path = fileIterator.next();
        File file = path.toFile();
        return nodeToString(toIngestDoc(file));
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(directoryStream);
        cleanup();
    }
}
