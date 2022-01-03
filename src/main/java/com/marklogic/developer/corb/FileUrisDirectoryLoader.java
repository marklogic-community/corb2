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

import com.marklogic.developer.corb.util.XmlUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @since 2.4.0
 */
public class FileUrisDirectoryLoader extends AbstractFileUrisLoader {

    protected static final String EXCEPTION_MSG_PROBLEM_READING_FILE = "Problem while reading the file";
    private Iterator<Path> fileIterator;
    private Stream<Path> fileStream;

    @Override
    public void open() throws CorbException {

        String dirName = getLoaderPath();

        Path dir = Paths.get(dirName);
        File file = dir.toFile();
        if (!(file.exists() && file.isDirectory() && Files.isReadable(dir))) {
            throw new CorbException(MessageFormat.format("{0}: {1} must be specified and an accessible directory", Options.LOADER_PATH, dirName));
        }

        try {
            if (shouldSetBatchRef()) {
                batchRef = file.getCanonicalPath();
            }
            fileStream = Files.walk(dir);
            fileIterator = fileStream.filter(this::accept).iterator();
            setTotalCount(fileCount(dir));
        } catch (IOException ex) {
            throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_FILE, ex);
        }
    }

    protected int fileCount(Path dir) throws IOException {
        try (Stream<Path> stream = Files.walk(dir)) {
            return Math.toIntExact(stream.parallel()
                    .filter(this::accept)
                    .count());
        }
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
        return XmlUtils.documentToString(toLoaderDoc(file));
    }

    @Override
    public void close() {
        super.close();
        if (fileStream != null) {
            fileStream.close();
        }
    }
}
