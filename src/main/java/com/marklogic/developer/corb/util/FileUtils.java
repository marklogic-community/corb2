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
package com.marklogic.developer.corb.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;
import java.nio.file.SimpleFileVisitor;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Common file manipulation utilities
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class FileUtils {
    private static final Logger LOG = Logger.getLogger(FileUtils.class.getName());
    private FileUtils() {
    }

    /**
     * Delete a file or folder and all of it's contents.
     *
     * @param file The file to be deleted.
     * @throws IOException
     */
    public static void deleteFile(final File file) throws IOException {
        delete(file.toPath());
    }

    /**
     * Delete a file.
     *
     * @param path Path to the file to be deleted.
     * @throws IOException
     */
    public static void deleteFile(final String path) throws IOException {
        deleteFile(getFile(path));
    }

    public static void deleteQuietly(final Path directory) {
        if (directory != null && directory.toFile().exists()) {
            try {
                FileUtils.delete(directory);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Unable to delete dir: " + directory.toString(), ex);
            }
        }
    }

    public static void deleteFileQuietly(final String directory, final String filename) {
        if (filename != null) {
            File file = new File(directory, filename);
            FileUtils.deleteQuietly(file.toPath());
        }
    }

    public static void delete(final Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(final Path file, final IOException e) {
                return handleException();
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
                if (e != null) {
                    return handleException();
                }
                Files.delete(dir);
                return CONTINUE;
            }

            private FileVisitResult handleException() {
                return TERMINATE;
            }
        });
    }

    /**
     * Moves a file. If the destination already exists, deletes before moving
     * source.
     *
     * @param source The file to be moved.
     * @param dest The destination file.
     */
    public static void moveFile(final File source, final File dest) {
        if (!source.getAbsolutePath().equals(dest.getAbsolutePath()) && source.exists()) {
            try {
                Files.deleteIfExists(dest.toPath());
            } catch (IOException ex) {
                LOG.log(Level.WARNING, MessageFormat.format("Unable to delete file: {0}", dest), ex);
            }
            if (!source.renameTo(dest)){
                LOG.log(Level.WARNING, () -> MessageFormat.format("Unable to rename {0} to {1}", source, dest));
            }
        }
    }

    /**
     * Determine how many lines are in the file. Returns 0 if the file is null
     * or does not exist.
     *
     * @param file
     * @return
     * @throws IOException
     */
    public static int getLineCount(final File file) throws IOException {
        if (file != null && file.exists()) {
            try (LineNumberReader lnr = new LineNumberReader(new FileReader(file))) {
                lnr.skip(Long.MAX_VALUE);
                return lnr.getLineNumber();
            }
        }
        return 0;
    }

    /**
     * Find the file with the given name. First checking for resources on the
     * classpath, then constructing a new File object.
     *
     * @param filename
     * @return File
     */
    public static File getFile(final String filename) {
        File file;
        ClassLoader classLoader = FileUtils.class.getClassLoader();
        URL resource = classLoader.getResource(filename);
        if (resource != null) {
            file = new File(resource.getFile());
        } else {
            file = new File(filename);
        }
        return file;
    }

}
