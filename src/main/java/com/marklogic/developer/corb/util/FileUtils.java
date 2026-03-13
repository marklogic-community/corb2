/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import org.jetbrains.annotations.NotNull;

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
 * Common file manipulation utilities for CoRB operations.
 * <p>
 * This utility class provides static methods for common file operations including:
 * </p>
 * <ul>
 *   <li>Deleting files and directories (recursively)</li>
 *   <li>Moving files with overwrite support</li>
 *   <li>Counting lines in text files</li>
 *   <li>Locating files on the classpath or filesystem</li>
 * </ul>
 * <p>
 * Many methods have "quiet" variants that suppress exceptions and log warnings instead,
 * useful for cleanup operations where failures should not stop execution.
 * </p>
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class FileUtils {
    private static final Logger LOG = Logger.getLogger(FileUtils.class.getName());

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private FileUtils() {
    }

    /**
     * Deletes a file or directory and all of its contents recursively.
     * <p>
     * If the specified file is a directory, this method will recursively delete
     * all files and subdirectories within it before deleting the directory itself.
     * </p>
     *
     * @param file the file or directory to be deleted, must not be {@code null}
     * @throws IOException if an I/O error occurs during deletion or if the file cannot be deleted
     */
    public static void deleteFile(final File file) throws IOException {
        delete(file.toPath());
    }

    /**
     * Deletes a file or directory specified by path string.
     * <p>
     * This is a convenience method that constructs a File object from the path
     * and delegates to {@link #deleteFile(File)}. The path can be a classpath
     * resource or a filesystem path.
     * </p>
     *
     * @param path the path to the file or directory to be deleted, must not be {@code null}
     * @throws IOException if an I/O error occurs during deletion or if the file cannot be deleted
     * @see #getFile(String)
     */
    public static void deleteFile(final String path) throws IOException {
        deleteFile(getFile(path));
    }

    /**
     * Deletes a directory and all of its contents, suppressing any exceptions.
     * <p>
     * This "quiet" method does not throw exceptions. If an error occurs during deletion,
     * it is logged as a warning and the method returns normally. This is useful for
     * cleanup operations where deletion failures should not stop execution.
     * </p>
     * <p>
     * If the directory is {@code null} or does not exist, this method does nothing.
     * </p>
     *
     * @param directory the directory to delete, may be {@code null}
     */
    public static void deleteQuietly(final Path directory) {
        if (directory != null && directory.toFile().exists()) {
            try {
                FileUtils.delete(directory);
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Unable to delete dir: " + directory.toString(), ex);
            }
        }
    }

    /**
     * Deletes a file in a specified directory, suppressing any exceptions.
     * <p>
     * This is a convenience method that constructs the full file path from the
     * directory and filename, then delegates to {@link #deleteQuietly(Path)}.
     * </p>
     * <p>
     * If the filename is {@code null}, this method does nothing.
     * </p>
     *
     * @param directory the directory containing the file to delete, may be {@code null}
     * @param filename the name of the file to delete, may be {@code null}
     */
    public static void deleteFileQuietly(final String directory, final String filename) {
        if (filename != null) {
            File file = new File(directory, filename);
            FileUtils.deleteQuietly(file.toPath());
        }
    }

    /**
     * Recursively deletes a file or directory using the NIO.2 file tree walker.
     * <p>
     * This method uses {@link Files#walkFileTree(Path, java.nio.file.FileVisitor)} to
     * traverse the file tree and delete all files and directories. The traversal is
     * depth-first, ensuring that directory contents are deleted before the directory itself.
     * </p>
     * <p>
     * If any file or directory cannot be deleted, the traversal is terminated and
     * an exception is thrown.
     * </p>
     *
     * @param path the path to the file or directory to delete, must not be {@code null}
     * @throws IOException if an I/O error occurs during deletion or tree traversal
     */
    public static void delete(final Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {

            @Override
            public @NotNull FileVisitResult visitFile(final @NotNull Path file, final @NotNull BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return CONTINUE;
            }

            @Override
            public @NotNull FileVisitResult visitFileFailed(final @NotNull Path file, final @NotNull IOException e) {
                return handleException();
            }

            @Override
            public @NotNull FileVisitResult postVisitDirectory(final @NotNull Path dir, final IOException e) throws IOException {
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
     * Moves a file from source to destination.
     * <p>
     * This method performs the following actions:
     * </p>
     * <ol>
     *   <li>Checks if source and destination are different and source exists</li>
     *   <li>Deletes the destination file if it already exists</li>
     *   <li>Renames (moves) the source file to the destination</li>
     * </ol>
     * <p>
     * If the source and destination paths are identical, or if the source does not exist,
     * this method does nothing. Any errors during deletion or renaming are logged as warnings
     * but do not throw exceptions.
     * </p>
     * <p>
     * <b>Note:</b> This method uses {@link File#renameTo(File)}, which may not work across
     * different file systems or volumes. For cross-filesystem moves, consider using
     * {@link Files#move(Path, Path, java.nio.file.CopyOption...)} instead.
     * </p>
     *
     * @param source the file to be moved, must not be {@code null}
     * @param dest the destination file path, must not be {@code null}
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
     * Determines the number of lines in a text file.
     * <p>
     * This method reads through the entire file to count the number of lines.
     * It uses a {@link LineNumberReader} to efficiently count lines without
     * storing file contents in memory.
     * </p>
     * <p>
     * If the file is {@code null} or does not exist, this method returns 0.
     * </p>
     *
     * @param file the file to count lines in, may be {@code null}
     * @return the number of lines in the file, or 0 if the file is null or does not exist
     * @throws IOException if an I/O error occurs while reading the file
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
     * Locates a file by first checking the classpath, then the filesystem.
     * <p>
     * This method performs a two-step search:
     * </p>
     * <ol>
     *   <li>Attempts to find the file as a classpath resource using the class loader</li>
     *   <li>If not found on the classpath, constructs a File object from the filename</li>
     * </ol>
     * <p>
     * This is useful for loading configuration files or other resources that may be
     * packaged within a JAR file or located on the filesystem.
     * </p>
     * <p>
     * <b>Note:</b> This method does not verify that the file exists. It only constructs
     * a File object that represents the potential location of the file.
     * </p>
     *
     * @param filename the name or path of the file to locate, must not be {@code null}
     * @return a File object representing the file location (from classpath or filesystem)
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
