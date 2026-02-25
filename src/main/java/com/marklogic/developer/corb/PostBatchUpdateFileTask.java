/*
 * Copyright (c) 2004-2023 MarkLogic Corporation
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

import com.google.code.externalsorting.ExternalSort;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_AS_ZIP;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_BOTTOM_CONTENT;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_HEADER_LINE_COUNT;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_PART_EXT;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_SORT;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_SORT_COMPARATOR;
import com.marklogic.developer.corb.util.FileUtils;
import static com.marklogic.developer.corb.util.FileUtils.deleteFile;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Post-batch task for finalizing and processing export files after all batch processing completes.
 * <p>
 * This task extends {@link ExportBatchToFileTask} and provides post-processing capabilities including:
 * </p>
 * <ul>
 * <li>Sorting export file contents (ascending or descending)</li>
 * <li>Removing duplicate lines from the export file</li>
 * <li>Appending footer/bottom content to the export file</li>
 * <li>Renaming temporary files to their final names</li>
 * <li>Compressing export files to ZIP format</li>
 * </ul>
 * <p>
 * The task is designed to run after all process tasks have completed and typically operates
 * on a single consolidated export file that was written by multiple {@link ExportBatchToFileTask}
 * instances during batch processing.
 * </p>
 * <h2>Sorting and Deduplication</h2>
 * <p>
 * The task supports external sorting via the {@link ExternalSort} library, which allows sorting
 * of files larger than available memory. Sorting behavior is controlled by:
 * </p>
 * <ul>
 * <li>{@link Options#EXPORT_FILE_SORT} - Sort direction ("asc", "desc") or pattern ("distinct", "uniq")</li>
 * <li>{@link Options#EXPORT_FILE_SORT_COMPARATOR} - Custom comparator class for sorting</li>
 * <li>{@link Options#EXPORT_FILE_HEADER_LINE_COUNT} - Number of header lines to preserve (not sorted)</li>
 * </ul>
 * <h2>File Finalization</h2>
 * <p>
 * Export files are initially written with a temporary extension (e.g., ".part") to indicate
 * they are in progress. This task removes the temporary extension to mark the file as complete.
 * </p>
 * <h2>Compression</h2>
 * <p>
 * If {@link Options#EXPORT_FILE_AS_ZIP} is enabled, the export file is compressed to a ZIP
 * archive and the original uncompressed file is deleted.
 * </p>
 * <h3>Configuration Properties</h3>
 * <ul>
 * <li>{@link Options#EXPORT_FILE_SORT} - Sort direction or pattern</li>
 * <li>{@link Options#EXPORT_FILE_SORT_COMPARATOR} - Custom comparator class name</li>
 * <li>{@link Options#EXPORT_FILE_HEADER_LINE_COUNT} - Header lines to preserve</li>
 * <li>{@link Options#EXPORT_FILE_BOTTOM_CONTENT} - Footer content to append</li>
 * <li>{@link Options#EXPORT_FILE_PART_EXT} - Temporary file extension</li>
 * <li>{@link Options#EXPORT_FILE_AS_ZIP} - Enable ZIP compression</li>
 * </ul>
 *
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @see ExportBatchToFileTask
 * @see PreBatchUpdateFileTask
 * @see com.google.code.externalsorting.ExternalSort
 */
public class PostBatchUpdateFileTask extends ExportBatchToFileTask {
    /**
     * File suffix used for distinct/deduplicated file operations.
     * This constant is available for use by subclasses or related utilities.
     */
    public static final String DISTINCT_FILE_SUFFIX = ".distinct";
    /**
     * Pattern to match valid sort direction strings.
     * Matches strings starting with "asc" or "desc" (case-insensitive).
     * Examples: "asc", "ascending", "desc", "descending"
     */
    protected static final Pattern SORT_DIRECTION_PATTERN = Pattern.compile("(?i)^(a|de)sc.*");
    /**
     * Pattern to match descending sort direction strings.
     * Matches strings starting with "desc" (case-insensitive).
     * Examples: "desc", "descending", "DESC"
     */
    protected static final Pattern DESCENDING_PATTERN = Pattern.compile("(?i)^desc.*");
    /**
     * Pattern to match distinct/unique operation strings.
     * Matches strings containing "distinct" or "uniq" (case-insensitive).
     * Examples: "distinct", "asc-distinct", "desc-unique", "uniq"
     */
    protected static final Pattern DISTINCT_PATTERN = Pattern.compile("(?i).*(distinct|uniq).*");
    /**
     * Logger for this class.
     * Used to log sorting progress, file operations, and any errors during post-batch processing.
     */
    private static final Logger LOG = Logger.getLogger(PostBatchUpdateFileTask.class.getName());

    /**
     * Sorts and removes duplicate lines from the export file.
     * <p>
     * This is a convenience method that operates on the default export file.
     * </p>
     *
     * @see #sortAndRemoveDuplicates(File)
     */
    protected void sortAndRemoveDuplicates() {
        File origFile = getExportFile();
        sortAndRemoveDuplicates(origFile);
    }

    /**
     * Sorts and optionally removes duplicate lines from the specified file.
     * <p>
     * The sorting process:
     * </p>
     * <ol>
     * <li>Checks if sorting is requested via {@link Options#EXPORT_FILE_SORT} or a custom comparator</li>
     * <li>Splits the file into sorted chunks (using external sort algorithm)</li>
     * <li>Merges the sorted chunks back into a single file</li>
     * <li>Optionally removes duplicate lines during the merge</li>
     * <li>Preserves header lines (configured via {@link Options#EXPORT_FILE_HEADER_LINE_COUNT})</li>
     * </ol>
     * <p>
     * Uses external sorting to handle files larger than available memory.
     * Temporary chunk files are created in the same directory as the original file.
     * </p>
     * <p>
     * If sorting is not configured or if an error occurs, the file is left unchanged
     * and a warning is logged.
     * </p>
     *
     * @param origFile the file to sort and deduplicate
     */
    protected void sortAndRemoveDuplicates(File origFile) {
        if (!origFile.exists()) {
            return;
        }

        try {
            String sort = getProperty(EXPORT_FILE_SORT);
            String comparatorCls = getProperty(EXPORT_FILE_SORT_COMPARATOR);

            //You must either specify asc/desc or provide your own comparator
            if ((sort == null || !SORT_DIRECTION_PATTERN.matcher(sort).matches()) && isBlank(comparatorCls)) {
                return;
            }

            int headerLineCount = getIntProperty(EXPORT_FILE_HEADER_LINE_COUNT);
            if (headerLineCount < 0) {
                headerLineCount = 0;
            }

            File sortedFile = getExportFile(getPartFileName() + getPartExt());
            File tempFileStore = origFile.getParentFile();

            Comparator<String> comparator = ExternalSort.defaultcomparator;
            if (isNotBlank(comparatorCls)) {
                comparator = getComparatorCls(comparatorCls).newInstance();
            } else if (DESCENDING_PATTERN.matcher(sort).matches()) {
                comparator = Collections.reverseOrder();
            }

            boolean distinct = !isBlank(sort) && DISTINCT_PATTERN.matcher(sort).matches();

            Charset charset = Charset.defaultCharset();
            boolean useGzip = false;

            List<File> fragments = ExternalSort.sortInBatch(origFile, comparator, ExternalSort.DEFAULTMAXTEMPFILES, charset, tempFileStore, distinct, headerLineCount, useGzip);
            LOG.log(INFO, () -> MessageFormat.format("Created {0} temp files for sort and dedup", fragments.size()));

            copyHeaderIntoFile(origFile, headerLineCount, sortedFile);
            boolean append = true;
            ExternalSort.mergeSortedFiles(fragments, sortedFile, comparator, charset, distinct, append, useGzip);

            FileUtils.moveFile(sortedFile, origFile);
        } catch (Exception exc) {
            LOG.log(WARNING, "Unexpected error while sorting the report file " + origFile.getPath() + ". The file can still be sorted locally after the job is finished.", exc);
        }
    }

    /**
     * Loads and validates a custom Comparator class.
     * <p>
     * The class must implement {@link Comparator} and have a no-argument constructor.
     * </p>
     *
     * @param className fully qualified class name of the Comparator
     * @return the Comparator class
     * @throws ClassNotFoundException if the class cannot be found
     * @throws InstantiationException if the class cannot be instantiated
     * @throws IllegalAccessException if the constructor is not accessible
     * @throws IllegalArgumentException if the class is not a Comparator
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends Comparator<String>> getComparatorCls(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> cls = Class.forName(className);
        if (Comparator.class.isAssignableFrom(cls)) {
            cls.newInstance(); // sanity check
            return (Class<? extends Comparator<String>>) cls.asSubclass(Comparator.class);
        } else {
            throw new IllegalArgumentException("Comparator must be of type java.util.Comparator");
        }
    }

    /**
     * Copies header lines from the input file to the output file.
     * <p>
     * This is used during sorting to preserve header lines that should not be sorted
     * with the data lines.
     * </p>
     *
     * @param inputFile the file to read header lines from
     * @param headerLineCount the number of header lines to copy
     * @param outputFile the file to write header lines to
     * @throws IOException if an I/O error occurs
     */
    protected void copyHeaderIntoFile(File inputFile, int headerLineCount, File outputFile) throws IOException {

        try (BufferedReader reader = Files.newBufferedReader(inputFile.toPath());
             BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath()) ) {
            String line;
            int currentLine = 0;
            while ((line = reader.readLine()) != null && currentLine < headerLineCount) {
                writer.write(line);
                writer.newLine();
                currentLine++;
            }
            writer.flush();
        }
    }

    /**
     * Retrieves the bottom/footer content to append to the export file.
     * <p>
     * The content is specified via {@link Options#EXPORT_FILE_BOTTOM_CONTENT}.
     * </p>
     *
     * @return the bottom content string, or null if not configured
     */
    protected String getBottomContent() {
        return getProperty(EXPORT_FILE_BOTTOM_CONTENT);
    }

    /**
     * Appends bottom/footer content to the export file.
     * <p>
     * The content is retrieved via {@link #getBottomContent()} and appended
     * using {@link #writeToExportFile(String)}.
     * </p>
     *
     * @throws IOException if an I/O error occurs while writing
     */
    protected void writeBottomContent() throws IOException {
        String bottomContent = getBottomContent();
        writeToExportFile(bottomContent);
    }

    /**
     * Moves the temporary export file to its final name.
     * <p>
     * Removes the temporary file extension (e.g., ".part") from the export file name.
     * This is typically done after all processing is complete to indicate the file
     * is finalized and ready for use.
     * </p>
     */
    protected void moveFile() {
        moveFile(getPartFileName(), getFileName());
    }

    /**
     * Moves a file from source filename to destination filename.
     * <p>
     * Both filenames are resolved relative to the export file directory.
     * </p>
     *
     * @param srcFilename the source filename (without directory)
     * @param destFilename the destination filename (without directory)
     */
    protected void moveFile(String srcFilename, String destFilename) {
        File srcFile =  getExportFile(srcFilename);
        File destFile = getExportFile(destFilename);
        FileUtils.moveFile(srcFile, destFile);
    }

    /**
     * Gets the temporary file extension for in-progress export files.
     * <p>
     * The extension is configured via {@link Options#EXPORT_FILE_PART_EXT}.
     * If not configured, defaults to ".part". Ensures the extension starts with a dot.
     * </p>
     *
     * @return the part file extension (including leading dot)
     */
    protected String getPartExt() {
        String partExt = getProperty(EXPORT_FILE_PART_EXT);
        if (isEmpty(partExt)) {
            partExt = ".part";
        } else if (!partExt.startsWith(".")) {
            partExt = '.' + partExt;
        }
        return partExt;
    }

    /**
     * Compresses the export file to ZIP format if configured.
     * <p>
     * The compression process:
     * </p>
     * <ol>
     * <li>Checks if compression is enabled via {@link Options#EXPORT_FILE_AS_ZIP}</li>
     * <li>Creates a ZIP file with a temporary extension</li>
     * <li>Adds the export file to the ZIP archive</li>
     * <li>Moves the ZIP file to its final name (removing temp extension)</li>
     * <li>Deletes the original uncompressed file</li>
     * </ol>
     * <p>
     * If compression is not enabled or if the export file doesn't exist,
     * this method does nothing.
     * </p>
     *
     * @throws IOException if an I/O error occurs during compression
     */
    protected void compressFile() throws IOException {
        if ("true".equalsIgnoreCase(getProperty(EXPORT_FILE_AS_ZIP))) {
            String outFileName = getFileName();
            String outZipFileName = outFileName + ".zip";
            String partExt = getPartExt();
            String partZipFileName = outZipFileName + partExt;

            File outFile = getExportFile(outFileName);
            File zipFile = getExportFile(partZipFileName);

            if (outFile.exists()) {
                deleteFile(zipFile);

                try (FileOutputStream fos = new FileOutputStream(zipFile);
                        ZipOutputStream zos = new ZipOutputStream(fos)) {
                    ZipEntry ze = new ZipEntry(outFileName);
                    zos.putNextEntry(ze);
                    byte[] buffer = new byte[2048];
                    try (FileInputStream fis = new FileInputStream(outFile)) {
                        int len;
                        while ((len = fis.read(buffer)) > 0) {
                            zos.write(buffer, 0, len);
                        }
                    }
                    zos.closeEntry();
                    zos.flush();
                }
            }

            // move the file if required
            moveFile(partZipFileName, outZipFileName);

            // now that we have everything, delete the uncompressed output file
            deleteFile(outFile);
        }
    }

    /**
     * Indicates whether a PROCESS-MODULE is required for this task.
     * <p>
     * Returns false because this is a post-batch task that operates on the
     * final export file, not on individual process results.
     * </p>
     *
     * @return false (PROCESS-MODULE is not required)
     */
    @Override
    protected boolean shouldHaveProcessModule(){
        return false;
    }

    /**
     * Executes the post-batch file processing operations.
     * <p>
     * This method performs the following operations in sequence:
     * </p>
     * <ol>
     * <li>Sort and remove duplicates from the export file (if configured)</li>
     * <li>Invoke the POST-BATCH-MODULE (if configured)</li>
     * <li>Append bottom/footer content (if configured)</li>
     * <li>Remove temporary file extension (finalize filename)</li>
     * <li>Compress to ZIP format (if configured)</li>
     * <li>Clean up temporary resources</li>
     * </ol>
     * <p>
     * All operations are optional and controlled by configuration properties.
     * If an error occurs during any step, subsequent steps may not execute.
     * </p>
     *
     * @return an empty string array (task result)
     * @throws Exception if an error occurs during processing
     */
    @Override
    public String[] call() throws Exception {
        try {
          	sortAndRemoveDuplicates();
            invokeModule();
            writeBottomContent();
            moveFile();
            compressFile();
            return new String[0];
        } finally {
            cleanup();
        }
    }
}
