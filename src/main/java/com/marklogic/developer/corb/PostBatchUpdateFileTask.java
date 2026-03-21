/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.NumberUtils;

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.util.FileUtils.deleteFile;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.ArrayList;
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

    private int currentFileIndex = 1;
    private long currentFileLineCount = 0;
    private long currentFileSize = 0;
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
     * Appends bottom/footer content to the export file(s).
     * <p>
     * The content is retrieved via {@link #getBottomContent()} and handling depends
     * on whether file splitting is enabled:
     * </p>
     * <ul>
     * <li>If file splitting is enabled (via {@link Options#EXPORT_FILE_SPLIT_MAX_LINES}
     *     or {@link Options#EXPORT_FILE_SPLIT_MAX_SIZE}), delegates to
     *     {@link #writeBottomContentWithSplitting(String)} to append content to
     *     each split file appropriately</li>
     * <li>If file splitting is disabled, directly appends content to the single
     *     export file using {@code appendToFile(String, File)}</li>
     * </ul>
     * <p>
     * If the bottom content is null or empty (i.e., {@link Options#EXPORT_FILE_BOTTOM_CONTENT}
     * is not configured), this method completes without modifying any files.
     * </p>
     *
     * @throws IOException if an I/O error occurs while writing to the export file(s)
     * @see #getBottomContent()
     * @see #writeBottomContentWithSplitting(String)
     * @see #shouldSplitFiles()
     */
    protected void writeBottomContent() throws IOException {
        String bottomContent = getBottomContent();
        if (shouldSplitFiles()) {
            writeBottomContentWithSplitting(bottomContent);
        } else {
            writeToExportFile(bottomContent);
        }
    }

    /**
     * Moves the temporary export file to its final name.
     * <p>
     * Removes the temporary file extension (e.g., ".part") from the export file name.
     * This is typically done after all processing is complete to indicate the file
     * is finalized and ready for use.
     * </p>
     * @throws IOException if an I/O error occurs
     */
    protected void moveFile() throws IOException {

        if (shouldSplitFiles()) {
            File exportFile = getExportFile();
            String partExt = getPartExt();
            for (File splitFile : getSplitFiles(exportFile)) {
                String splitFileName = splitFile.getName();
                String baseFileName = splitFileName.substring(0, splitFileName.lastIndexOf(partExt));
                File splitFileWithoutPartExt = new File(splitFile.getParent(), baseFileName);
                FileUtils.moveFile(splitFile, splitFileWithoutPartExt);
            }
            deleteFile(exportFile);
        } else {
            //No need to split, just rename
            moveFile(getPartFileName(), getFileName());
        }
    }

    protected void writeBottomContentWithSplitting(String bottomContent) throws IOException {
        File exportFile = getExportFile();
        writeBottomContentWithSplitting(exportFile, bottomContent, getMaxLines(), getMaxSize());
    }

    /**
     * Writes the bottom content to the output file, splitting files as needed
     * based upon line count or file size thresholds.
     *
     * @param batchFile the batch file to split
     * @param maxLines maximum lines per file (0 or negative to disable)
     * @param maxSize maximum bytes per file (0 or negative to disable)
     * @throws IOException if an I/O error occurs
     */
    protected void writeBottomContentWithSplitting(File batchFile, String bottomContent, long maxLines, long maxSize) throws IOException {
        IOException pendingException = null;
        OutputStream writer = null;
        try (BufferedReader reader = Files.newBufferedReader(batchFile.toPath())) {
            File splitFile = getCurrentSplitFile();
            int headerLineCount = getIntProperty(EXPORT_FILE_HEADER_LINE_COUNT);
            if (headerLineCount > 0) {
                copyHeaderIntoFile(batchFile, headerLineCount, splitFile);
                //skip over headerLineCount lines in the reader
                while (currentFileLineCount < headerLineCount && reader.readLine() != null) { currentFileLineCount++; }
                //reset line count after reading in the header, so that it's not counted for splits
                currentFileLineCount = 0;
            }
            writer = new BufferedOutputStream(new FileOutputStream(splitFile, true));

            String line;
            while ((line = reader.readLine()) != null) {
                // Check if we need to rotate to a new file
                boolean needsRotation = maxLines > 0
                    ? currentFileLineCount >= maxLines
                    : maxSize > 0 && currentFileSize >= maxSize;

                if (needsRotation) {
                    if (isNotEmpty(bottomContent)) {
                        writer.write(bottomContent.getBytes(StandardCharsets.UTF_8));
                        writer.write(NEWLINE);
                    }
                    writer.flush();
                    writer.close();

                    currentFileIndex++;
                    currentFileLineCount = 0;
                    currentFileSize = 0;
                    File nextFile = getCurrentSplitFile();

                    if (headerLineCount > 0) {
                        copyHeaderIntoFile(batchFile, headerLineCount, nextFile);
                    }
                    writer = new BufferedOutputStream(new FileOutputStream(nextFile, true));
                }
                byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
                writer.write(lineBytes);
                writer.write(NEWLINE);

                currentFileLineCount++;
                currentFileSize += lineBytes.length + NEWLINE.length;
            }
            if (isNotEmpty(bottomContent)) {
                writer.write(bottomContent.getBytes(StandardCharsets.UTF_8));
                writer.write(NEWLINE);
            }
            writer.flush();
        } catch (IOException ex) {
            pendingException = ex;
            throw ex;
        } finally {
            if (writer != null) {
                if (pendingException != null) {
                    try {
                        writer.close();
                    } catch (IOException closeException) {
                        pendingException.addSuppressed(closeException);
                    }
                } else {
                    writer.close();
                }
            }
        }
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
            List<File> splitFiles = getSplitFiles(outFile);
            File zipFile = getExportFile(partZipFileName);

            if (outFile.exists() || !splitFiles.isEmpty()) {

                deleteFile(zipFile);

                try (FileOutputStream fos = new FileOutputStream(zipFile);
                     ZipOutputStream zos = new ZipOutputStream(fos)) {
                    if (shouldSplitFiles()) {
                        for (File splitFile : splitFiles) {
                            addZipEntry(zos, splitFile);
                            deleteFile(splitFile);
                        }
                    } else {
                        addZipEntry(zos, outFile);
                    }
                }
                // move the zip file, if required
                moveFile(partZipFileName, outZipFileName);
                // now that we have everything, delete the uncompressed output file
                deleteFile(outFile);
            }
        }
    }

    /**
     * Compresses a single export file into a ZIP archive.
     * <p>
     * This method performs the following operations:
     * </p>
     * <ol>
     * <li>Creates a temporary ZIP file (with {@link #getPartExt()} extension)</li>
     * <li>Adds the export file to the ZIP archive using its canonical path as the entry name</li>
     * <li>Moves the temporary ZIP file to its final name (removing temp extension)</li>
     * <li>Deletes the original uncompressed export file</li>
     * </ol>
     * <p>
     * The ZIP file is created with the same base name as the export file, plus a ".zip" extension.
     * If the export file does not exist, this method does nothing.
     * </p>
     * <p>
     * This method is called by {@link #compressFile()} for each split file when ZIP compression
     * is enabled via {@link Options#EXPORT_FILE_AS_ZIP}.
     * </p>
     *
     * @param zos the Zip File Output Stream to add the file entry to
     * @param outFile the export file to compress; must not be null
     * @throws IOException if an I/O error occurs during ZIP creation, file reading, or file deletion
     */
    protected void addZipEntry(ZipOutputStream zos, File outFile) throws IOException {
        if (outFile.exists()) {
            ZipEntry ze = new ZipEntry(outFile.getName());
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

    /**
     * Retrieves all split files associated with a base export file.
     * <p>
     * When large exports are split across multiple files, this method identifies and
     * returns all related file parts. The method searches for:
     * </p>
     * <ol>
     * <li>The base file itself (if it exists)</li>
     * <li>Numbered split files in sequential order (e.g., file.txt, file_1.txt, file_2.txt, etc.)</li>
     * </ol>
     * <p>
     * The search continues sequentially until a numbered file is not found, at which point
     * the search terminates. File numbering is expected to be consecutive starting from 1.
     * </p>
     * <p>
     * The returned list maintains the order of files, with the base file first (if present),
     * followed by numbered split files in ascending order. This method relies on
     * {@link #insertIndexIntoFileName(String, int)} to generate expected split file names.
     * </p>
     *
     * @param baseFile the base export file to search for splits; must not be null
     * @return a list of all existing split files (may be empty if no files exist);
     *         never returns null
     */
    protected List<File> getSplitFiles(File baseFile) {
        List<File> splitFiles = new ArrayList<>();
        // Find all numbered split files
        int index = 1;
        while (true) {
            String splitFileName = insertIndexIntoFileName(baseFile.getName(), index);
            File splitFile = new File(baseFile.getParent(), splitFileName);
            if (splitFile.exists()) {
                splitFiles.add(splitFile);
                index++;
            } else {
                break;
            }
        }

        return splitFiles;
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
     * Gets the maximum number of lines per file from configuration.
     *
     * @return maximum lines per file, or -1 if not configured
     */
    protected long getMaxLines() {
        String maxLinesStr = getProperty(EXPORT_FILE_SPLIT_MAX_LINES);
        if (isNotEmpty(maxLinesStr)) {
            try {
                return Long.parseLong(maxLinesStr.trim());
            } catch (NumberFormatException e) {
                LOG.warning("Invalid value for " + EXPORT_FILE_SPLIT_MAX_LINES + ": " + maxLinesStr);
            }
        }
        return -1;
    }

    /**
     * Gets the maximum file size in bytes from configuration.
     *
     * @return maximum file size in bytes, or -1 if not configured
     */
    protected long getMaxSize() {
        String maxSizeStr = getProperty(EXPORT_FILE_SPLIT_MAX_SIZE);
        if (isNotEmpty(maxSizeStr)) {
            try {
                return NumberUtils.parseSize(maxSizeStr);
            } catch (NumberFormatException e) {
                LOG.warning("Invalid value for " + EXPORT_FILE_SPLIT_MAX_SIZE + ": " + maxSizeStr);
            }
        }
        return -1;
    }

    /**
     * Gets the current split file based on the current file index.
     * <p>
     * This method retrieves the part file name and generates the corresponding split file
     * using the {@link #currentFileIndex}. The index is formatted and inserted into the
     * filename via {@link #insertIndexIntoFileName(String, int)}.
     * </p>
     *
     * @return the File object for the current split file
     * @see #getCurrentSplitFile(String, int)
     * @see #currentFileIndex
     */
    protected File getCurrentSplitFile() {
        String baseFileName = getPartFileName();
        return getCurrentSplitFile(baseFileName, currentFileIndex);
    }

    /**
     * Gets a split file based on the specified base filename and index.
     * <p>
     * This method generates a split filename by inserting the specified index into the
     * base filename (e.g., "file.txt" with index 1 becomes "001_file.txt"). The generated
     * filename is then resolved to a File object in the export directory.
     * </p>
     * <p>
     * This method is used when file splitting is enabled to create multiple output files
     * that meet size or line count constraints configured via {@link Options#EXPORT_FILE_SPLIT_MAX_SIZE}
     * or {@link Options#EXPORT_FILE_SPLIT_MAX_LINES}.
     * </p>
     *
     * @param baseFileName the base filename to insert the index into
     * @param index the numeric index to insert (formatted as 3 digits with leading zeros)
     * @return the File object for the split file in the export directory
     * @see #insertIndexIntoFileName(String, int)
     * @see #getExportFile(String)
     */
    protected File getCurrentSplitFile(String baseFileName, int index) {
        String splitFileName = insertIndexIntoFileName(baseFileName, index);
        return getExportFile(splitFileName);
    }

    /**
     * Inserts an index into a filename before the extension.
     * For example: "output.txt" with index 1 becomes "output1.txt"
     *
     * @param fileName the base filename
     * @param index the index to insert
     * @return the filename with the index inserted
     */
    protected String insertIndexIntoFileName(String fileName, int index) {
        String regex = "(?<path>.*[\\\\/])*(?<filename>.+?)$";
        String replacement = "${path}" + String.format("%03d", index) + "_${filename}";
        return fileName.replaceFirst(regex, replacement);
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
     * Checks if file splitting was enabled
     * @return true if splitting options were configured
     */
    protected boolean shouldSplitFiles() {
        return getMaxLines() > 0 || getMaxSize() > 0;
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
            moveFile(); //ensure that EXPORT-FILE-PART-EXT is removed
            compressFile();
            return new String[0];
        } finally {
            cleanup();
        }
    }
}
