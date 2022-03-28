/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PostBatchUpdateFileTask extends ExportBatchToFileTask {
    public static final String DISTINCT_FILE_SUFFIX = ".distinct";
    protected static final Pattern SORT_DIRECTION_PATTERN = Pattern.compile("(?i)^(a|de)sc.*");
    protected static final Pattern DESCENDING_PATTERN = Pattern.compile("(?i)^desc.*");
    protected static final Pattern DISTINCT_PATTERN = Pattern.compile("(?i).*(distinct|uniq).*");
    private static final Logger LOG = Logger.getLogger(PostBatchUpdateFileTask.class.getName());

    protected void sortAndRemoveDuplicates() {
        File origFile = getExportFile();
        sortAndRemoveDuplicates(origFile);
    }

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

    protected String getBottomContent() {
        return getProperty(EXPORT_FILE_BOTTOM_CONTENT);
    }

    protected void writeBottomContent() throws IOException {
        String bottomContent = getBottomContent();
        writeToExportFile(bottomContent);
    }

    protected void moveFile() {
        moveFile(getPartFileName(), getFileName());
    }

    protected void moveFile(String srcFilename, String destFilename) {
        File srcFile =  getExportFile(srcFilename);
        File destFile = getExportFile(destFilename);
        FileUtils.moveFile(srcFile, destFile);
    }

    protected String getPartExt() {
        String partExt = getProperty(EXPORT_FILE_PART_EXT);
        if (isEmpty(partExt)) {
            partExt = ".part";
        } else if (!partExt.startsWith(".")) {
            partExt = '.' + partExt;
        }
        return partExt;
    }

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
