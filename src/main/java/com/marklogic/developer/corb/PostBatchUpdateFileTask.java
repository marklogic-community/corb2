/*
 * Copyright (c) 2004-2015 MarkLogic Corporation
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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import com.google.code.externalsorting.ExternalSort;
import com.marklogic.developer.corb.util.FileUtils;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trimToEmpty;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PostBatchUpdateFileTask extends ExportBatchToFileTask {
    public static final String DISTINCT_FILE_SUFFIX = ".distinct";
    protected static final String SORT_DIRECTION = "(?i)^(a|de)sc.*";
    protected static final String DESCENDING = "(?i)^desc.*";
    protected static final String DISTINCT = "(?i).*(distinct|uniq).*";
    private static final Logger LOG = Logger.getLogger(PostBatchUpdateFileTask.class.getName());
     
    protected void sortAndRemoveDuplicates() {
        File origFile = new File(exportDir, getPartFileName());
        if (!origFile.exists()) {
            return;
        }

        try {
            String sort = getProperty("EXPORT-FILE-SORT");
            String comparatorCls = getProperty("EXPORT-FILE-SORT-COMPARATOR");

            //You must either specify asc/desc or provide your own comparator
            if ((sort == null || !sort.matches(SORT_DIRECTION)) && isBlank(comparatorCls)) {
                return;
            }

            int headerLineCount = getIntProperty("EXPORT-FILE-HEADER-LINE-COUNT");
            if (headerLineCount < 0) {
                headerLineCount = 0;
            }

            File sortedFile = new File(exportDir, getPartFileName() + getPartExt());
            File tempFileStore = origFile.getParentFile();

            Comparator<String> comparator = ExternalSort.defaultcomparator;
            if (isNotBlank(comparatorCls)) {
                comparator = getComparatorCls(comparatorCls).newInstance();
            } else if (sort.matches(DESCENDING)) {
                comparator = Collections.reverseOrder();
            }

            boolean distinct = isBlank(sort) ? false : sort.matches(DISTINCT);

            Charset charset = Charset.defaultCharset();
            boolean useGzip = false;

            List<File> fragments = ExternalSort.sortInBatch(origFile, comparator, ExternalSort.DEFAULTMAXTEMPFILES, charset, tempFileStore, distinct, headerLineCount, useGzip);
            LOG.log(Level.INFO, "Created {0} temp files for sort and dedup", fragments.size());

            copyHeaderIntoFile(origFile, headerLineCount, sortedFile);
            boolean append = true;
            ExternalSort.mergeSortedFiles(fragments, sortedFile, comparator, charset, distinct, append, useGzip);

            FileUtils.moveFile(sortedFile, origFile);
        } catch (Exception exc) {
            LOG.log(Level.WARNING, "Unexpected error while sorting the report file " + origFile.getPath() + ". The file can still be sorted locally after the job is finished.", exc);
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
        BufferedWriter writer = null;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            writer = new BufferedWriter(new FileWriter(outputFile, false));
            String line;
            int currentLine = 0;
            while ((line = reader.readLine()) != null && currentLine < headerLineCount) {
                writer.write(line);
                writer.newLine();
                currentLine++;
            }
            writer.flush();
            reader.close();
            writer.close();
        } finally {
            closeQuietly(reader);
            closeQuietly(writer);
        }
    }

    protected String getBottomContent() {
        return getProperty("EXPORT-FILE-BOTTOM-CONTENT");
    }

    protected void writeBottomContent() throws IOException {
        String bottomContent = getBottomContent();
        bottomContent = trimToEmpty(bottomContent);
        if (isNotEmpty(bottomContent)) {
            BufferedOutputStream writer = null;
            try {
                writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, getPartFileName()), true));
                writer.write(bottomContent.getBytes());
                writer.write(NEWLINE);
                writer.flush();
            } finally {
                closeQuietly(writer);
            }
        }
    }
    
    protected void moveFile(String srcFilename, String destFilename) throws IOException {
        File srcFile = new File(exportDir, srcFilename);
        File destFile = new File(exportDir, destFilename);
        FileUtils.moveFile(srcFile, destFile);
    }
    
    protected void moveFile() throws IOException {
        moveFile(getPartFileName(), getFileName());
    }
    
    protected String getPartExt() {
        String partExt = getProperty("EXPORT-FILE-PART-EXT");
        if (isEmpty(partExt)) {
            partExt = ".part";
        } else if (!partExt.startsWith(".")) {
            partExt = "." + partExt;
        }
        return partExt;
    }
       
    protected void compressFile() throws IOException {
        if ("true".equalsIgnoreCase(getProperty("EXPORT_FILE_AS_ZIP"))) {
            String outFileName = getFileName();
            String outZipFileName = outFileName + ".zip";
            String partExt = getPartExt();
            String partZipFileName = outZipFileName + partExt;

            File outFile = new File(exportDir, outFileName);
            File zipFile = new File(exportDir, partZipFileName);

            ZipOutputStream zos = null;
            FileOutputStream fos = null;
            
            try {
                if (outFile.exists()) {
                    FileUtils.deleteFile(zipFile);

                    fos = new FileOutputStream(zipFile);
                    zos = new ZipOutputStream(fos);

                    ZipEntry ze = new ZipEntry(outFileName);
                    zos.putNextEntry(ze);

                    byte[] buffer = new byte[2048];
                    FileInputStream fis = null; 
                    try {
                        fis = new FileInputStream(outFile);
                        int len;
                        while ((len = fis.read(buffer)) > 0) {
                            zos.write(buffer, 0, len);
                        }
                    } finally {
                        closeQuietly(fis);
                    }
                    zos.closeEntry();
                    zos.flush();
                }
            } finally {
                closeQuietly(zos);
                closeQuietly(fos);
            }
            // move the file if required
            moveFile(partZipFileName, outZipFileName);

            // now that we have everything, delete the uncompressed output file
            FileUtils.deleteFile(outFile);
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
