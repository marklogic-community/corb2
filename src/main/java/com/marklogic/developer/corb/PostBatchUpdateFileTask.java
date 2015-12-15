/*
 * Copyright 2005-2015 MarkLogic Corporation
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

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PostBatchUpdateFileTask extends ExportBatchToFileTask {
    public static final String DISTINCT_FILE_SUFFIX = ".distinct";
    protected static final String SORT_DIRECTION = "(?i)^(a|de)sc.*";
    protected static final String DESCENDING = "(?i)^desc.*";
    protected static final String DISTINCT = "(?i).*\\|(distinct|uniq).*";
    private static final Logger LOG = Logger.getLogger(PostBatchUpdateFileTask.class.getName());
    
    protected String getBottomContent() {
        return getProperty("EXPORT-FILE-BOTTOM-CONTENT");
    }

    protected void writeBottomContent() throws IOException {
        String bottomContent = getBottomContent();
        bottomContent = bottomContent != null ? bottomContent.trim() : "";
        if (bottomContent.length() > 0) {
            BufferedOutputStream writer = null;
            try {
                writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, getPartFileName()), true));
                writer.write(bottomContent.getBytes());
                writer.write(NEWLINE);
                writer.flush();
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    protected void moveFile(File source, File dest) {
        if (!source.getAbsolutePath().equals(dest.getAbsolutePath())) {
            if (source.exists()) {
                if (dest.exists()) {
                    dest.delete();
                }
                source.renameTo(dest);
            }
        }
    }
    
    protected void moveFile(String source, String dest) throws IOException {
        File srcFile = new File(exportDir, source);
        File destFile = new File(exportDir, dest);
        moveFile(srcFile, destFile);
    }

    protected void moveFile() throws IOException {
        String partFileName = getPartFileName();
        String finalFileName = getFileName();
        moveFile(partFileName, finalFileName);
    }

    protected void compressFile() throws IOException {
        if ("true".equalsIgnoreCase(getProperty("EXPORT_FILE_AS_ZIP"))) {
            String outFileName = getFileName();
            String outZipFileName = outFileName + ".zip";
            String partExt = getPartExtension();
            String partZipFileName = outZipFileName + partExt;

            File outFile = new File(exportDir, outFileName);
            File zipFile = new File(exportDir, partZipFileName);

            ZipOutputStream zos = null;
            FileOutputStream fos = null;
            
            try {
                if (outFile.exists()) {
                    if (zipFile.exists()) {
                        zipFile.delete();
                    }

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
                        if (fis != null) {
                            fis.close();
                        }
                    }
                    zos.closeEntry();
                    zos.flush();
                }
            } finally {
                if (zos != null) {
                    zos.close();
                }
             
                if (fos != null) {
                    fos.close();
                }
            }
            // move the file if required
            moveFile(partZipFileName, outZipFileName);

            // now that we have everything, delete the uncompressed output file
            if (outFile.exists()) {
                outFile.delete();
            }
        }
    }

    private void sortAndRemoveDuplicates() throws IOException {
        String sort = getProperty("EXPORT-FILE-SORT");

        if (sort == null || !sort.matches(SORT_DIRECTION)) {
            return;
        }

        File partFile = new File(exportDir, getFileName());
        if (!partFile.exists()) {
            return;
        }
        
        int headerLineCount = getIntProperty("EXPORT-FILE-HEADER-LINE-COUNT");
        if (headerLineCount < 0) {
            headerLineCount = 0;
        }
        
        File sortedFile = new File(exportDir, getPartFileName());
        File tempFileStore = partFile.getParentFile();
        Comparator comparator = ExternalSort.defaultcomparator;
        if (sort.matches(DESCENDING)) {
            comparator = Collections.reverseOrder();
        }
        boolean distinct = sort.matches(DISTINCT);
        
        Charset charset = Charset.defaultCharset();
        boolean useGzip = false;
        
        List<File> fragments = ExternalSort.sortInBatch(partFile, comparator, ExternalSort.DEFAULTMAXTEMPFILES, charset, tempFileStore, distinct, headerLineCount, useGzip);
        LOG.log(Level.INFO, "Created {0} temp files for sort and dedup", fragments.size());
          
        copyHeaderIntoFile(partFile, headerLineCount, sortedFile);
        boolean append = true;
        ExternalSort.mergeSortedFiles(fragments, sortedFile, comparator, charset, distinct, append, useGzip);
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
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        }
    }
    
    @Override
    public String[] call() throws Exception {
        try {
            invokeModule();
            sortAndRemoveDuplicates();
            writeBottomContent();
            moveFile();
            compressFile();
            return new String[0];
        } finally {
            cleanup();
        }
    }
}
