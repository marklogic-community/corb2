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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PostBatchUpdateFileTask extends ExportBatchToFileTask {

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

    protected void moveFile(String source, String dest) throws IOException {
        if (!source.equals(dest)) {
            File srcFile = new File(exportDir, source);
            if (srcFile.exists()) {
                File destFile = new File(exportDir, dest);
                if (destFile.exists()) {
                    destFile.delete();
                }
                srcFile.renameTo(destFile);
            }
        }
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

    private void removeDuplicatesAndSort() throws IOException {
        String removeDuplicates = getProperty("EXPORT-FILE-REMOVE-DUPLICATES");
        if (removeDuplicates == null || !removeDuplicates.toLowerCase().startsWith("true")) {
            return;
        }

        String outFileName = getFileName();
        File outFile = new File(exportDir, outFileName);
        if (!outFile.exists()) {
            return;
        }

        Set<String> lines = null;
        if (removeDuplicates.toLowerCase().startsWith("true|sort")) {
            lines = new TreeSet<String>();
        } else if (removeDuplicates.toLowerCase().startsWith("true|order")) {
            lines = new LinkedHashSet<String>(10000);
        } else {
            lines = new HashSet<String>(10000);
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(outFile));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        String partExt = getPartExtension();
        String partFileName = outFileName + partExt;

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File(exportDir, partFileName)));
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
            writer.flush();
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        lines.clear();

        moveFile(partFileName, outFileName);
    }

    @Override
    public String[] call() throws Exception {
        try {
            invokeModule();
            removeDuplicatesAndSort();
            writeBottomContent();
            moveFile();
            compressFile();
            return new String[0];
        } finally {
            cleanup();
        }
    }
}
