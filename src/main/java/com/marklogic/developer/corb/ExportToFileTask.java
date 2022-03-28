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

import static com.marklogic.developer.corb.Options.EXPORT_FILE_URI_TO_PATH;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trimToEmpty;

import com.marklogic.xcc.ResultSequence;

import java.io.*;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class ExportToFileTask extends AbstractTask {

	protected String getFileName() {
		return getExportFileName();
	}

    protected String getExportFileName() {
        String filename = inputUris[0].charAt(0) == '/' ? inputUris[0].substring(1) : inputUris[0];
        String uriInPath = getProperty(EXPORT_FILE_URI_TO_PATH);
        int lastIdx = filename.lastIndexOf('/');
        if ("false".equalsIgnoreCase(uriInPath) && lastIdx > 0 && filename.length() > (lastIdx + 1)) {
            filename = filename.substring(lastIdx + 1);
        }
        return filename;
    }

	protected void writeToFile(ResultSequence seq) throws IOException {
		if (seq == null || !seq.hasNext()) {
			return;
		}

        File exportFile = getExportFile();
        writeToFile(seq, exportFile);
	}

	protected void writeToFile(ResultSequence seq, File exportFile) throws IOException {
        try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(exportFile))) {
            write(seq, writer);
        }
    }

    protected void writeToExportFile(String content) throws IOException {
        String trimmedContent = trimToEmpty(content);
        if (isNotEmpty(trimmedContent)) {
            File exportFile = getExportFile();
            try (BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(exportFile, true))) {
                writer.write(trimmedContent.getBytes());
                writer.write(NEWLINE);
            }
        }
    }

    protected void write(ResultSequence seq, OutputStream writer) throws IOException {
        while (seq.hasNext()) {
            writer.write(getValueAsBytes(seq.next().getItem()));
            writer.write(NEWLINE);
        }
        writer.flush();
    }

    /**
     * Return a File with file name from getFileName()
     * @return
     */
    protected File getExportFile() {
        return getExportFile(getFileName());
    }

    /**
     * Return a File with file name specified, resolving to the exportDir, if configured.
     * Otherwise, it will be the present working directory of the job.
     * @return
     */
    protected File getExportFile(String fileName) {
        File exportFile = new File(exportDir, fileName);
        exportFile.getAbsoluteFile().getParentFile().mkdirs();
        return exportFile;
    }

	@Override
	protected String processResult(ResultSequence seq) throws CorbException {
		try {
			writeToFile(seq);
			return TRUE;
		} catch (IOException exc) {
			throw new CorbException(exc.getMessage(), exc);
		}
	}

}
