/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

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

	protected void writeToFile(Iterator results) throws IOException {
		if (results == null || !results.hasNext()) {
			return;
		}

        File exportFile = getExportFile();
        exportFile.getParentFile().mkdirs();
        try (BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(exportFile))) {
            while (results.hasNext()) {
                writer.write(getValueAsBytes(results.next()));
                writer.write(NEWLINE);
            }
            writer.flush();
        }
	}

    protected void writeToExportFile(String content) throws IOException {
        String trimmedContent = trimToEmpty(content);
        if (isNotEmpty(trimmedContent)) {
            File exportFile = getExportFile();
            exportFile.getParentFile().mkdirs();
            try (BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(exportFile, true))) {
                writer.write(trimmedContent.getBytes());
                writer.write(NEWLINE);
            }
        }
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
        return new File(exportDir, fileName);
    }

	@Override
	protected String processResult(Iterator seq) throws CorbException {
		try {
			writeToFile(seq);
			return TRUE;
		} catch (IOException exc) {
			throw new CorbException(exc.getMessage(), exc);
		}
	}

}
