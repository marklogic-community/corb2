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

import static com.marklogic.developer.corb.Options.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_PART_EXT;
import static com.marklogic.developer.corb.Options.URIS_BATCH_REF;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.ResultSequence;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ExportBatchToFileTask extends ExportToFileTask {

	private static final Object SYNC_OBJ = new Object();

	@Override
	protected String getFileName() {
		return getExportBatchFileName();
	}

	protected String getExportBatchFileName() {
        String fileName = getProperty(EXPORT_FILE_NAME);
        if (isEmpty(fileName)) {
            String batchRef = trim(getProperty(Manager.URIS_BATCH_REF));
            if (isNotEmpty(batchRef)) {
                fileName = batchRef.substring(batchRef.lastIndexOf('/') + 1);
            }
        }
        if (isEmpty(fileName)) {
            throw new NullPointerException("Missing " + EXPORT_FILE_NAME + " or " + URIS_BATCH_REF + " property");
        }
        return fileName;
    }

    /**
     * Append a filename extension, if {@value com.marklogic.developer.corb.Options#EXPORT_FILE_PART_EXT} has been specified
     * and the fileName is not empty.
     * @return fileName with EXPORT_FILE_PART_EXT suffix appended
     */
	protected String getPartFileName() {
        String fileName = getFileName();
		if (isNotEmpty(fileName)) {
			String partExt = getProperty(EXPORT_FILE_PART_EXT);
			if (isNotEmpty(partExt)) {
				if (!partExt.startsWith(".")) {
					partExt = '.' + partExt;
				}
				fileName += partExt;
			}
		}
		return fileName;
	}

	@Override
    protected File getExportFile() {
        return getExportFile(getPartFileName());
    }

	@Override
	protected void writeToFile(ResultSequence seq, File exportFile) throws IOException {
		synchronized (SYNC_OBJ) {
			try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(exportFile, true))){
				write(seq, writer);
			}
		}
	}

}
