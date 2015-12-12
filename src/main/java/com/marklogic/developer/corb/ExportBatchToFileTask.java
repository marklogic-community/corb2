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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.marklogic.xcc.ResultSequence;

public class ExportBatchToFileTask extends ExportToFileTask {

	private static final Object SYNC_OBJ = new Object();

	@Override
	protected String getFileName() {
		String fileName = getProperty("EXPORT-FILE-NAME");
		if (fileName == null || fileName.length() == 0) {
			String batchRef = getProperty(Manager.URIS_BATCH_REF);
			if (batchRef != null && (batchRef = batchRef.trim()).length() > 0) {
				fileName = batchRef.substring(batchRef.lastIndexOf('/') + 1);
			}
		}
		if (fileName == null || fileName.length() == 0) {
			throw new NullPointerException("Missing EXPORT-FILE-NAME or URIS_BATCH_REF property");
		}
		return fileName;
	}

    protected String getPartFileName() {
        String fileName = getFileName();
        String partExt = getProperty("EXPORT-FILE-PART-EXT");
        if (partExt != null && partExt.length() > 0) {
            if (!partExt.startsWith(".")) {
                partExt = "." + partExt;
            }
            fileName += partExt;
        }
        return fileName;
    }

	@Override
	protected void writeToFile(ResultSequence seq) throws IOException {
		if (seq == null || !seq.hasNext()) {
			return;
		}
		synchronized (SYNC_OBJ) {
			BufferedOutputStream writer = null;
			try {
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, getPartFileName()), true));
				while (seq.hasNext()) {
					writer.write(getValueAsBytes(seq.next().getItem()));
					writer.write(NEWLINE);
				}
				writer.flush();
			} finally {
				if (writer != null) { writer.close(); }
			}
		}
	}
}
