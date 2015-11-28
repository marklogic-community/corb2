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

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PreBatchUpdateFileTask extends ExportBatchToFileTask {

	protected String getTopContent() {
		String topContent = getProperty("EXPORT-FILE-TOP-CONTENT");
		String batchRef = getProperty(Manager.URIS_BATCH_REF);
		if (topContent != null && batchRef != null) {
			topContent = topContent.replace("@" + Manager.URIS_BATCH_REF, batchRef);
		}
		return topContent;
	}

	private void deleteFileIfExists() throws IOException {
		File batchFile = new File(exportDir, getPartFileName());
		if (batchFile.exists()) {
			batchFile.delete();
		}
	}

	protected void writeTopContent() throws IOException {
		String topContent = getTopContent();
		topContent = topContent != null ? topContent.trim() : "";
		if (topContent.length() > 0) {
			BufferedOutputStream writer = null;
			try {
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, getPartFileName())));
				writer.write(topContent.getBytes());
				writer.write(NEWLINE);
				writer.flush();
			} finally {
				if(writer != null) writer.close();
			}
		}
	}

	@Override
	public String[] call() throws Exception {
		try {
			deleteFileIfExists();
			writeTopContent();
			invokeModule();
			return new String[0];
		} finally {
			cleanup();
		}
	}
}
