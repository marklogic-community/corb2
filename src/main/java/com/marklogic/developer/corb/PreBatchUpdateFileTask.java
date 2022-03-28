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

import static com.marklogic.developer.corb.Options.EXPORT_FILE_HEADER_LINE_COUNT;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_TOP_CONTENT;
import com.marklogic.developer.corb.util.FileUtils;

import java.io.*;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PreBatchUpdateFileTask extends ExportBatchToFileTask {

	protected String getTopContent() {
		String topContent = getProperty(EXPORT_FILE_TOP_CONTENT);
		String batchRef = getProperty(Manager.URIS_BATCH_REF);
		if (topContent != null && batchRef != null) {
			topContent = topContent.replace('@' + Manager.URIS_BATCH_REF, batchRef);
		}
		return topContent;
	}

	private void deleteFileIfExists() throws IOException {
		File batchFile = getExportFile();
        FileUtils.deleteFile(batchFile);
	}

	protected void writeTopContent() throws IOException {
		String topContent = getTopContent();
		writeToExportFile(topContent);
	}

	private void addLineCountToProps() throws IOException{
		int ct = FileUtils.getLineCount(getExportFile());
		if (this.properties != null && ct > 0) {
			this.properties.setProperty(EXPORT_FILE_HEADER_LINE_COUNT, String.valueOf(ct));
		}
	}

	@Override
	public String[] call() throws Exception {
		try {
			deleteFileIfExists();
			writeTopContent();
			invokeModule();
			addLineCountToProps();
			return new String[0];
		} finally {
			cleanup();
		}
	}

}
