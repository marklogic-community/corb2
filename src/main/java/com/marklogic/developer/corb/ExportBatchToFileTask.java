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
		if (fileName != null && fileName.length() > 0) {
			String partExt = getProperty("EXPORT-FILE-PART-EXT");
			if (partExt != null && partExt.length() > 0) {
				if (!partExt.startsWith(".")) {
					partExt = "." + partExt;
				}
				fileName = fileName + partExt;
			}
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
				if (writer != null) writer.close();
			}
		}
	}
}
