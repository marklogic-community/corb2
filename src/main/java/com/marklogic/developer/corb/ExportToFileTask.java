package com.marklogic.developer.corb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.marklogic.xcc.ResultSequence;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class ExportToFileTask extends AbstractTask {
	protected String getFileName() {
		return inputUris[0].charAt(0) == '/' ? inputUris[0].substring(1) : inputUris[0];
	}

	protected void writeToFile(ResultSequence seq) throws IOException {
		if (seq == null || !seq.hasNext()) {
			return;
		}
		BufferedOutputStream writer = null;
		try {
			File f = new File(exportDir, getFileName());
			f.getParentFile().mkdirs();
			writer = new BufferedOutputStream(new FileOutputStream(f));
			while (seq.hasNext()) {
				writer.write(getValueAsBytes(seq.next().getItem()));
				writer.write(NEWLINE);
			}
			writer.flush();
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
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

	@Override
	protected void cleanup() {
		super.cleanup();
		exportDir = null;
	}

	@Override
	public String[] call() throws Exception {
		try {
			return invokeModule();
		} finally {
			cleanup();
		}
	}
}
