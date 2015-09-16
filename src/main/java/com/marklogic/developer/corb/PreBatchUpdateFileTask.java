package com.marklogic.developer.corb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PreBatchUpdateFileTask extends ExportBatchToFileTask {
	protected String getTopContent(){
		String topContent = getProperty("EXPORT-FILE-TOP-CONTENT");
		String batchRef = getProperty(Manager.URIS_BATCH_REF);
		if(topContent != null && batchRef != null){
			topContent = topContent.replace("@"+Manager.URIS_BATCH_REF, batchRef);
		}
		return topContent;
	}
	
	private void deleteFileIfExists() throws IOException{
		File batchFile = new File(exportDir,getPartFileName());
		if(batchFile.exists()) batchFile.delete();
	}
	
	protected void writeTopContent() throws IOException{
		String topContent = getTopContent();
		topContent = topContent != null ? topContent.trim() : "";
		if(topContent.length() > 0){
			BufferedOutputStream writer = null;
			try{
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getPartFileName())));
				writer.write(topContent.getBytes());
				writer.write(NEWLINE);
				writer.flush();
			}finally{
				if(writer != null){
					writer.close();
				}
			}
		}
	}
	
	public String[] call() throws Exception {
		try{
			deleteFileIfExists();
			writeTopContent();
	    	invokeModule();
	    	return new String[0];
		}finally{
    		cleanup();
    	}
    }
}
