package com.marklogic.developer.corb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.marklogic.xcc.ResultSequence;

/**
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 */
public class PostBatchUpdateFileTask extends ExportBatchToFileTask {
	
	protected String getBottomContent(){
		return getProperty("EXPORT-FILE-BOTTOM-CONTENT");
	}
	
	protected void writeBottomContent() throws IOException{
		String bottomContent = getBottomContent();
		bottomContent = bottomContent != null ? bottomContent.trim() : "";
		if(bottomContent.length() > 0){
			BufferedOutputStream writer = null;
			try{
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getPartFileName()),true));
				if(bottomContent.length() > 0){
					writer.write(bottomContent.getBytes());
					writer.write(NEWLINE);	
				}
				writer.flush();
			}finally{
				if(writer != null){
					writer.close();
				}
			}
		}
	}
	
	protected void moveFile() throws IOException{
		String partFileName = getPartFileName();
		String finalFileName = getFileName();
		if(!partFileName.equals(finalFileName)){
			File partFile = new File(exportDir,partFileName);
			if(partFile.exists()){
				File finalFile = new File(exportDir,finalFileName);
				if(finalFile.exists()){
					finalFile.delete();				
				}
				partFile.renameTo(finalFile);
			}
		}
	}
	
	public String call() throws Exception {
    	invokeModule();
		writeBottomContent();
		moveFile();
    	return TRUE;
    }
}
