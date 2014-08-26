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
	
	protected void writeToFile(ResultSequence seq) throws IOException{
		String bottomContent = getBottomContent();
		bottomContent = bottomContent != null ? bottomContent.trim() : "";
				
		File batchFile = new File(exportDir,getPartFileName());
		if(bottomContent.length() > 0 || (seq != null && seq.hasNext())){
			BufferedOutputStream writer = null;
			try{
				writer = new BufferedOutputStream(new FileOutputStream(batchFile,true));
				while(seq != null && seq.hasNext()){
					writer.write(getValueAsBytes(seq.next().getItem()));
					writer.write(NEWLINE);
				}
				//write bottom content
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
		
		String finalFileName = getFileName();
		if(batchFile.exists() && !getPartFileName().equals(finalFileName)){
			File finalFile = new File(exportDir,finalFileName);
			if(finalFile.exists()) finalFile.delete();
			batchFile.renameTo(finalFile);
		}
	}
}
