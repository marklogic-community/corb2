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
		
		if(bottomContent.length() == 0 && (seq == null || !seq.hasNext())) return;
		
		BufferedOutputStream writer = null;
		try{
			writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getFileName()),true));
			while(seq != null && seq.hasNext()){
				writer.write(getValueAsBytes(seq.next().getItem()));
				writer.write(NEWLINE);
			}
			//write bottom content
			if(bottomContent.length() > 0){
				writer.write(bottomContent.getBytes());
				writer.write(NEWLINE);	
			}
		}finally{
			if(writer != null){
				writer.close();
			}
		}
	}
}
