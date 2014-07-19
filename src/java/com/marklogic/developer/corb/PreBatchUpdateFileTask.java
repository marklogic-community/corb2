package com.marklogic.developer.corb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.marklogic.xcc.ResultSequence;

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
	
	protected void writeToFile(ResultSequence seq) throws IOException{
		String topContent = getTopContent();
		topContent = topContent != null ? topContent.trim() : "";
		
		if(topContent.length() == 0 && (seq == null || !seq.hasNext())) return;
		
		BufferedOutputStream writer = null;
		try{
			writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getFileName())));
			//write top content
			if(topContent.length() > 0){
				writer.write(topContent.getBytes());
				writer.write(NEWLINE);	
			}
			while(seq != null && seq.hasNext()){
				writer.write(getValueAsBytes(seq.next().getItem()));
				writer.write(NEWLINE);
			}
		}finally{
			if(writer != null){
				writer.close();
			}
		}
	}
}
