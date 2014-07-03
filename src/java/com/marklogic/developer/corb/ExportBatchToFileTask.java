package com.marklogic.developer.corb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.marklogic.xcc.ResultSequence;

public class ExportBatchToFileTask extends ExportToFileTask {
	static private Object sync = new Object();
	
	protected String getFileName(){
		String fileName = getProperty("EXPORT-FILE-NAME");
		if(fileName == null || (fileName=fileName.trim()).length() == 0){
			String batchRef = getProperty(Manager.URIS_BATCH_REF);
			if(batchRef != null && (batchRef=batchRef.trim()).length() > 0){
				fileName = batchRef.substring(batchRef.lastIndexOf('/')+1); 
			}
		}
		if(fileName == null){
			fileName = inputUri.substring(inputUri.lastIndexOf('/')+1);
		}
		return fileName;
	}
	
	protected void writeToFile(String fileName, ResultSequence seq) throws IOException{
		if(seq == null || !seq.hasNext()) return;
		synchronized(sync){
			BufferedOutputStream writer = null;
			try{
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getFileName()),true));
				while(seq.hasNext()){
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
}
