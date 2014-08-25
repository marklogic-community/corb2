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
		if(fileName == null || fileName.length() == 0){
			String batchRef = getProperty(Manager.URIS_BATCH_REF);
			if(batchRef != null && (batchRef=batchRef.trim()).length() > 0){
				fileName = batchRef.substring(batchRef.lastIndexOf('/')+1); 
			}
		}
		return fileName;
	}
	
	protected String getPartFileName(){
		String fileName = getFileName();
		if(fileName != null && fileName.length() > 0){
			String partExt = getProperty("EXPORT-FILE-PART-EXT");
			if(partExt != null && partExt.length() > 0){
				if(!partExt.startsWith(".")) partExt = "."+partExt;
				fileName = fileName+partExt;
			}			
		}
		return fileName;
	}
	
	protected void writeToFile(ResultSequence seq) throws IOException{
		if(seq == null || !seq.hasNext()) return;
		synchronized(sync){
			BufferedOutputStream writer = null;
			try{
				writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getPartFileName()),true));
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
