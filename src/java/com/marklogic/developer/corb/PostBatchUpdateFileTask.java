package com.marklogic.developer.corb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PostBatchUpdateFileTask extends ExportToFileTask {
	
	protected String getFileName(){
		String fileName = getProperty("EXPORT-FILE-NAME");
		if(fileName == null || (fileName=fileName.trim()).length() == 0){
			String batchRef = getProperty(Manager.URIS_BATCH_REF);
			if(batchRef != null && (batchRef=batchRef.trim()).length() > 0){
				fileName = batchRef.substring(batchRef.lastIndexOf('/')+1); 
			}
		}
		return fileName;
	}
	
	protected String getTopContent(){
		String topContent = getProperty("EXPORT-FILE-TOP-CONTENT");
		String batchRef = getProperty(Manager.URIS_BATCH_REF);
		if(topContent != null && batchRef != null){
			topContent = topContent.replace("@"+Manager.URIS_BATCH_REF, batchRef);
		}
		return topContent;
	}
	
	protected String getBottomContent(){
		return getProperty("EXPORT-FILE-BOTTOM-CONTENT");
	}
		
	protected void writeToFile(String fileName) throws IOException{		
		String topContent = getTopContent();
		topContent = topContent != null ? topContent.trim() : "";	
		
		String bottomContent = getBottomContent();
		bottomContent = bottomContent != null ? bottomContent.trim() : "";
		
		File exportFile = new File(exportDir,getFileName());
		if(exportFile.exists() && (topContent.length() > 0 || bottomContent.length() > 0)){
			File newFile = new File(exportDir,getFileName()+".new");
			
			InputStream in =  null;
			OutputStream out = null;
			try{
				try{
					in = new BufferedInputStream(new FileInputStream(exportFile));
					out = new BufferedOutputStream(new FileOutputStream(newFile,true));
					//write top content
					if(topContent.length() > 0){
						out.write(topContent.getBytes());
						out.write(NEWLINE);	
					}
					//copy the original file
				    final byte[] buffer = new byte[1024];
			        int n;
			        while ((n = in.read(buffer)) != -1){
			           out.write(buffer, 0, n);
			        }
			        //copy bottom content    
			        if(bottomContent.length() > 0){
			        	out.write(NEWLINE);
						out.write(bottomContent.getBytes());
			        }
				}finally{
					if(out != null){
						out.close();
					}
				}
			}finally{
				if(in != null){
					in.close();
				}
			}
			
			exportFile.delete();
			newFile.renameTo(exportFile);
		}		
	}
	
	@Override
	public String call() throws Exception {
		Thread.yield(); // try to avoid thread starvation
		writeToFile(getFileName());
		Thread.yield(); // try to avoid thread starvation
		invoke();
		Thread.yield(); // try to avoid thread starvation
		return TRUE;
	}
}
