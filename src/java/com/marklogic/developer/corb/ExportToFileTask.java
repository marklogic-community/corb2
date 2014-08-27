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
	protected String exportDir;
	
	public void setExportDir(String exportFileDir){
		this.exportDir = exportFileDir;
	}
	
	public String getExportDir(){
		return this.exportDir == null ? System.getProperty("user.dir") : this.exportDir;
	}
	
	protected String getFileName(){
		return inputUri.substring(inputUri.lastIndexOf('/')+1);
	}
	
	protected void writeToFile(ResultSequence seq) throws IOException{
		if(seq == null || !seq.hasNext()) return;
		BufferedOutputStream writer = null;
		try{
			writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getFileName())));
			while(seq.hasNext()){				
				writer.write(getValueAsBytes(seq.next().getItem()));
				writer.write(NEWLINE);
			}
			writer.flush();
		}finally{
			if(writer != null){
				writer.close();
			}
		}
	}
	
	protected String processResult(ResultSequence seq) throws CorbException{
		try{
			writeToFile(seq);
			return TRUE;
		}catch(IOException exc){
			throw new CorbException(exc.getMessage(),exc);
		}
	}
	
    public String call() throws Exception {
    	return invokeModule();
    }
}
