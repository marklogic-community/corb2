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
		return this.exportDir;
	}
	
	protected String getFileName(){
		return inputUri.charAt(0) == '/' ? inputUri.substring(1)  : inputUri;
	}
	
	protected void writeToFile(ResultSequence seq) throws IOException{
		if(seq == null || !seq.hasNext()) return;
		BufferedOutputStream writer = null;
		try{
			File f = new File(exportDir,getFileName());
			f.getParentFile().mkdirs();
			writer = new BufferedOutputStream(new FileOutputStream(f));
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
	
	protected void cleanup(){
		super.cleanup();
		exportDir=null;
	}
	
    public String call() throws Exception {
    	try{
    		return invokeModule();
    	}finally{
    		cleanup();
    	}
    }
}
