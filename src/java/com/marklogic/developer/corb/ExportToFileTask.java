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
	
	protected void writeToFile(String fileName, ResultSequence seq) throws IOException{
		if(seq == null || !seq.hasNext()) return;
		BufferedOutputStream writer = null;
		try{
			writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir,getFileName())));
			writer.write(getValueAsBytes(seq.next().getItem()));
			while(seq.hasNext()){
				writer.write(NEWLINE);
				writer.write(getValueAsBytes(seq.next().getItem()));
			}
		}finally{
			if(writer != null){
				writer.close();
			}
		}
	}
		
	
	@Override
	public String call() throws Exception {
		Thread.yield(); // try to avoid thread starvation
		ResultSequence seq = invoke();
		Thread.yield(); // try to avoid thread starvation
		writeToFile(getFileName(),seq);
		Thread.yield(); // try to avoid thread starvation
		return TRUE;
	}

}
