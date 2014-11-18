package com.marklogic.developer.corb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Properties;

import com.marklogic.developer.SimpleLogger;
import com.marklogic.xcc.ContentSource;

public class FileUrisLoader implements UrisLoader {
	TransformOptions options;	
	ContentSource cs;
	String collection;
	Properties properties;
	
	BufferedReader br = null;
	int total=0;
	
	String[] replacements = new String[0];
	SimpleLogger logger;
	
	public void setOptions(TransformOptions options){
		this.options = options;
	}
	
	public void setContentSource(ContentSource cs){
		this.cs = cs;
	}
	
	public void setCollection(String collection){
		this.collection = collection;
	}
	
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public void open() throws CorbException {
		configureLogger();
		if(properties.containsKey("URIS-REPLACE-PATTERN")){
			String pattern = properties.getProperty("URIS-REPLACE-PATTERN").trim(); 
			replacements = pattern.split(",",-1);
			if(replacements.length % 2 != 0) throw new IllegalArgumentException("Invalid replacement pattern " + pattern);
		}
		try{
			String fileName = options.getUrisFile();
			
			LineNumberReader  lnr = new LineNumberReader(new FileReader(fileName));
			lnr.skip(Long.MAX_VALUE);
			total = lnr.getLineNumber()+1;
			lnr.close();
			
			FileReader fr = new FileReader(fileName);
			br = new BufferedReader(fr);
			
		}catch(Exception exc){
			throw new CorbException("Problem loading data from uris file "+options.getUrisFile(),exc);
		}
	}

	public String getBatchRef() {
		return null;
	}

	public int getTotalCount() {
		return this.total;
	}
	
	private String readNextLine() throws IOException{
		String line = br.readLine();
		if(line != null && (line = line.trim()).length() == 0){
			line = readNextLine();
		}
		return line;
	}
	
	String nextLine = null;
	
	public boolean hasNext() throws CorbException{
		if(nextLine == null){
			try{
				nextLine = readNextLine();
			}catch(Exception exc){
				throw new CorbException("Problem while reading the uris file");
			}
		}
		return nextLine != null;
	}

	@Override
	public String next() throws CorbException{
		String line = null;
		if(nextLine != null){
			line = nextLine;
			nextLine = null;
		}else{
			try{
				line = readNextLine();
			}catch(Exception exc){
				throw new CorbException("Problem while reading the uris file");
			}
		}
		for(int i=0; line != null && i<replacements.length-1; i=i+2){
			line = line.replaceAll(replacements[i], replacements[i+1]);
		}
		return line;
	}

	public void close() {
		if(br != null){
			logger.info("closing uris file reader");
			try{
				br.close();
				br = null;
			}catch(Exception exc){
				logger.logException("while closing uris file reader",exc);
			}
		}
	}
	
	private void configureLogger() {
        if (logger == null) {
            logger = SimpleLogger.getSimpleLogger();
        }
        Properties props = new Properties();
        props.setProperty("LOG_LEVEL", options.getLogLevel());
        props.setProperty("LOG_HANDLER", options.getLogHandler());
        logger.configureLogger(props);
    }

}
