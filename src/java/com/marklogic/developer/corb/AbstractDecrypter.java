package com.marklogic.developer.corb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.marklogic.developer.SimpleLogger;

public abstract class AbstractDecrypter{
	protected Properties properties = null;
	protected Properties decrypterProps = null;
	protected static final Pattern regex = Pattern.compile("^ENC\\((.*)\\)$");
	
	static protected SimpleLogger logger;
	static{
		logger = SimpleLogger.getSimpleLogger();
		Properties props = new Properties();
        props.setProperty("LOG_LEVEL", "INFO");
        props.setProperty("LOG_HANDLER", "CONSOLE");
        logger.configureLogger(props);
	}
	
	public void init(Properties properties) throws IOException{
		this.properties = (properties == null ? new Properties() : properties);
		
		String decryptPropsFile = getProperty("DECRYPTER-PROPERTIES-FILE");
		if(decryptPropsFile == null || decryptPropsFile.trim().length() == 0){
			decryptPropsFile=default_decrypter_file();
		}
		decrypterProps = loadPropertiesFile(decryptPropsFile);
		
		init_decrypter();
	}
	
	public String getConnectionURI(String uri, String username, String password, String host, String port) {
		if(uri != null){
			return decrypt(uri);
		}else{
			return "xcc://"+decrypt(username)+":"+decrypt(password)+"@"+decrypt(host)+":"+decrypt(port);
		}
	}
	
	protected String decrypt(String value){
		Matcher match = regex.matcher(value);
        if (match.matches()) {
            value = match.group(1);
        }
		return doDecrypt(value);
	}
	
	protected Properties loadPropertiesFile(String filename) throws IOException{
		Properties props = new Properties();
		if(filename != null && (filename=filename.trim()).length() > 0){
			logger.info("Loading "+filename);
			InputStream is = null;
			try {
				is = Manager.class.getResourceAsStream("/" + filename);
				if (is != null) {
					props.load(is);
				} else {
					File f = new File(filename);
					if (f.exists() && !f.isDirectory()) {
						FileInputStream fis = null;
						try {
							fis = new FileInputStream(f);
							props.load(fis);
						} finally {
							if (null != fis) {
								fis.close();
							}
						}
					}else{
						throw new IllegalStateException("Unable to load decrypter properties");
					}
				}
			} finally {
				if (null != is) {
					is.close();
				}
			}
		}
		return props;
	}
	
	protected abstract String default_decrypter_file();
	
	protected abstract void init_decrypter();
		
	protected abstract String doDecrypt(String value);
	
	protected String getProperty(String key){
		String val = System.getProperty(key);
		if(val == null || val.trim().length() == 0){
			val = properties.getProperty(key);
		}
		return val != null ? val.trim() : val;
	}
}
