package com.marklogic.developer.corb;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Properties;

public class JasyptDecrypter extends AbstractDecrypter{
	protected Properties jaspytProperties = null;
	Class<?> decrypterCls = null;
	Object decrypter = null;
	
	@Override
	protected void init_decrypter() throws IOException{
		String decryptPropsFile = getProperty("JASYPT-PROPERTIES");
		if(decryptPropsFile == null || decryptPropsFile.trim().length() == 0){
			decryptPropsFile="jasypt.properties";
		}
		jaspytProperties = Manager.loadPropertiesFile(decryptPropsFile);
		
		String algorithm = jaspytProperties.getProperty("jasypt.algorithm");
		if(algorithm == null || algorithm.trim().length() == 0){
			algorithm = "PBEWithMD5AndTripleDES"; //select a secure algorithm as default
		}
		String passphrase = jaspytProperties.getProperty("jasypt.password");
		if(passphrase != null && passphrase.trim().length() > 0){
			try{
				decrypterCls = Class.forName("org.jasypt.encryption.pbe.StandardPBEStringEncryptor");
				decrypter = decrypterCls.newInstance();
				Method setAlgorithm = decrypterCls.getMethod("setAlgorithm", String.class);
				setAlgorithm.invoke(decrypter, algorithm);
				
				Method setPassword = decrypterCls.getMethod("setPassword", String.class);
				setPassword.invoke(decrypter, passphrase);
			}catch(Exception exc){
				throw new IllegalStateException("Unable to initialize org.jasypt.encryption.pbe.StandardPBEStringEncryptor - check if jasypt libraries are in classpath",exc);
			}
		}else{
			Manager.logger.severe("Unable to initialize jasypt decrypter. Couldn't find jasypt.password");
		}
	}

	@Override
	protected String doDecrypt(String property,String value) {
		String dValue = null;
		if(decrypter != null){
			try{
				Method decrypt = decrypterCls.getMethod("decrypt", String.class);
				dValue = (String)decrypt.invoke(decrypter, value);
			}catch(Exception exc){
				Manager.logger.info("Cannot decrypt "+property+". Ignore if clear text.");
			}
		}
		return dValue == null ? value : dValue;
	}

}