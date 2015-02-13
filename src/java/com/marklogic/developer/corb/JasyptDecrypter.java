package com.marklogic.developer.corb;

import java.lang.reflect.Method;

public class JasyptDecrypter extends AbstractDecrypter{
	
	Class<?> decrypterCls = null;
	Object decrypter = null;
	
	protected String default_decrypter_file(){
		return "jasypt.properties";
	}
	
	@Override
	protected void init_decrypter(){
		String algorithm = decrypterProps.getProperty("jasypt.algorithm");
		if(algorithm == null || algorithm.trim().length() == 0){
			algorithm = "PBEWithMD5AndTripleDES"; //select a secure algorithm as default
		}
		String passphrase = decrypterProps.getProperty("jasypt.password");
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
			logger.severe("Unable to initialize decrypter. Couldn't find jasypt.password");
		}
	}

	@Override
	protected String doDecrypt(String value) {
		String dValue = null;
		if(decrypter != null){
			try{
				Method decrypt = decrypterCls.getMethod("decrypt", String.class);
				dValue = (String)decrypt.invoke(decrypter, value);
			}catch(Exception exc){
				logger.info("Unable to decrypt. Ignore if clear text.");
			}
		}
		return dValue == null ? value : dValue;
	}

}