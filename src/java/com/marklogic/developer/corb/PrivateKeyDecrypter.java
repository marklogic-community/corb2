package com.marklogic.developer.corb;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.crypto.Cipher;
import javax.xml.bind.DatatypeConverter;

public class PrivateKeyDecrypter extends AbstractDecrypter {
	private String algorithm=null;
	//rsa
	//openssl genrsa -out private.pem 1024
	//openssl rsa -in private.pem -pubout > public.key
	//openssl pkcs8 -topk8 -nocrypt -in private.pem -out private.pkcs8.key
	//echo "password or uri" | openssl rsautl -encrypt -pubin -inkey public.key | base64
	private PrivateKey privateKey = null;

	@Override
	protected void init_decrypter() throws IOException,ClassNotFoundException {		
		algorithm = getProperty("PRIVATE-KEY-ALGORITHM");
		if(algorithm==null || algorithm.trim().length() == 0){
			algorithm="RSA";
		}
		
		String filename = getProperty("PRIVATE-KEY-FILE");
		if(filename != null && (filename=filename.trim()).length() > 0){
			InputStream is = null;
			try {
				is = Manager.class.getResourceAsStream("/" + filename);
				if(is != null){
					Manager.logger.info("Loading private key file "+filename+" from classpath");
				}else{
					File f = new File(filename);
					if (f.exists() && !f.isDirectory()) {
						Manager.logger.info("Loading private key file "+filename+" from filesystem");
						is = new FileInputStream(f);
					}else{
						throw new IllegalStateException("Unable to load "+filename);
					}
				}
				
			    String keyAsString = new String(toByteArray(is));
			    //remove the begin and end key lines if present. 
			    keyAsString=keyAsString.replaceAll("[-]+(BEGIN|END)[A-Z ]*KEY[-]+","");
			    
				KeyFactory keyFactory = KeyFactory.getInstance(algorithm);				
				privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(DatatypeConverter.parseBase64Binary(keyAsString)));				
			}catch(Exception exc){
				exc.printStackTrace();
			}finally{
				if(is != null){
					is.close();
				}
			}
		}else{
			Manager.logger.severe("PRIVATE-KEY-FILE property must be defined");
		}
	}
	
	private static byte[] toByteArray(final InputStream input) throws IOException {
		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		
		byte[] buffer = new byte[1024];
		int n = 0;
		while (-1 != (n = input.read(buffer))) {
			output.write(buffer, 0, n);
		}
		
		return output.toByteArray();
	}

	@Override
	protected String doDecrypt(String property, String value) {
		String dValue = null;
		if(privateKey != null){
			try{
				final Cipher cipher = Cipher.getInstance(algorithm);
			    cipher.init(Cipher.DECRYPT_MODE, privateKey);
			    dValue = new String(cipher.doFinal(DatatypeConverter.parseBase64Binary(value)));
			}catch(Exception exc){
				Manager.logger.info("Cannot decrypt "+property+". Ignore if clear text.");
			}
		}
		return dValue == null ? value : dValue.trim();
	}
}
