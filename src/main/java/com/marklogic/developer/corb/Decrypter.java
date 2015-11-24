package com.marklogic.developer.corb;

import java.io.IOException;
import java.util.Properties;

public interface Decrypter {
	public void init(Properties properties) throws IOException, ClassNotFoundException;
	public String getConnectionURI(String uri, String username, String password, String host, String port, String dbname);
	public String decrypt(String property, String value);
}
