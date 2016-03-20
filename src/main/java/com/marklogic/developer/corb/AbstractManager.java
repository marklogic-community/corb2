/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.DECRYPTER;
import static com.marklogic.developer.corb.Options.OPTIONS_FILE;
import static com.marklogic.developer.corb.Options.SSL_CONFIG_CLASS;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_URI;
import static com.marklogic.developer.corb.Options.XCC_DBNAME;
import static com.marklogic.developer.corb.Options.XCC_HOSTNAME;
import static com.marklogic.developer.corb.Options.XCC_PASSWORD;
import static com.marklogic.developer.corb.Options.XCC_PORT;
import static com.marklogic.developer.corb.Options.XCC_USERNAME;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import java.util.logging.Logger;

public abstract class AbstractManager {
	public static final String VERSION = "2.3.0";
	
	protected static final String VERSION_MSG = "version " + VERSION + " on " + System.getProperty("java.version") + " ("+ System.getProperty("java.runtime.name") + ")";
	protected static final String DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER = "declare namespace mlss = 'http://marklogic.com/xdmp/status/server'\n";
	protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\"\n";
	
	protected Decrypter decrypter;
	protected SSLConfig sslConfig;
	
	protected URI connectionUri;
	protected String collection;
	protected Properties properties = new Properties();;
	protected TransformOptions options = new TransformOptions();;
	protected ContentSource contentSource;
	
    protected static final int EXIT_CODE_SUCCESS = 0;
    protected static final int EXIT_CODE_INIT_ERROR = 1;
    protected static final int EXIT_CODE_PROCESSING_ERROR = 2;

	private static final Logger LOG = Logger.getLogger(AbstractManager.class.getName());
    
	public static Properties loadPropertiesFile(String filename) throws IOException {
		return loadPropertiesFile(filename, true);
	}
	
	public static Properties loadPropertiesFile(String filename, boolean excIfNotFound) throws IOException {
		Properties props = new Properties();
		return loadPropertiesFile(filename, excIfNotFound, props);
	}

	protected static Properties loadPropertiesFile(String filename, boolean excIfNotFound, Properties props) throws IOException {
        filename = trim(filename);
		if (isNotBlank(filename)) {
			InputStream is = null;
			try {
				is = Manager.class.getResourceAsStream("/" + filename);
				if (is != null) {
					LOG.log(INFO, "Loading {0} from classpath", filename);
					props.load(is);
				} else {
					File f = new File(filename);
					if (f.exists() && !f.isDirectory()) {
						LOG.log(INFO, "Loading {0} from filesystem", filename);
						FileInputStream fis = null;
						try {
							fis = new FileInputStream(f);
							props.load(fis);
						} finally {
                            closeQuietly(fis);
						}
					} else if (excIfNotFound) {
						throw new IllegalStateException("Unable to load properties file " + filename);
					}
				}
			} finally {
                closeQuietly(is);
			}
		}
		return props;
	}

	public static String getAdhocQuery(String module) {       
		InputStream is = null;
		InputStreamReader reader = null;
		StringWriter writer = null;
		try {
			is = TaskFactory.class.getResourceAsStream("/" + module);
			if (is == null) {
				File f = new File(module);
				if (f.exists() && !f.isDirectory()) {
					is = new FileInputStream(f);
				} else {
					throw new IllegalStateException("Unable to find adhoc query module " + module + " in classpath or filesystem");
				}
			} else if (isDirectory(is)) {
                throw new IllegalStateException("Adhoc query module cannot be a directory");
            }

			reader = new InputStreamReader(is);
			writer = new StringWriter();
			char[] buffer = new char[512];
			int n = 0;
			while (-1 != (n = reader.read(buffer))) {
				writer.write(buffer, 0, n);
			}
			writer.close();
			reader.close();

			return writer.toString().trim();
		} catch (IOException exc) {
			throw new IllegalStateException("Problem reading adhoc query module " + module, exc);
		} finally {
			closeQuietly(writer);
			closeQuietly(reader);
            closeQuietly(is);
		}
	}
	
    /**
     * Tests whether the <code>InputStream</code> is a directory. 
     * A Directory will be a ByteArrayInputStream and a File will be a BufferedInputStream.
     * @param is
     * @return <code>true</code> if the InputStream class is ByteArrayInputStream
     */
    protected static final boolean isDirectory(InputStream is) {
        return is.getClass().getSimpleName().equals("ByteArrayInputStream");
    }
    
	public Properties getProperties() {
		return this.properties;
	}

	public TransformOptions getOptions() {
		return options;
	}
	
	public void initPropertiesFromOptionsFile() throws IOException{	
		String propsFileName = System.getProperty(OPTIONS_FILE);
		loadPropertiesFile(propsFileName, true, this.properties);
	}
	
	public void init(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException{			
		init(args, null);
	}
	
	public abstract void init(String[] args, Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException;			

	/**
	 * function that is used to get the Decrypter, returns null if not specified
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected void initDecrypter() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
		String decrypterClassName = getOption(DECRYPTER);
		if (decrypterClassName != null) {
			Class<?> decrypterCls = Class.forName(decrypterClassName);
			if (Decrypter.class.isAssignableFrom(decrypterCls)) {
				this.decrypter = (Decrypter) decrypterCls.newInstance();
				decrypter.init(this.properties);
			} else {
				throw new IllegalArgumentException(DECRYPTER + " must be of type com.marklogic.developer.corb.Decrypter");
			}
		} else {
			this.decrypter = null;
		}
	}
	
	protected void initSSLConfig() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String sslConfigClassName = getOption(SSL_CONFIG_CLASS);
		if (sslConfigClassName != null) {
			Class<?> decrypterCls = Class.forName(sslConfigClassName);
			if (SSLConfig.class.isAssignableFrom(decrypterCls)) {
				this.sslConfig = (SSLConfig) decrypterCls.newInstance();
			} else {
				throw new IllegalArgumentException("SSL Options must be of type com.marklogic.developer.corb.SSLConfig");
			}
		} else {
			this.sslConfig = new TrustAnyoneSSLConfig();
		}
		sslConfig.setProperties(this.properties);
		sslConfig.setDecrypter(this.decrypter);
	}
	
	protected void initURI(String uriArg) throws URISyntaxException {
		String uriAsString = getOption(uriArg, XCC_CONNECTION_URI);
		String username = getOption(XCC_USERNAME);
		String password = getOption(XCC_PASSWORD);
		String host = getOption(XCC_HOSTNAME);
		String port = getOption(XCC_PORT);
		String dbname = getOption(XCC_DBNAME);

		if (uriAsString == null && (username == null || password == null || host == null || port == null)) {
			LOG.severe(XCC_CONNECTION_URI + " or " + 
                    XCC_USERNAME + ", " + XCC_PASSWORD + ", " + XCC_HOSTNAME + 
                    " and " + XCC_PORT + " must be specified");
			usage();
			System.exit(EXIT_CODE_INIT_ERROR);
		}
		
		if (this.decrypter != null) {
			uriAsString = this.decrypter.getConnectionURI(uriAsString, username, password, host, port, dbname);
		} else if (uriAsString == null) {
			uriAsString = "xcc://" + username + ":" + password + "@" + host + ":" + port + (dbname != null ? "/" + dbname : "");
		}
		
		this.connectionUri = new URI(uriAsString);
	}
	
    /**
     * Retrieve the value of the specified key from either the System properties, or the properties object.
     * @param propName
     * @return the trimmed property value
     */
    protected String getOption(String propName) {
        return getOption(null, propName);
    }
    
    /**
     * Retrieve either the argVal or the first property value from the System.properties or properties object that is not empty or null.
     * @param argVal
     * @param propName
     * @return the trimmed property value
     */
	protected String getOption(String argVal, String propName) {
		if (isNotBlank(argVal)) {
			return argVal.trim();
		} else if (isNotBlank(System.getProperty(propName))) {
			return System.getProperty(propName).trim();
		} else if (this.properties.containsKey(propName) && isNotBlank(this.properties.getProperty(propName))) {
			String val = this.properties.getProperty(propName).trim();
			this.properties.remove(propName); //remove from properties file as we would like to keep the properties file simple. 
			return val;
		}
		return null;
	}
	
	protected void prepareContentSource() throws XccConfigException, GeneralSecurityException {
		try {
			// support SSL
			boolean ssl = connectionUri != null && connectionUri.getScheme() != null
					&& connectionUri.getScheme().equals("xccs");
			contentSource = ssl ? ContentSourceFactory.newContentSource(connectionUri, getSecurityOptions())
					: ContentSourceFactory.newContentSource(connectionUri);
		} catch (XccConfigException e) {
			LOG.log(SEVERE, "Problem creating content source. Check if URI is valid. If encrypted, check if options are configured correctly.{0}", e.getMessage());
			throw e;
		} catch (KeyManagementException e) {
			LOG.log(SEVERE, "Problem creating content source with ssl. {0}", e.getMessage());
			throw e;
		} catch (NoSuchAlgorithmException e) {
			LOG.log(SEVERE, "Problem creating content source with ssl. {0}", e.getMessage());
			throw e;
		}
	}
	
	protected SecurityOptions getSecurityOptions() throws KeyManagementException, NoSuchAlgorithmException{
		return this.sslConfig.getSecurityOptions();
	}
	
	public ContentSource getContentSource() {
		return this.contentSource;
	}
	
	protected abstract void usage();
	
	protected void logRuntimeArgs(){
		RuntimeMXBean runtimemxBean = ManagementFactory.getRuntimeMXBean();
		List<String> arguments = runtimemxBean.getInputArguments();
		List<String> argsToLog = new ArrayList<String>();
        for (String argument : arguments) {
            if (!argument.startsWith("-DXCC")) {
                argsToLog.add(argument);
            }
        }
		LOG.log(INFO, "runtime arguments = {0}", StringUtils.join(argsToLog, " "));
	}
    
}
