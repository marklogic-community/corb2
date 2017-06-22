/*
 * Copyright (c) 2004-2017 MarkLogic Corporation
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
import static com.marklogic.developer.corb.util.IOUtils.isDirectory;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isInlineModule;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;

import com.marklogic.developer.corb.util.StringUtils;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmItem;

public abstract class AbstractManager {


	public static final String VERSION = "2.4.0";

    protected static final String VERSION_MSG = "version " + VERSION + " on " + System.getProperty("java.version") + " (" + System.getProperty("java.runtime.name") + ')';
    protected static final String DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER = "declare namespace mlss = 'http://marklogic.com/xdmp/status/server';\n";
    protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\";\n";

    protected Decrypter decrypter;
    protected SSLConfig sslConfig;
    protected URI connectionUri;
    protected String collection;
    protected TransformOptions options = new TransformOptions();
    protected Properties properties = new Properties();
    protected ContentSource contentSource;
    protected Map<String,String> userProvidedOptions = new HashMap<String,String>();
	
    protected static final int EXIT_CODE_SUCCESS = 0;
    protected static final int EXIT_CODE_INIT_ERROR = 1;
    protected static final int EXIT_CODE_PROCESSING_ERROR = 2;
    protected static final String SPACE = " ";

    private static final Logger LOG = Logger.getLogger(AbstractManager.class.getName());

    public static Properties loadPropertiesFile(String filename) throws IOException {
        return loadPropertiesFile(filename, true);
    }

    public static Properties loadPropertiesFile(String filename, boolean excIfNotFound) throws IOException {
        Properties props = new Properties();
        return loadPropertiesFile(filename, excIfNotFound, props);
    }

    protected static Properties loadPropertiesFile(String filename, boolean exceptionIfNotFound, Properties props) throws IOException {
        String name = trim(filename);
        if (isNotBlank(name)) {
            try (InputStream is = Manager.class.getResourceAsStream('/' + name)) {
                if (is != null) {
                    LOG.log(INFO, () -> MessageFormat.format("Loading {0} from classpath", name));
                    props.load(is);
                } else {
                    File f = new File(filename);
                    if (f.exists() && !f.isDirectory()) {
                        LOG.log(INFO, () -> MessageFormat.format("Loading {0} from filesystem", name));
                        try (FileInputStream fis = new FileInputStream(f)) {
                            props.load(fis);
                        }
                    } else if (exceptionIfNotFound) {
                        throw new IllegalStateException("Unable to load properties file " + name);
                    }
                }
            }
        }
        return props;
    }

    public static String getAdhocQuery(String module) {

        try {
            InputStream is = TaskFactory.class.getResourceAsStream('/' + module);
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

            try (InputStreamReader reader = new InputStreamReader(is);
                    StringWriter writer = new StringWriter()) {

                char[] buffer = new char[512];
                int n = 0;
                while (-1 != (n = reader.read(buffer))) {
                    writer.write(buffer, 0, n);
                }
                return writer.toString().trim();
            }
        } catch (IOException exc) {
            throw new IllegalStateException("Problem reading adhoc query module " + module, exc);
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public TransformOptions getOptions() {
        return options;
    }

    public void initPropertiesFromOptionsFile() throws IOException {
        String propsFileName = System.getProperty(OPTIONS_FILE);
        loadPropertiesFile(propsFileName, true, this.properties);
    }

    public void init(String... args) throws CorbException {
        init(args, null);
    }

    public void init(Properties props) throws CorbException {
        String[] args = {};
        init(args, props);
    }

    public void init(String[] commandlineArgs, Properties props) throws CorbException {
        String[] args = commandlineArgs;
        if (args == null) {
            args = new String[0];
        }
        if (props == null || props.isEmpty()) {
            try {
                initPropertiesFromOptionsFile();
            } catch (IOException ex) {
                throw new CorbException("Failed to initialized properties from options file", ex);
            }
        } else {
            this.properties = props;
        }
        initDecrypter();
        initSSLConfig();
        initURI(args.length > 0 ? args[0] : null);
        initOptions(args);
        logRuntimeArgs();
        prepareContentSource();
        registerStatusInfo();
    }

    /**
     * function that is used to get the Decrypter, returns null if not specified
     *
     * @throws CorbException
     */
    protected void initDecrypter() throws CorbException {
        String decrypterClassName = getOption(DECRYPTER);
        if (decrypterClassName != null) {
            try {
                Class<?> decrypterCls = Class.forName(decrypterClassName);
                if (Decrypter.class.isAssignableFrom(decrypterCls)) {
                    this.decrypter = (Decrypter) decrypterCls.newInstance();
                    decrypter.init(this.properties);
                } else {
                    throw new IllegalArgumentException(DECRYPTER + " must be of type com.marklogic.developer.corb.Decrypter");
                }
            } catch (ClassNotFoundException | IOException | InstantiationException | IllegalAccessException ex) {
                throw new CorbException(MessageFormat.format("Unable to instantiate {0} {1}", SSL_CONFIG_CLASS, decrypterClassName), ex);
            }
        } else {
            this.decrypter = null;
        }
    }

    protected void initSSLConfig() throws CorbException {
        String sslConfigClassName = getOption(SSL_CONFIG_CLASS);
        if (sslConfigClassName != null) {
            try {
                Class<?> decrypterCls = Class.forName(sslConfigClassName);
                if (SSLConfig.class.isAssignableFrom(decrypterCls)) {
                    this.sslConfig = (SSLConfig) decrypterCls.newInstance();
                } else {
                    throw new IllegalArgumentException("SSL Options must be of type com.marklogic.developer.corb.SSLConfig");
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                throw new CorbException(MessageFormat.format("Unable to instantiate {0} {1}", SSL_CONFIG_CLASS, sslConfigClassName), ex);
            }
        } else {
            LOG.info("Using TrustAnyoneSSSLConfig because no " + SSL_CONFIG_CLASS + " value specified.");
            this.sslConfig = new TrustAnyoneSSLConfig();
        }
        sslConfig.setProperties(this.properties);
        sslConfig.setDecrypter(this.decrypter);
    }

    protected void initURI(String uriArg) throws CorbException {
        String uriAsString = getOption(uriArg, XCC_CONNECTION_URI);
        String username = getOption(XCC_USERNAME);
        String password = getOption(XCC_PASSWORD);
        String host = getOption(XCC_HOSTNAME);
        String port = getOption(XCC_PORT);
        String dbname = getOption(XCC_DBNAME);

        if (StringUtils.anyIsNull(uriAsString) && StringUtils.anyIsNull(username, password, host, port)) {
            throw new CorbException(String.format("Either %1$s or %2$s, %3$s, %4$s, and %5$s must be specified",
                    XCC_CONNECTION_URI, XCC_USERNAME, XCC_PASSWORD, XCC_HOSTNAME, XCC_PORT));
        }

        if (this.decrypter != null) {
            uriAsString = this.decrypter.getConnectionURI(uriAsString, username, password, host, port, dbname);
        } else if (uriAsString == null) {
            uriAsString = StringUtils.getXccUri(username, password, host, port, dbname);
        }

        try {
            this.connectionUri = new URI(uriAsString);
        } catch (URISyntaxException ex) {
            throw new CorbException("XCC URI is invalid", ex);
        }
    }

    protected void initOptions(String... args) throws CorbException {
        String xccHttpCompliant = getOption(Options.XCC_HTTPCOMPLIANT);
        if (isNotBlank(xccHttpCompliant)) {
            System.setProperty("xcc.httpcompliant", Boolean.toString(StringUtils.stringToBoolean(xccHttpCompliant)));
        }
    }

    protected boolean deleteFileIfExists(String directory, String filename) {
        if (filename != null) {
            File file = new File(directory, filename);
            if (file.exists()) {
                return file.delete();
            }
        }
        return false;
    }

    protected void registerStatusInfo() {
        Session session = contentSource.newSession();
        AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_ML + DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
                + "let $status := xdmp:server-status(xdmp:host(), xdmp:server())\n"
                + "let $modules := $status/mlss:modules\n"
                + "let $root := $status/mlss:root\n"
                + "return (data($modules), data($root))");
        ResultSequence rs = null;
        try {
            rs = session.submitRequest(q);
        } catch (RequestException e) {
            LOG.log(SEVERE, "registerStatusInfo request failed", e);
            e.printStackTrace();
        } finally {
            session.close();
        }
        while (null != rs && rs.hasNext()) {
            ResultItem rsItem = rs.next();
            XdmItem item = rsItem.getItem();
            if (rsItem.getIndex() == 0 && "0".equals(item.asString())) {
                options.setModulesDatabase("");
            }
            if (rsItem.getIndex() == 1) {
                options.setXDBC_ROOT(item.asString());
            }
        }
        logOptions();
        logProperties();
    }

    protected void logOptions() {
        //default behavior is not to log anything
    }

    protected void logProperties() {
        for (Entry<Object, Object> e : properties.entrySet()) {
            if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
                LOG.log(INFO, () -> MessageFormat.format("Loaded property {0}={1}", e.getKey(), e.getValue()));
            }
        }
    }

    /**
     * Retrieve the value of the specified key from either the System
     * properties, or the properties object.
     *
     * @param propertyName
     * @return the trimmed property value
     */
    protected String getOption(String propertyName) {
        return getOption(null, propertyName);
    }

    /**
     * Retrieve either the value from the commandline arguments at the argIndex,
     * or the first property value from the System.properties or properties object
     * that is not empty or null.
     * @param commandlineArgs
     * @param argIndex
     * @param propertyName
     * @return the trimmed property value
     */
    protected String getOption(String[] commandlineArgs, int argIndex, String propertyName) {
        String argValue = commandlineArgs.length > argIndex ? commandlineArgs[argIndex] : null;
        return getOption(argValue, propertyName);
    }

    /**
     * Retrieve either the argVal or the first property value from the
     * System.properties or properties object that is not empty or null.
     *
     * @param argVal
     * @param propertyName
     * @return the trimmed property value
     */
    protected String getOption(String argVal, String propName) {
        String retVal=null;
    	if (isNotBlank(argVal)) {
    		retVal= argVal.trim();
        } else if (isNotBlank(System.getProperty(propName))) {
        	retVal= System.getProperty(propName).trim();
        } else if (this.properties.containsKey(propName) && isNotBlank(this.properties.getProperty(propName))) {
        	retVal = this.properties.getProperty(propName).trim();
            this.properties.remove(propName); //remove from properties file as we would like to keep the properties file simple. 
        }
    	//doesnt capture defaults, only user provided.
    	if(retVal !=null && !retVal.toUpperCase().contains("XCC") && !propName.toUpperCase().contains("XCC")){
    		this.userProvidedOptions.put(propName, retVal);
    	}
        return retVal;
    }

    protected void prepareContentSource() throws CorbException {
        try {
            // support SSL
            boolean ssl = connectionUri != null && connectionUri.getScheme() != null
                    && "xccs".equals(connectionUri.getScheme());
            contentSource = ssl ? ContentSourceFactory.newContentSource(connectionUri, getSecurityOptions())
                    : ContentSourceFactory.newContentSource(connectionUri);
        } catch (XccConfigException ex) {
            throw new CorbException("Problem creating content source. Check if URI is valid. If encrypted, check if options are configured correctly.", ex);
        } catch (KeyManagementException | NoSuchAlgorithmException ex) {
            throw new CorbException("Problem creating content source with ssl", ex);
        }
    }

    protected SecurityOptions getSecurityOptions() throws KeyManagementException, NoSuchAlgorithmException {
        return this.sslConfig.getSecurityOptions();
    }

    public ContentSource getContentSource() {
        return this.contentSource;
    }

    protected void usage() {
        PrintStream err = System.err;
        err.println("CoRB2 requires options to be specified through one or more of the following mechanisms:\n"
                + "1.) command-line parameters\n"
                + "2.) Java system properties ex: -DXCC-CONNECTION-URI=xcc://user:password@localhost:8202\n"
                + "3.) As properties file in the class path specified using -DOPTIONS-FILE=myjob.properties. "
                + "Relative and full file system paths are also supported.\n"
                + "If specified in more than one place, a command line parameter takes precedence over "
                + "a Java system property, which take precedence over a property "
                + "from the OPTIONS-FILE properties file.\n\n"
                + "CoRB2 Options:\n"); // NOPMD

        for (java.lang.reflect.Field field : Options.class.getDeclaredFields()) {
            Usage usage = field.getAnnotation(Usage.class);
            if (usage != null && StringUtils.isNotEmpty(usage.description())) {
                err.println(field.getName() + "\n\t" + usage.description()); // NOPMD
            }
        }

        err.println("\nPlease report issues at: https://github.com/marklogic-community/corb2/issues\n"); // NOPMD
    }

    protected String buildSystemPropertyArg(String property, String value) {
        StringBuilder arg = new StringBuilder("-D");
        arg.append(property);
        if (StringUtils.isNotEmpty(value)) {
            arg.append('=');
            arg.append(value);
        }
        return arg.toString();
    }

    protected void logRuntimeArgs() {
        RuntimeMXBean runtimemxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimemxBean.getInputArguments();
        List<String> argsToLog = new ArrayList<>(arguments.size());
        for (String argument : arguments) {
            if (!argument.startsWith("-DXCC")) {
                argsToLog.add(argument);
            }
        }
        LOG.log(INFO, () -> MessageFormat.format("runtime arguments = {0}", StringUtils.join(argsToLog, SPACE)));
    }
    public Map<String, String> getUserProvidedOptions() {
		return userProvidedOptions;
	}

	public void setUserProvidedOptions(Map<String, String> userProvidedOptions) {
		this.userProvidedOptions = userProvidedOptions;
	}
	protected Request getRequestForModule(String processModule, Session session) {
		Request request;
		if (isInlineOrAdhoc(processModule)) {
		    String adhocQuery;
		    if (isInlineModule(processModule)) {
		        adhocQuery = StringUtils.getInlineModuleCode(processModule);
		        if (isBlank(adhocQuery)) {
		            throw new IllegalStateException("Unable to read inline query ");
		        }
		        LOG.log(INFO, "invoking inline process module");
		    } else {
		        String queryPath = processModule.substring(0, processModule.indexOf('|'));
		        adhocQuery = getAdhocQuery(queryPath);
		        if (isBlank(adhocQuery)) {
		            throw new IllegalStateException("Unable to read adhoc query " + queryPath + " from classpath or filesystem");
		        }
		        LOG.log(INFO, "invoking adhoc process module {0}", queryPath);
		    }
		    request = session.newAdhocQuery(adhocQuery);
		    
		} else {
		    String root = options.getModuleRoot();
		    String modulePath = buildModulePath(root, processModule);
		    LOG.log(INFO, "invoking module {0}", modulePath);
		    request = session.newModuleInvoke(modulePath);
		}
		return request;
	}
}
