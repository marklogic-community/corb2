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
import static com.marklogic.developer.corb.util.IOUtils.isDirectory;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isInlineModule;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.SecurityOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XdmItem;

public abstract class AbstractManager {

    private static final String METRICS_COLLECTIONS_PARAM = "collections";

	private static final String METRICS_DOCUMENT_STR_PARAM = "metricsDocumentStr";

	private static final String METRICS_DB_NAME_PARAM = "dbName";

	private static final String METRICS_URI_ROOT_PARAM = "uriRoot";

	public static final String VERSION = "2.4.0";

    protected static final String VERSION_MSG = "version " + VERSION + " on " + System.getProperty("java.version") + " (" + System.getProperty("java.runtime.name") + ")";
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
	protected JobStats jobStats = null;
	protected long startMillis;
	protected long transformStartMillis;
	
	protected long endMillis;
	protected transient PausableThreadPoolExecutor pool;//moved from Manager
    protected static final int EXIT_CODE_SUCCESS = 0;
    protected static final int EXIT_CODE_INIT_ERROR = 1;
    protected static final int EXIT_CODE_PROCESSING_ERROR = 2;
    protected static final String SPACE = " ";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");  
	
    private static final Logger LOG = Logger.getLogger(AbstractManager.class.getName());

    public static Properties loadPropertiesFile(String filename) throws IOException {
        return loadPropertiesFile(filename, true);
    }

    public static Properties loadPropertiesFile(String filename, boolean excIfNotFound) throws IOException {
        Properties props = new Properties();
        return loadPropertiesFile(filename, excIfNotFound, props);
    }

    protected static Properties loadPropertiesFile(String filename, boolean excIfNotFound, Properties props) throws IOException {
        String name = trim(filename);
        if (isNotBlank(name)) {
            try (InputStream is = Manager.class.getResourceAsStream("/" + name)) {
                if (is != null) {
                    LOG.log(INFO, "Loading {0} from classpath", name);
                    props.load(is);
                } else {
                    File f = new File(filename);
                    if (f.exists() && !f.isDirectory()) {
                        LOG.log(INFO, "Loading {0} from filesystem", name);
                        try (FileInputStream fis = new FileInputStream(f)) {
                            props.load(fis);
                        }
                    } else if (excIfNotFound) {
                        throw new IllegalStateException("Unable to load properties file " + name);
                    }
                }
            }
        }
        return props;
    }

    public static String getAdhocQuery(String module) {

        try {
            InputStream is = TaskFactory.class.getResourceAsStream("/" + module);
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
        return this.properties;
    }

    public TransformOptions getOptions() {
        return options;
    }

    public void initPropertiesFromOptionsFile() throws IOException {
        String propsFileName = System.getProperty(OPTIONS_FILE);
        loadPropertiesFile(propsFileName, true, this.properties);
    }

    public void init(String... args) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
        init(args, null);
    }

    public void init(Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException {
        String[] args = {};
        init(args, props);
    }
    
    public abstract void init(String[] args, Properties props) throws IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, XccConfigException, GeneralSecurityException, RequestException;

    /**
     * function that is used to get the Decrypter, returns null if not specified
     *
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

    protected void initURI(String uriArg) throws InstantiationException, URISyntaxException {
        String uriAsString = getOption(uriArg, XCC_CONNECTION_URI);
        String username = getOption(XCC_USERNAME);
        String password = getOption(XCC_PASSWORD);
        String host = getOption(XCC_HOSTNAME);
        String port = getOption(XCC_PORT);
        String dbname = getOption(XCC_DBNAME);

        if (StringUtils.anyIsNull(uriAsString) && StringUtils.anyIsNull(username, password, host, port)) {
            throw new InstantiationException(String.format("Either %1$s or %2$s, %3$s, %4$s, and %5$s must be specified",
                    XCC_CONNECTION_URI, XCC_USERNAME, XCC_PASSWORD, XCC_HOSTNAME, XCC_PORT));
        }

        if (this.decrypter != null) {
            uriAsString = this.decrypter.getConnectionURI(uriAsString, username, password, host, port, dbname);
        } else if (uriAsString == null) {
            uriAsString = StringUtils.getXccUri(username, password, host, port, dbname);
        }

        this.connectionUri = new URI(uriAsString);
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
    
    protected void logOptions(){
        //default behavior is not lo log anything
    }
    
    protected void logProperties() {
        for (Entry<Object, Object> e : properties.entrySet()) {
            if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
                LOG.log(INFO, "Loaded property {0}={1}", new Object[]{e.getKey(), e.getValue()});
            }
        }
    }

    /**
     * Retrieve the value of the specified key from either the System
     * properties, or the properties object.
     *
     * @param propName
     * @return the trimmed property value
     */
    protected String getOption(String propName) {
        return getOption(null, propName);
    }

    /**
     * Retrieve either the argVal or the first property value from the
     * System.properties or properties object that is not empty or null.
     *
     * @param argVal
     * @param propName
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

    protected void prepareContentSource() throws XccConfigException, GeneralSecurityException {
        String errorMsg = "Problem creating content source with ssl. {0}";
        try {
            // support SSL
            boolean ssl = connectionUri != null && connectionUri.getScheme() != null
                    && "xccs".equals(connectionUri.getScheme());
            contentSource = ssl ? ContentSourceFactory.newContentSource(connectionUri, getSecurityOptions())
                    : ContentSourceFactory.newContentSource(connectionUri);
        } catch (XccConfigException e) {
            LOG.log(SEVERE, "Problem creating content source. Check if URI is valid. If encrypted, check if options are configured correctly.{0}", e.getMessage());
            throw e;
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            LOG.log(SEVERE, errorMsg, e.getMessage());
            throw e;
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
                + "CoRB2 Options:\n");

        for (java.lang.reflect.Field field : Options.class.getDeclaredFields()) {
            Usage usage = field.getAnnotation(Usage.class);
            if (usage != null && StringUtils.isNotEmpty(usage.description())) {
                err.println(field.getName() + "\n\t" + usage.description());
            }
        }

        err.println("\nPlease report issues at: https://github.com/marklogic/corb2/issues\n");
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
        LOG.log(INFO, "runtime arguments = {0}", StringUtils.join(argsToLog, SPACE));
    }
    public Map<String, String> getUserProvidedOptions() {
		return userProvidedOptions;
	}

	public void setUserProvidedOptions(Map<String, String> userProvidedOptions) {
		this.userProvidedOptions = userProvidedOptions;
	}
	protected void logJobStatsToServer(String message) {
		logJobStatsToServerDocument();
		logJobStatsToServerLog(message,false);
		LOG.info(jobStats.toString(false));
	}

	protected void logJobStatsToServerLog(String message, boolean concise) {
		Session session = null;
		try {
			if(contentSource !=null){
				session=contentSource.newSession();
				this.initJobStats();
				String logMetricsToServerLog = options.getLogMetricsToServerLog();
				if (logMetricsToServerLog != null && !logMetricsToServerLog.equalsIgnoreCase("NONE")) {

					String xquery = XQUERY_VERSION_ML
							+ ((message != null)
									? "xdmp:log(\"" + message + "\",'" + logMetricsToServerLog.toLowerCase() + "')," : "")
							+ "xdmp:log('" + jobStats.toString(concise) + "\','" + logMetricsToServerLog.toLowerCase()
							+ "')";

					AdhocQuery q = session.newAdhocQuery(xquery);

					session.submitRequest(q);
				}
			}			
		} catch (Exception e) {
			LOG.log(SEVERE, "logJobStatsToServer request failed", e);
			e.printStackTrace();
		} finally {
			if (session != null) {
				session.close();
			}
		}

	}
	protected void logJobStatsToServerDocument() {	
		try {
			this.populateJobStats();
			String logMetricsToServerDBName=options.getLogMetricsToServerDBName();
			if(logMetricsToServerDBName !=null){
				String uriRoot=options.getLogMetricsToServerDBURIRoot();
				
				String uri=logMetricsToDB(logMetricsToServerDBName,uriRoot,options.getLogMetricsToServerDBCollections(), jobStats,options.getLogMetricsToServerDBTransformModule());
				this.jobStats.setUri(uri);
			}
		} catch (Exception e) {
			LOG.log(INFO, "Unable to log metrics to server as Document", e);
		}          
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

	protected String logMetricsToDB(String dbName,String uriRoot,String collections,JobStats jobStats,String processModule) throws CorbException {
        Session session = null;
        ResultSequence seq = null;
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setCacheResult(false);

        Thread.yield();// try to avoid thread starvation
        try {
        	if(contentSource !=null){    			
	            session = contentSource.newSession();
	            Request request = getRequestForModule(processModule, session);
	            
	            request.setNewStringVariable(METRICS_DB_NAME_PARAM, dbName);
	            if(uriRoot!=null){
	            	request.setNewStringVariable(METRICS_URI_ROOT_PARAM, uriRoot);
	            }
	            else{
	            	request.setNewStringVariable(METRICS_URI_ROOT_PARAM, "NA");
	            }
	            if(collections!=null) {
	            	request.setNewStringVariable(METRICS_COLLECTIONS_PARAM, collections);
	            }
	            else{
	            	request.setNewStringVariable(METRICS_COLLECTIONS_PARAM, "NA");
	            }
	            if (isJavaScriptModule(processModule)) {
			        requestOptions.setQueryLanguage("javascript");
			        request.setNewStringVariable(METRICS_DOCUMENT_STR_PARAM, jobStats.toJSONString());	            
			    }
	            else{
	            	request.setNewStringVariable(METRICS_DOCUMENT_STR_PARAM, jobStats.toXMLString());
	                
	            }
	            request.setOptions(requestOptions);
	
	            seq = session.submitRequest(request);
	            String uri=seq.hasNext()?seq.next().asString():"";
	            session.close();
	            Thread.yield();// try to avoid thread starvation
	            seq.close();
	            Thread.yield();// try to avoid thread starvation
	            return uri;
        	}
        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            if (null != session && !session.isClosed()) {
                session.close();
                session = null;
            }
            if (null != seq && !seq.isClosed()) {
                seq.close();
                seq = null;
            }
            Thread.yield();// try to avoid thread starvation
        }
        return null;
    }

	protected JobStats populateJobStats() {
		try{
			synchronized (this.jobStats) {
				this.initJobStats();
				Long taskCount=(pool!=null)?pool.getTaskCount():0l;
				if(taskCount > 0 ) {
					this.jobStats.setTopTimeTakingUris((pool!=null)?this.pool.getTopUris():null);
					List<String> failedUris=(pool!=null)?this.pool.getFailedUris():null;
					this.jobStats.setFailedUris(failedUris);
					long numberOfFailedTasks = (this.pool!=null)?new Long(this.pool.getNumFailedUris()):0l;
					this.jobStats.setNumberOfFailedTasks(numberOfFailedTasks);
					long numberOfSucceededTasks = (this.pool!=null)?new Long(this.pool.getNumSucceededUris()):0l;
					this.jobStats.setNumberOfSucceededTasks(numberOfSucceededTasks);
					this.jobStats.setTotalNumberOfTasks(taskCount);
					this.jobStats.setEndTime(sdf.format(new Date(this.endMillis)));
					Long totalTime = endMillis - startMillis;
					this.jobStats.setTotalRunTimeInMillis(totalTime);
					Long totalTransformTime = endMillis - transformStartMillis;
					this.jobStats.setAverageTransactionTime(new Double(totalTransformTime / new Double(numberOfFailedTasks+numberOfSucceededTasks)));
				}	
			}
							
		}
		catch(Exception e){
			LOG.log(INFO,"Unable to populate job stats");
		}
		return this.jobStats;
	}

	private void initJobStats() {
		if(this.jobStats==null){
			this.jobStats=new JobStats();
		
			String jobName=options.getJobName();
			if(jobName!=null){
				jobStats.setJobName(jobName);
			}
			String hostname = "Unknown";
	
			try
			{
			    InetAddress addr;
			    addr = InetAddress.getLocalHost();
			    hostname = addr.getHostName();
			}
			catch (UnknownHostException ex)
			{
				try {
					hostname = InetAddress.getLoopbackAddress().getHostName();
				} catch (Exception e) {
					LOG.log(INFO, "Hostname can not be resolved", e);
				}
			}
			this.jobStats.setHost(hostname);
			this.jobStats.setJobRunLocation(System.getProperty("user.dir"));
			this.jobStats.setStartTime(sdf.format(new Date(this.startMillis)));
			this.jobStats.setUserProvidedOptions(this.getUserProvidedOptions());
		}
	}
	
}
