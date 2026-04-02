/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import static com.marklogic.developer.corb.util.IOUtils.isDirectory;

import com.marklogic.developer.corb.util.NumberUtils;

import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.XdmItem;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;

import static com.marklogic.developer.corb.util.StringUtils.*;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.SEVERE;

import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract base class for CoRB managers.
 * Provides common functionality for managing CoRB jobs including property loading,
 * connection management, SSL configuration, decryption, and XCC content source pool initialization.
 * Subclasses must implement specific job execution strategies.
 */
public abstract class AbstractManager {

    /** Version obtained from META-INF/MANIFEST.MF Implementation-Version attribute */
    public static final String VERSION = AbstractManager.class.getPackage().getImplementationVersion();

    /**
     * Formatted version message string including CoRB version, Java version, and runtime name.
     * Used in usage information and startup logging.
     */
    protected static final String VERSION_MSG = "version " + VERSION + " on " + System.getProperty("java.version") + " (" + System.getProperty("java.runtime.name") + ')';

    /**
     * XQuery namespace declaration for MarkLogic server status queries.
     * Required for querying server configuration via xdmp:server-status().
     */
    protected static final String DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER = "declare namespace mlss = 'http://marklogic.com/xdmp/status/server';\n";

    /**
     * XQuery version declaration for MarkLogic-specific XQuery 1.0.
     * Enables MarkLogic extensions to XQuery standard.
     */
    protected static final String XQUERY_VERSION_ML = "xquery version \"1.0-ml\";\n";

    /** Regular expression pattern for parsing XCC connection URIs */
    protected static final String XCC_CONNECTION_URI_PATTERN = "(xccs?)://((.+?):(.+?)@)?(.+?):(\\d*)(/.*)?(\\?(.+))?";

    /**
     * Decrypter instance for decrypting sensitive configuration values such as passwords,
     * connection URIs, and other encrypted properties.
     */
    protected Decrypter decrypter;

    /**
     * SSL configuration instance for managing SSL/TLS connections to MarkLogic.
     * Includes cipher suites, protocols, keystores, and truststores.
     */
    protected SSLConfig sslConfig;

    /**
     * Collection name to use when loading URIs.
     * May be used by URI loaders to filter or organize URI selection.
     */
    protected String collection;

    /**
     * Pool of XCC content sources for connecting to MarkLogic database.
     * Manages multiple connections and provides connection pooling for improved performance.
     */
    protected ContentSourcePool csp;

    /**
     * Transform options containing CoRB job configuration settings.
     * Includes module paths, batch sizes, thread counts, and other execution parameters.
     */
    protected TransformOptions options = new TransformOptions();

    /**
     * Configuration properties loaded from OPTIONS-FILE, system properties, or command-line.
     * Contains all job settings including connection parameters, module paths, and options.
     */
    protected Properties properties = new Properties();

    /**
     * Map of user-provided options from command-line or properties.
     * Excludes sensitive values (those containing "XCC", "PASSWORD", or "SSL").
     * Used for tracking and logging non-sensitive configuration.
     */
    protected Map<String, String> userProvidedOptions = new HashMap<>();

    /** Exit code for successful execution */
    protected static final int EXIT_CODE_SUCCESS = 0;
    /** Exit code for initialization errors */
    protected static final int EXIT_CODE_INIT_ERROR = 1;
    /** Exit code for processing errors */
    protected static final int EXIT_CODE_PROCESSING_ERROR = 2;

    /**
     * Space character constant used for joining strings and formatting output.
     */
    protected static final String SPACE = " ";

    /**
     * Logger instance for logging manager operations, errors, and diagnostic information.
     */
    private static final Logger LOG = Logger.getLogger(AbstractManager.class.getName());

    /**
     * Loads a properties file by name.
     * Throws an exception if the file is not found.
     *
     * @param filename the name of the properties file to load
     * @return the loaded Properties object
     * @throws IOException if an error occurs reading the file
     */
    public static Properties loadPropertiesFile(String filename) throws IOException {
        return loadPropertiesFile(filename, true);
    }

    /**
     * Loads a properties file by name.
     * Optionally throws an exception if the file is not found.
     *
     * @param filename the name of the properties file to load
     * @param excIfNotFound if true, throws an exception if file is not found
     * @return the loaded Properties object
     * @throws IOException if an error occurs reading the file
     */
    public static Properties loadPropertiesFile(String filename, boolean excIfNotFound) throws IOException {
        Properties properties = new Properties();
        return loadPropertiesFile(filename, excIfNotFound, properties);
    }

    /**
     * Loads a properties file into an existing Properties object.
     * Handles character encoding specified by OPTIONS_FILE_ENCODING.
     *
     * @param filename the name of the properties file to load
     * @param exceptionIfNotFound if true, throws an exception if file is not found
     * @param properties the Properties object to load into
     * @return the Properties object with loaded values
     * @throws IOException if an error occurs reading the file
     */
    protected static Properties loadPropertiesFile(String filename, boolean exceptionIfNotFound, Properties properties) throws IOException {
        String name = trim(filename);
        if (isNotBlank(name)) {
            Charset charset = Charset.defaultCharset();
            String encoding = System.getProperty(OPTIONS_FILE_ENCODING);
            if (encoding != null) {
                charset = Charset.forName(encoding);
            }
            loadPropertiesFile(name, charset, exceptionIfNotFound, properties);
        }
        return properties;
    }

    /**
     * Loads a properties file from classpath or filesystem.
     * First attempts to load from classpath, then falls back to filesystem.
     *
     * @param propertiesFilename the properties file name
     * @param encoding the character encoding to use
     * @param exceptionIfNotFound if true, throws an exception if file is not found
     * @param properties the Properties object to load into
     * @throws IOException if an error occurs reading the file
     */
    private static void loadPropertiesFile(String propertiesFilename, Charset encoding, boolean exceptionIfNotFound, @NotNull Properties properties) throws IOException {
        try (InputStream inputStream = Manager.class.getResourceAsStream('/' + propertiesFilename)) {
            if (inputStream != null) {
                LOG.log(INFO, () -> MessageFormat.format("Loading {0} from classpath", propertiesFilename));
                loadPropertiesFile(inputStream, encoding, propertiesFilename, exceptionIfNotFound, properties);
            } else {
                File f = new File(propertiesFilename);
                if (f.exists() && !f.isDirectory()) {
                    LOG.log(INFO, "Loading {0} from filesystem", propertiesFilename);
                    try (FileInputStream fileInputStream = new FileInputStream(f)) {
                        loadPropertiesFile(fileInputStream, encoding, propertiesFilename, exceptionIfNotFound, properties);
                    }
                } else if (exceptionIfNotFound) {
                    throw new IllegalStateException("Unable to load properties file " + propertiesFilename);
                }
            }
        }
    }

    /**
     * Loads properties from an input stream with character encoding support.
     * If OPTIONS_FILE_ENCODING is specified in the loaded properties and differs
     * from the encoding used, reloads the file with the correct encoding.
     *
     * @param inputStream the input stream to read from
     * @param encoding the character encoding to use
     * @param name the file name (for logging and potential reloading)
     * @param exceptionIfNotFound if true, throws an exception if file is not found
     * @param properties the Properties object to load into
     * @throws IOException if an error occurs reading the stream
     */
    private static void loadPropertiesFile(InputStream inputStream, Charset encoding, String name, boolean exceptionIfNotFound, @NotNull Properties properties) throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream, encoding) ) {
            properties.load(inputStreamReader);
            //If the OPTIONS-FILE-ENCODING specified which encoding to read it as, and it's different from what was already used,
            // then re-load it using the specified character encoding
            String optionsFileEncoding = properties.getProperty(OPTIONS_FILE_ENCODING);
            if (optionsFileEncoding != null ) {
                Charset optionsCharset = Charset.forName(optionsFileEncoding);
                if (!encoding.equals(optionsCharset)) {
                    LOG.log(INFO, "Reloading properties as {0} encoded", optionsCharset);
                    loadPropertiesFile(name, optionsCharset, exceptionIfNotFound, properties);
                }
            }
        }
    }

    /**
     * Reads an adhoc query module from classpath or filesystem.
     * The query content is returned as a trimmed string.
     *
     * @param module the module path to load
     * @return the query content as a string
     * @throws IllegalStateException if the module cannot be found or is a directory
     */
    public static String getAdhocQuery(String module) {
        InputStream is = null;
        try {
            is = TaskFactory.class.getResourceAsStream('/' + module);
            if (is == null) {
                File f = new File(module);
                if (f.exists() && !f.isDirectory()) {
                    is = Files.newInputStream(f.toPath());
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
        } finally {
            closeQuietly(is);
        }
    }

    /**
     * Gets the configuration properties.
     *
     * @return the Properties object
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Gets the transform options.
     *
     * @return the TransformOptions object
     */
    public TransformOptions getOptions() {
        return options;
    }

    /**
     * Checks if the command-line arguments contain a help flag.
     * Recognized flags: -h, --help, --usage (case-insensitive)
     *
     * @param args the command-line arguments
     * @return true if a help flag is present, false otherwise
     */
    public static boolean hasUsage(String... args) {
        return args != null &&
            Arrays.stream(args).anyMatch(arg ->
                "-h".equalsIgnoreCase(arg) ||
                "--help".equalsIgnoreCase(arg) ||
                "--usage".equalsIgnoreCase(arg));
    }

    /**
     * Returns a message about how to get help information.
     *
     * @return the help flag message
     */
    public static String getHelpFlagMessage() {
        return "For a full list of options and usage information, use commandline switch -h or --help or --usage";
    }

    /**
     * Initializes properties from the provided Properties object or from the options file.
     * If properties are null or empty, loads from the OPTIONS-FILE system property.
     * Merges system properties with loaded properties, giving precedence to system properties.
     *
     * @param props the Properties object to initialize with, may be null
     * @throws CorbException if an error occurs loading properties
     */
    public void initProperties(Properties props) throws CorbException {
        if (props == null || props.isEmpty()) {
            try {
                initPropertiesFromOptionsFile();
            } catch (IOException ex) {
                throw new CorbException("Failed to initialized properties from options file", ex);
            }
        } else {
            properties = props;
        }
        /*
         For each of the Options, if there are any System Properties specified and there is not already an entry in the properties, set it.
         This helps ensure that SSL properties are available in SSL-CONFIG-CLASS, which do not have the getOption() method
         */
        try {
            for (Field field : Options.class.getFields()) {
                if (String.class.equals(field.getType())) {
                    String optionKey = (String) field.get(null);
                    String optionValue = getOption(optionKey);
                    if (!properties.containsKey(optionKey) && optionValue != null) {
                        LOG.log(INFO, "applying system property: {0} to properties", optionKey);
                        properties.put(optionKey, optionValue);
                    }
                }
            }
        } catch (IllegalAccessException ex) {
            LOG.log(WARNING, "Unable to access Options class fields with reflection");
        }
    }

    /**
     * Initializes properties from the OPTIONS-FILE specified in system properties.
     *
     * @throws IOException if an error occurs reading the properties file
     */
    public void initPropertiesFromOptionsFile() throws IOException {
        String propsFileName = System.getProperty(OPTIONS_FILE);
        loadPropertiesFile(propsFileName,true, properties);
    }

    /**
     * Initializes the manager with command-line arguments.
     * Properties are loaded from the options file if not provided.
     *
     * @param args command-line arguments
     * @throws CorbException if initialization fails
     */
    public void init(String... args) throws CorbException {
        init(args, null);
    }

    /**
     * Initializes the manager with a Properties object.
     *
     * @param props the Properties object to initialize with
     * @throws CorbException if initialization fails
     */
    public void init(Properties props) throws CorbException {
        String[] args = {};
        init(args, props);
    }

    /**
     * Initializes the manager with command-line arguments and properties.
     * Performs the following initialization steps:
     * <ol>
     *   <li>Logs runtime arguments</li>
     *   <li>Initializes properties</li>
     *   <li>Initializes options</li>
     *   <li>Initializes decrypter</li>
     *   <li>Initializes SSL configuration</li>
     *   <li>Initializes content source pool</li>
     *   <li>Registers status information</li>
     * </ol>
     *
     * @param commandlineArgs command-line arguments, may be null
     * @param props the Properties object to initialize with, may be null
     * @throws CorbException if initialization fails at any step
     */
    public void init(String[] commandlineArgs, Properties props) throws CorbException {
        String[] args = commandlineArgs;
        if (args == null) {
            args = new String[0];
        }
        logRuntimeArgs();
        initProperties(props);
        initOptions(args);
        initDecrypter();
        initSSLConfig();
        initContentSourcePool(args.length > 0 ? args[0] : null);
        registerStatusInfo();
    }

    /**
     * Initializes the decrypter from the DECRYPTER option.
     * If no decrypter is specified, sets decrypter to null.
     * The decrypter class must implement the Decrypter interface.
     *
     * @throws CorbException if the decrypter class cannot be instantiated or initialized
     */
    protected void initDecrypter() throws CorbException {
        String decrypterClassName = getOption(DECRYPTER);
        if (decrypterClassName != null) {
            try {
                Class<?> decrypterCls = Class.forName(decrypterClassName);
                if (Decrypter.class.isAssignableFrom(decrypterCls)) {
                    decrypter = (Decrypter) decrypterCls.newInstance();
                    decrypter.init(properties);
                } else {
                    throw new IllegalArgumentException(DECRYPTER + " must be of type com.marklogic.developer.corb.Decrypter");
                }
            } catch (ClassNotFoundException | IOException | InstantiationException | IllegalAccessException ex) {
                throw new CorbException(MessageFormat.format("Unable to instantiate {0} {1}", SSL_CONFIG_CLASS, decrypterClassName), ex);
            }
        } else {
            decrypter = null;
        }
    }

    /**
     * Initializes the SSL configuration from the SSL_CONFIG_CLASS option.
     * If keystore options are set but SSL_CONFIG_CLASS is not, defaults to TwoWaySSLConfig.
     * If no SSL configuration is specified, defaults to TrustAnyoneSSLConfig.
     * Sets the decrypter on the SSL configuration.
     *
     * @throws CorbException if the SSL configuration class cannot be instantiated
     */
    protected void initSSLConfig() throws CorbException {
        String sslConfigClassName = getOption(SSL_CONFIG_CLASS);
        //If the keystore options are set, but the sslConfigClassName wasn't configured, assume that they wanted the TwoWaySSLConfig class configured
        if (getOption(SSL_KEYSTORE) != null &&
            (getOption(SSL_KEYSTORE_PASSWORD) !=null || getOption(SSL_KEY_PASSWORD) !=null) &&
            sslConfigClassName == null) {
            sslConfigClassName = TwoWaySSLConfig.class.getCanonicalName();
        }
        if (sslConfigClassName != null) {
            try {
                Class<?> decrypterCls = Class.forName(sslConfigClassName);
                if (SSLConfig.class.isAssignableFrom(decrypterCls)) {
                    sslConfig = (SSLConfig) decrypterCls.newInstance();
                    LOG.log(INFO, () -> MessageFormat.format("Using SSLConfig {0}", decrypterCls.getName()));
                } else {
                    throw new IllegalArgumentException("SSL Options must be of type com.marklogic.developer.corb.SSLConfig");
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                throw new CorbException(MessageFormat.format("Unable to instantiate {0} {1}", SSL_CONFIG_CLASS, sslConfigClassName), ex);
            }
        } else {
            LOG.log(INFO, () -> "Using TrustAnyoneSSSLConfig because no " + SSL_CONFIG_CLASS + " value specified.");
            sslConfig = new TrustAnyoneSSLConfig();
        }
        sslConfig.setProperties(properties);
        sslConfig.setDecrypter(decrypter);
    }

    /**
     * Initializes the content source pool with connection parameters.
     * Supports multiple connection methods:
     * <ul>
     *   <li>XCC connection URI (single or comma-separated list)</li>
     *   <li>Individual components (protocol, host, port, database, credentials)</li>
     *   <li>MarkLogic Cloud (API key, base path, grant type, token)</li>
     *   <li>OAuth (OAuth token)</li>
     * </ul>
     * Decrypts sensitive values if a decrypter is configured.
     *
     * @param uriArg optional URI argument from command line
     * @throws CorbException if connection parameters are invalid or pool initialization fails
     */
    protected void initContentSourcePool(String uriArg) throws CorbException{
        String uriAsStrings = getOption(uriArg, XCC_CONNECTION_URI);
        String protocol = getOption(XCC_PROTOCOL);
        String hostnames = getOption(XCC_HOSTNAME);
        String port = getOption(XCC_PORT);
        String dbname = getOption(XCC_DBNAME);
        //DIGEST or BASIC
        String username = getOption(XCC_USERNAME);
        String password = getOption(XCC_PASSWORD);
        //MarkLogic Cloud
        String apiKey = getOption(XCC_API_KEY);
        String basePath = getOption(XCC_BASE_PATH);
        String grantType = getOption(XCC_GRANT_TYPE);
        String tokenDuration = getOption(XCC_TOKEN_DURATION);
        String tokenEndpoint = getOption(XCC_TOKEN_ENDPOINT);
        //OAuth
        String oauthToken = getOption(XCC_OAUTH_TOKEN);

        if (anyIsNull(uriAsStrings)) {
            if (anyIsNull(hostnames, port)) {
                throw new CorbException(String.format("Either %1$s or %2$s and %3$s must be specified",
                    XCC_CONNECTION_URI, XCC_HOSTNAME, XCC_PORT));
            }
            if (anyIsNull(username, password) && anyIsNull(basePath, apiKey) && anyIsNull(oauthToken)) {
                LOG.warning(String.format("Either %1$s and %2$s or %3$s and %4$s or %5$s must be specified",
                    XCC_USERNAME, XCC_PASSWORD, XCC_BASE_PATH, XCC_API_KEY, XCC_OAUTH_TOKEN));
            }
        }
        List<String> connectionUriList = new ArrayList<>();
        String urlEncode = getOption(XCC_URL_ENCODE_COMPONENTS);
        if (uriAsStrings == null) {
            if (decrypter != null) {
                apiKey = decryptIfNotBlank(XCC_API_KEY, apiKey);
                basePath = decryptIfNotBlank(XCC_BASE_PATH, basePath);
                dbname = decryptIfNotBlank(XCC_DBNAME, dbname);
                grantType = decryptIfNotBlank(XCC_GRANT_TYPE, grantType);
                oauthToken = decryptIfNotBlank(XCC_OAUTH_TOKEN, oauthToken);
                password = decryptIfNotBlank(XCC_PASSWORD, password);
                port = decryptIfNotBlank(XCC_PORT, port);
                tokenDuration = decryptIfNotBlank(XCC_TOKEN_DURATION, tokenDuration);
                tokenEndpoint = decryptIfNotBlank(XCC_TOKEN_ENDPOINT, tokenEndpoint);
                username = decryptIfNotBlank(XCC_USERNAME, username);
            }
            Map<String, String> xccConnectionParameters = new HashMap<>();
            putIfNotBlank(xccConnectionParameters, XCC_API_KEY, apiKey);
            putIfNotBlank(xccConnectionParameters, XCC_BASE_PATH, basePath);
            putIfNotBlank(xccConnectionParameters, XCC_DBNAME, dbname);
            putIfNotBlank(xccConnectionParameters, XCC_GRANT_TYPE, grantType);
            putIfNotBlank(xccConnectionParameters, XCC_OAUTH_TOKEN, oauthToken);
            putIfNotBlank(xccConnectionParameters, XCC_PASSWORD, password);
            putIfNotBlank(xccConnectionParameters, XCC_PROTOCOL, protocol);
            putIfNotBlank(xccConnectionParameters, XCC_PORT, port);
            putIfNotBlank(xccConnectionParameters, XCC_TOKEN_DURATION, tokenDuration);
            putIfNotBlank(xccConnectionParameters, XCC_TOKEN_ENDPOINT, tokenEndpoint);
            putIfNotBlank(xccConnectionParameters, XCC_USERNAME, username);

            for (String host: commaSeparatedValuesToList(hostnames)) {
                if (decrypter != null) {
                    host = decrypter.decrypt(XCC_HOSTNAME, host);
                }
                xccConnectionParameters.put(XCC_HOSTNAME, host);
                String connectionUri = getXccUri(xccConnectionParameters, urlEncode);
                connectionUriList.add(connectionUri);
            }
        } else {
            for (String connectionUri : commaSeparatedValuesToList(uriAsStrings)) {
                if (decrypter != null) {
                    connectionUri = decrypter.decrypt(XCC_CONNECTION_URI, connectionUri);
                    if (connectionUri != null) {
                        //see if individual parts of the connection string are encrypted separately
                        connectionUri = tryToDecryptUriInParts(connectionUri, urlEncode);
                    }
                }
                if (connectionUri != null) {
                    connectionUriList.add(connectionUri);
                }
            }
        }

        csp = createContentSourcePool();
        LOG.info("Using the content source manager " + csp.getClass().getName());
        csp.init(properties, sslConfig, connectionUriList.toArray(new String[connectionUriList.size()]));

        if (!csp.available()) {
            throw new CorbException("No connections available. Please check connection parameters or initialization errors");
        }
    }

    /**
     * Decrypts a value if it is not blank.
     *
     * @param key the property key (used by decrypter)
     * @param value the value to decrypt
     * @return the decrypted value, or the original value if blank
     */
    private String decryptIfNotBlank(String key, String value) {
        if (isNotBlank(value)) {
            value = decrypter.decrypt(key, value);
        }
        return value;
    }

    /**
     * Adds a key-value pair to the map if the value is not blank.
     *
     * @param map the map to add to
     * @param key the key
     * @param value the value
     */
    private void putIfNotBlank(Map<String, String> map, String key, String value) {
      if (isNotBlank(value)) {
          map.put(key, value);
      }
    }

    /**
     * Attempts to decrypt individual parts of an XCC connection URI.
     * Parses the URI using a regular expression and decrypts each component
     * (protocol, username, password, host, port, database, query parameters).
     * Reconstructs the URI with decrypted values.
     *
     * @param connectionUri the XCC connection URI to decrypt
     * @param urlEncode the URL encoding strategy
     * @return the URI with decrypted components, or the original URI if parsing fails
     */
    protected String tryToDecryptUriInParts(String connectionUri, String urlEncode) {
        LOG.info("Checking if any part of the connection string are encrypted");
        String uriAfterDecrypt = connectionUri;
        Pattern pattern = Pattern.compile(XCC_CONNECTION_URI_PATTERN);
        try {
            Matcher matcher = pattern.matcher(connectionUri);

            if (matcher.matches() && matcher.groupCount() >= 6) {
                String protocol = matcher.group(1);
                String username = matcher.group(3);
                String password = matcher.group(4);
                String host = matcher.group(5);
                String port = matcher.group(6);
                String dbname = matcher.groupCount() > 6 ? matcher.group(7) : null;
                if (dbname != null && dbname.startsWith("/")) {
                    dbname = dbname.substring(1);
                }
                String query = matcher.groupCount() > 8 ? matcher.group(9) : EMPTY;

                if (isNotBlank(protocol) && isNotBlank(host) && isNotBlank(port) && NumberUtils.toInt(port) > 0) {
                    Map<String, String> urlComponents = new HashMap<>();
                    urlComponents.put(XCC_PROTOCOL, protocol);
                    host = decrypter.decrypt(XCC_HOSTNAME, host);
                    urlComponents.put(XCC_HOSTNAME, host);
                    urlComponents.put(XCC_PORT, port);
                    dbname = isBlank(dbname) ? EMPTY : decrypter.decrypt(XCC_DBNAME, dbname);
                    urlComponents.put(XCC_DBNAME, dbname);
                    if (isNotBlank(username)) {
                        username = decrypter.decrypt(XCC_USERNAME, username);
                        urlComponents.put(XCC_USERNAME, username);
                    }
                    if (isNotBlank(password)) {
                        password = decrypter.decrypt(XCC_PASSWORD, password);
                        urlComponents.put(XCC_PASSWORD, password);
                    }
                    // Parse querystring parameters
                    if (query != null) {
                        String[] pairs = query.split("&");
                        for (String pair : pairs) {
                            int i = pair.indexOf('=');
                            if (i > 0) {
                                try {
                                    String paramName = URLDecoder.decode(pair.substring(0, i), "UTF-8").toLowerCase();
                                    String key;
                                    switch (paramName) {
                                        case "apikey":
                                            key = XCC_API_KEY;
                                            break;
                                        case "basepath":
                                            key = XCC_BASE_PATH;
                                            break;
                                        case "granttype":
                                            key = XCC_GRANT_TYPE;
                                            break;
                                        case "oauthtoken":
                                            key = XCC_OAUTH_TOKEN;
                                            break;
                                        case "tokenduration":
                                            key = XCC_TOKEN_DURATION;
                                            break;
                                        case "tokenendpoint":
                                            key = XCC_TOKEN_ENDPOINT;
                                            break;
                                        default:
                                            key = null;
                                    }
                                    //we found a querystring parameter that maps to an XCC option
                                    if (key != null) {
                                        String value = pair.substring(i + 1);
                                        if (isNotBlank(value)) {
                                            value = URLDecoder.decode(value, "UTF-8");
                                            value = decrypter.decrypt(key, value);
                                            urlComponents.put(key, value);
                                        }
                                    }
                                } catch (UnsupportedEncodingException e) {
                                    LOG.log(SEVERE, "Unsupported encoding in XCC URI for " + pair, e);
                                    throw new IllegalStateException(e);
                                }
                            }
                        }
                    }
                    uriAfterDecrypt = getXccUri(urlComponents, urlEncode);
                }
            }
        } catch (IllegalStateException exc) {
           LOG.log(WARNING,"Unable to parse connection URI "+ exc.getMessage());
        }
        return uriAfterDecrypt;
    }

    /**
     * Creates a content source pool instance.
     * If CONTENT_SOURCE_POOL option is specified, instantiates that class.
     * Otherwise, creates a DefaultContentSourcePool.
     *
     * @return the ContentSourcePool instance
     * @throws CorbException if the pool cannot be created
     */
    protected ContentSourcePool createContentSourcePool() throws CorbException {
        ContentSourcePool contentSourcePool;
        String contentSourcePoolClassName = getOption(CONTENT_SOURCE_POOL);
        if (contentSourcePoolClassName != null) {
            contentSourcePool = createContentSourcePool(contentSourcePoolClassName);
        } else {
            contentSourcePool = new DefaultContentSourcePool();
        }
        return contentSourcePool;
    }

    /**
     * Creates a content source pool instance from a class name.
     * The class must implement ContentSourcePool interface.
     *
     * @param className the fully qualified class name
     * @return the ContentSourcePool instance
     * @throws CorbException if the class cannot be found or instantiated
     */
    protected ContentSourcePool createContentSourcePool(String className) throws CorbException{
        try {
            Class<?> cls = Class.forName(className);
            if (ContentSourcePool.class.isAssignableFrom(cls)) {
                return cls.asSubclass(ContentSourcePool.class).newInstance();
            } else {
                throw new CorbException("ConnectionManager class " + className + " must be of type com.marklogic.developer.corb.Task");
            }
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException exc) {
            throw new CorbException("Exception while creating the ContentSourcePool class " + className,exc);
        }
    }

    /**
     * Gets the content source pool.
     *
     * @return the ContentSourcePool instance
     */
    public ContentSourcePool getContentSourcePool() {
        return csp;
    }

    /**
     * Initializes XCC options.
     * Sets the xcc.httpcompliant system property based on XCC_HTTPCOMPLIANT option.
     * Defaults to true if not explicitly set to ensure compatibility with load balancers.
     *
     * @param args command-line arguments
     * @throws CorbException if initialization fails
     */
    protected void initOptions(String... args) throws CorbException {
        final String xccHttpCompliantPropertyName = "xcc.httpcompliant";
        final String xccHttpCompliantOption = getOption(XCC_HTTPCOMPLIANT);
        if (isNotBlank(xccHttpCompliantOption)) {
            System.setProperty(xccHttpCompliantPropertyName, Boolean.toString(stringToBoolean(xccHttpCompliantOption)));
        } else { // if not explicitly set as a system property or option, enable HTTP compliance so that we play nice with load balancers out of the box
            System.setProperty(xccHttpCompliantPropertyName, Boolean.toString(true));
        }
    }

    /**
     * Registers status information by querying MarkLogic server status.
     * Retrieves modules database and XDBC root settings.
     * Logs options and properties after registration.
     *
     * @throws CorbException if the status query fails
     */
    protected void registerStatusInfo() throws CorbException {
        ContentSource contentSource = csp.get();
        ResultSequence resultSequence = null;
        try (Session session = contentSource.newSession()) {

            AdhocQuery q = session.newAdhocQuery(XQUERY_VERSION_ML + DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
                    + "let $status := xdmp:server-status(xdmp:host(), xdmp:server())\n"
                    + "let $modules := $status/mlss:modules\n"
                    + "let $root := $status/mlss:root\n"
                    + "return (data($modules), data($root))");
            resultSequence = session.submitRequest(q);

            while (null != resultSequence && resultSequence.hasNext()) {
                ResultItem rsItem = resultSequence.next();
                XdmItem item = rsItem.getItem();
                if (item != null) {
                    if (rsItem.getIndex() == 0 && "0".equals(item.asString())) {
                        options.setModulesDatabase("");
                    }
                    if (rsItem.getIndex() == 1) {
                        options.setXDBC_ROOT(item.asString());
                    }
                }
            }
        } catch (RequestException e) {
            LOG.log(SEVERE, "registerStatusInfo request failed", e);
        } finally {
            if (null != resultSequence && !resultSequence.isClosed()) {
                resultSequence.close();
            }
        }
        logOptions();
        logProperties();
    }

    /**
     * Logs options.
     * Default implementation does nothing; subclasses may override.
     */
    protected void logOptions() {
        //default behavior is not to log anything
    }

    /**
     * Logs non-XCC properties.
     * Properties with keys starting with "XCC-" are excluded from logging.
     */
    protected void logProperties() {
        for (Entry<Object, Object> e : properties.entrySet()) {
            if (e.getKey() != null && !e.getKey().toString().toUpperCase().startsWith("XCC-")) {
                LOG.log(INFO, () -> MessageFormat.format("Loaded property {0}={1}", e.getKey(), e.getValue()));
            }
        }
    }

    /**
     * Retrieves the value of the specified property from System properties or the properties object.
     * System properties take precedence over properties object.
     * The value is trimmed before returning.
     *
     * @param propertyName the property name
     * @return the trimmed property value, or null if not found
     */
    protected String getOption(String propertyName) {
        return getOption(null, propertyName);
    }

    /**
     * Retrieves a property value from command-line arguments or properties.
     * Command-line arguments take precedence over properties.
     * The value is trimmed before returning.
     *
     * @param commandlineArgs the command-line arguments array
     * @param argIndex the index of the argument to retrieve
     * @param propertyName the property name to fall back to
     * @return the trimmed property value, or null if not found
     */
    protected String getOption(String[] commandlineArgs, int argIndex, String propertyName) {
        String argValue = commandlineArgs.length > argIndex ? commandlineArgs[argIndex] : null;
        return getOption(argValue, propertyName);
    }

    /**
     * Retrieves a property value from an argument value or properties.
     * Argument value takes precedence over properties.
     * Non-sensitive values are tracked in userProvidedOptions.
     * Values containing "XCC", "PASSWORD", or "SSL" are considered sensitive.
     *
     * @param argVal the argument value, may be null
     * @param propertyName the property name to fall back to
     * @return the trimmed property value, or null if not found
     */
    protected String getOption(String argVal, String propertyName) {
        String retVal = null;
        if (isNotBlank(argVal)) {
            retVal = argVal.trim();
        } else {
            String property = findOption(properties, propertyName);
            if (isNotBlank(property)) {
                retVal = property;
            }
        }
        //doesn't capture defaults, only user provided.
        String[] secureWords = {"XCC", "PASSWORD", "SSL"};
        boolean hasSecureWords = false;
        for (String secureWord : secureWords) {
            if (retVal != null && retVal.toUpperCase().contains(secureWord) || propertyName.toUpperCase().contains(secureWord)) {
                hasSecureWords = true;
                break;
            }
        }
        if (retVal != null && !hasSecureWords) {
            userProvidedOptions.put(propertyName, retVal);
        }
        return retVal;
    }

    /**
     * Displays usage information to System.err.
     * Shows version information, configuration precedence rules,
     * and all available options with descriptions from @Usage annotations.
     */
    protected void usage() {
        PrintStream err = System.err;
        err.println("CoRB " + VERSION_MSG + " requires options to be specified through one or more of the following mechanisms:\n"
                + "1.) command-line parameters\n"
                + "2.) Java system properties ex: -DXCC-CONNECTION-URI=xcc://user:password@localhost:8202\n"
                + "3.) As properties file in the class path specified using -DOPTIONS-FILE=myjob.properties. "
                + "Relative and full file system paths are also supported.\n"
                + "If specified in more than one place, a command line parameter takes precedence over "
                + "a Java system property, which take precedence over a property "
                + "from the OPTIONS-FILE properties file.\n\n"
                + "CoRB Options:\n"); // NOPMD

        for (Field field : Options.class.getDeclaredFields()) {
            Usage usage = field.getAnnotation(Usage.class);
            if (usage != null && isNotEmpty(usage.description())) {
                err.println(field.getName() + "\n\t" + usage.description()); // NOPMD
            }
        }

        err.println("\nPlease report issues at: https://github.com/marklogic-community/corb2/issues\n"); // NOPMD
    }

    /**
     * Builds a system property argument string in the format "-Dproperty=value".
     * If value is empty, returns "-Dproperty" without the equals sign.
     *
     * @param property the property name
     * @param value the property value, may be null or empty
     * @return the formatted system property argument
     */
    protected String buildSystemPropertyArg(String property, String value) {
        StringBuilder arg = new StringBuilder("-D");
        arg.append(property);
        if (isNotEmpty(value)) {
            arg.append('=');
            arg.append(value);
        }
        return arg.toString();
    }

    /**
     * Logs runtime JVM arguments.
     * Excludes arguments containing "XCC" or "PASSWORD" for security.
     */
    protected void logRuntimeArgs() {
        RuntimeMXBean runtimemxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimemxBean.getInputArguments();
        List<String> argsToLog = new ArrayList<>(arguments.size());
        for (String argument : arguments) {
            if (!argument.startsWith("-DXCC") && !argument.toUpperCase().contains("PASSWORD")) {
                argsToLog.add(argument);
            }
        }
        LOG.log(INFO, () -> MessageFormat.format("runtime arguments = {0}", join(argsToLog, SPACE)));
    }

    /**
     * Sets the user-provided options map.
     *
     * @param userProvidedOptions map of user-provided option names to values
     */
    public void setUserProvidedOptions(Map<String, String> userProvidedOptions) {
        this.userProvidedOptions = userProvidedOptions;
    }

    /**
     * Creates an XCC Request object for a module.
     * Handles three types of modules:
     * <ul>
     *   <li>Inline modules: code embedded in the module string</li>
     *   <li>Adhoc modules: query loaded from file with |ADHOC suffix</li>
     *   <li>Installed modules: module path in MarkLogic modules database</li>
     * </ul>
     *
     * @param processModule the module specification
     * @param session the XCC session
     * @return the Request object for the module
     * @throws IllegalStateException if the module cannot be loaded
     */
    protected Request getRequestForModule(String processModule, Session session) {
        Request request;
        if (isInlineOrAdhoc(processModule)) {
            String adhocQuery;
            if (isInlineModule(processModule)) {
                adhocQuery = getInlineModuleCode(processModule);
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
                LOG.log(INFO, () -> MessageFormat.format("invoking adhoc process module {0}", queryPath));
            }
            request = session.newAdhocQuery(adhocQuery);

        } else {
            String root = options.getModuleRoot();
            String modulePath = buildModulePath(root, processModule);
            LOG.log(INFO, () -> MessageFormat.format("invoking module {0}", modulePath));
            request = session.newModuleInvoke(modulePath);
        }
        return request;
    }

    /**
     * Gets the user-provided options map.
     * Does not include sensitive options (containing "XCC", "PASSWORD", or "SSL").
     *
     * @return map of user-provided option names to values
     */
    public Map<String, String> getUserProvidedOptions() {
        return userProvidedOptions;
    }
}
