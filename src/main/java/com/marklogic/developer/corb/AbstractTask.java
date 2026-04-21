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

import static com.marklogic.developer.corb.Manager.DEFAULT_BATCH_URI_DELIM;
import static com.marklogic.developer.corb.Manager.URIS_BATCH_REF;
import static com.marklogic.developer.corb.Options.BATCH_SIZE;
import static com.marklogic.developer.corb.Options.BATCH_URI_DELIM;
import static com.marklogic.developer.corb.Options.ERROR_FILE_NAME;
import static com.marklogic.developer.corb.Options.LOADER_VARIABLE;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_ERROR_CODES;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_ERROR_MESSAGE;
import static com.marklogic.developer.corb.TransformOptions.FAILED_URI_TOKEN;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.commaSeparatedValuesToList;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;

import com.marklogic.developer.corb.util.XmlUtils;
import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.QueryException;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import com.marklogic.xcc.exceptions.RetryableQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XName;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Abstract base class for CoRB task implementations.
 * Provides common functionality for processing URIs including request generation,
 * error handling, retry logic, and result processing. Tasks are executed concurrently
 * to process batches of URIs against MarkLogic modules.
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Automatic retry logic for transient failures</li>
 *   <li>Configurable error handling (fail-fast or continue)</li>
 *   <li>Support for both installed modules and adhoc queries</li>
 *   <li>Request variable configuration (URI or DOC mode)</li>
 *   <li>Error logging to file</li>
 *   <li>Thread name management for monitoring</li>
 * </ul>
 *
 * <p>Subclasses must implement {@link #processResult(ResultSequence)} to handle
 * the results returned from MarkLogic.</p>
 *
 * @author Bhagat Bandlamudi, MarkLogic Corporation
  * @since 2.0.0
 */
public abstract class AbstractTask implements Task {

    /**
     * Synchronization object for thread-safe error file writing operations.
     * Ensures that multiple concurrent tasks do not interfere when writing to the error file.
     */
    private static final Object ERROR_SYNC_OBJ = new Object();

    /**
     * Synchronization object for thread-safe restart journal writes.
     */
    private static final Object RESTART_STATE_SYNC_OBJ = new Object();

    /**
     * String constant for boolean true value.
     * Used for property and configuration comparisons.
     */
    protected static final String TRUE = "true";

    /**
     * String constant for boolean false value.
     * Used for property and configuration comparisons.
     */
    protected static final String FALSE = "false";

    /**
     * Request variable name for DOC mode.
     * When LOADER_VARIABLE is set to "DOC", input URIs are parsed and passed as document nodes.
     */
    protected static final String REQUEST_VARIABLE_DOC = "DOC";

    /**
     * Request variable name for URI mode.
     * Default mode where input URIs are passed as delimited strings.
     */
    protected static final String REQUEST_VARIABLE_URI = "URI";

    /**
     * Platform-specific newline byte sequence.
     * Obtained from system property "line.separator" or defaults to "\n".
     * Used for writing line breaks to error files.
     */
    protected static final byte[] NEWLINE = System.getProperty("line.separator") != null ?
        System.getProperty("line.separator").getBytes(StandardCharsets.UTF_8) : "\n".getBytes(StandardCharsets.UTF_8);

    /**
     * Empty byte array constant to avoid repeated allocation.
     * Returned when converting null XdmItems to byte arrays.
     */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Content source pool for obtaining XCC sessions to MarkLogic.
     * Shared across all tasks and managed by the CoRB manager.
     */
    protected ContentSourcePool csp;

    /**
     * Module type identifier (e.g., "PROCESS-MODULE", "PRE-BATCH-MODULE").
     * Used to identify custom module-specific properties.
     */
    protected String moduleType;

    /**
     * URI path of the installed module in MarkLogic modules database.
     * Null if using adhoc query mode.
     */
    protected String moduleUri;

    /**
     * Configuration properties for the task including custom module variables,
     * retry limits, error handling settings, and other job parameters.
     */
    protected Properties properties = new Properties();

    /**
     * Array of input URIs to be processed by this task.
     * May contain single or multiple URIs depending on batch size configuration.
     */
    protected String[] inputUris;

    /**
     * Adhoc query code to execute instead of an installed module.
     * Null if using installed module mode.
     */
    protected String adhocQuery;

    /**
     * Query language for the module (e.g., "xquery" or "javascript").
     * Used to set the RequestOptions language.
     */
    protected String language;

    /**
     * Timezone for query execution.
     * Applied to XCC RequestOptions if configured.
     */
    protected TimeZone timeZone;

    /**
     * Export directory path for writing output files.
     * Used by tasks that export results to the filesystem.
     */
    protected String exportDir;

    /**
     * Default interval in seconds to wait between query retry attempts.
     * Used when {@link Options#QUERY_RETRY_INTERVAL} is not specified.
     */
    protected static final int DEFAULT_QUERY_RETRY_INTERVAL = 20;

    /**
     * Default maximum number of query retry attempts.
     * Used when {@link Options#QUERY_RETRY_LIMIT} is not specified.
     */
    protected static final int DEFAULT_QUERY_RETRY_LIMIT = 2;

    /**
     * Current number of retry attempts for the current invocation.
     * Reset to 0 after successful execution or when retry limit is exceeded.
     */
    protected int retryCount = 0;

    /** Whether to fail fast on errors or continue processing */
    protected boolean failOnError = true;

    /**
     * Logger instance for logging task execution, errors, retries, and diagnostic information.
     */
    private static final Logger LOG = Logger.getLogger(AbstractTask.class.getName());

    /**
     * String constant for error messages indicating URI context.
     * Used to append URI information to exception messages.
     */
    private static final String AT_URI = " at URI: ";

    /**
     * Sets the content source pool for database connections.
     *
     * @param csp the ContentSourcePool instance
     */
    @Override
    public void setContentSourcePool(ContentSourcePool csp) {
        this.csp = csp;
    }

    /**
     * Sets the module type (e.g., "PROCESS-MODULE").
     *
     * @param moduleType the module type identifier
     */
    @Override
    public void setModuleType(String moduleType) {
        this.moduleType = moduleType;
    }

    /**
     * Sets the URI of the installed module in MarkLogic.
     *
     * @param moduleUri the module URI path
     */
    @Override
    public void setModuleURI(String moduleUri) {
        this.moduleUri = moduleUri;
    }

    /**
     * Sets the adhoc query code to execute.
     *
     * @param adhocQuery the query code as a string
     */
    @Override
    public void setAdhocQuery(String adhocQuery) {
        this.adhocQuery = adhocQuery;
    }

    /**
     * Sets the query language (XQuery or JavaScript).
     *
     * @param language the query language
     */
    @Override
    public void setQueryLanguage(String language) {
        this.language = language;
    }

    /**
     * Sets the timezone for query execution.
     *
     * @param timeZone the TimeZone to use
     */
    @Override
    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    /**
     * Sets the configuration properties.
     *
     * @param properties the Properties object
     */
    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Sets the input URIs to process.
     * The array is cloned to prevent external modification.
     *
     * @param inputUri variable number of URI strings
     */
    @Override
    public void setInputURI(String... inputUri) {
        this.inputUris = inputUri != null ? inputUri.clone() : new String[]{};
    }

    /**
     * Sets whether to fail fast on errors or continue processing.
     *
     * @param failOnError true to throw exceptions, false to log and continue
     */
    @Override
    public synchronized void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    /**
     * Sets the export directory for output files.
     *
     * @param exportFileDir the directory path
     */
    @Override
    public void setExportDir(String exportFileDir) {
        this.exportDir = exportFileDir;
    }

    /**
     * Gets the export directory.
     *
     * @return the export directory path
     */
    public String getExportDir() {
        return this.exportDir;
    }

    /**
     * Creates a new XCC session from the content source pool.
     *
     * @return a new Session instance
     * @throws CorbException if a session cannot be created
     */
    public Session newSession() throws CorbException{
        ContentSource contentSource = csp.get();
        if (contentSource == null) {
            throw new CorbException("Unable to obtain ContentSource from pool");
        }
        return contentSource.newSession();
    }

    /**
     * Main entry point for task execution (Callable interface).
     * Invokes the module and ensures cleanup is performed.
     *
     * @return array of processed input URIs
     * @throws Exception if an error occurs during execution
     */
    @Override
    public String[] call() throws Exception {
        try {
            return invokeModule();
        } finally {
            cleanup();
        }
    }

    /**
     * Invokes the configured module (adhoc or installed) with the input URIs.
     * Handles session creation, request generation, execution, and error handling.
     * Implements retry logic for transient failures.
     * Thread yields are used throughout to avoid thread starvation.
     *
     * @return array of processed input URIs
     * @throws CorbException if an unrecoverable error occurs
     */
    protected String[] invokeModule() throws CorbException {
        if (moduleUri == null && adhocQuery == null) {
            return new String[0];
        }

        ResultSequence seq = null;
        Thread.yield();// try to avoid thread starvation
        try (Session session = newSession()) {

            Request request = generateRequest(session);
            //This is how the long-running uris can be populated
            Thread.currentThread().setName(urisAsString(inputUris));

            Thread.yield();// try to avoid thread starvation
            synchronized (session) {
                seq = session.submitRequest(request);
                retryCount = 0;
            }

            Thread.yield();// try to avoid thread starvation
            processResult(seq);
            seq.close();

            Thread.yield();// try to avoid thread starvation
            writeCompletedUrisToRestartState(inputUris);

            return inputUris;
        } catch (RequestException exc) {
            return handleRequestException(exc);
        } catch (Exception exc) {
            throw  wrapProcessException(exc, inputUris);
        } finally {
            if (null != seq && !seq.isClosed()) {
                seq.close();
            }
            Thread.yield();// try to avoid thread starvation
        }
    }

    /**
     * Generates an XCC Request object for the module.
     * Configures request options (language, timezone) and sets request variables:
     * <ul>
     *   <li>URI or DOC variable based on LOADER_VARIABLE setting</li>
     *   <li>URIS_BATCH_REF if configured</li>
     *   <li>Custom module-specific variables</li>
     * </ul>
     *
     * @param session the XCC session
     * @return configured Request object
     * @throws CorbException if request generation fails
     */
    protected Request generateRequest(Session session) throws CorbException {
        Request request;
        //determine whether this is an eval or execution of installed module
        if (moduleUri == null) {
            request = session.newAdhocQuery(adhocQuery);
        } else {
            request = session.newModuleInvoke(moduleUri);
        }

        RequestOptions requestOptions = request.getOptions();
        if (isNotBlank(language)) {
            requestOptions.setQueryLanguage(language);
        }
        if (timeZone != null) {
            requestOptions.setTimeZone(timeZone);
        }

        if (inputUris != null && inputUris.length > 0) {
            if (REQUEST_VARIABLE_DOC.equalsIgnoreCase(getProperty(LOADER_VARIABLE))) {
                setDocRequestVariable(request, inputUris);
            } else {
                setUriRequestVariable(request, inputUris);
            }
        }

        if (properties != null && properties.containsKey(URIS_BATCH_REF)) {
            request.setNewStringVariable(URIS_BATCH_REF, getProperty(URIS_BATCH_REF));
        }

        setCustomInputs(request);
        return request;
    }

    /**
     * Sets custom input variables on the request based on properties that start with the module type prefix.
     * For each property named "&lt;moduleType&gt;.&lt;varName&gt;", a request variable named "&lt;varName&gt;" is set with the property value.
     *
     * @param request the XCC Request to set variables on
     */
    private void setCustomInputs(Request request) {
        for (String customInputPropertyName : getCustomInputPropertyNames()) {
            String varName = customInputPropertyName.substring(moduleType.length() + 1);
            String value = getProperty(customInputPropertyName);
            if (value != null) {
                request.setNewStringVariable(varName, value);
            }
        }
    }

    /**
     * Sets the URI request variable with batch URIs joined by delimiter.
     *
     * @param request the XCC request
     * @param inputUris the URIs to set as a string variable
     */
    protected void setUriRequestVariable(Request request, String... inputUris) {
        String delim = getBatchUriDelimiter();
        String uriValue = StringUtils.join(inputUris, delim);
        request.setNewStringVariable(REQUEST_VARIABLE_URI, uriValue);
    }

    /**
     * Sets the DOC request variable with parsed document content.
     * Only supports batch size of 1 due to XCC limitations.
     *
     * @param request the XCC request
     * @param inputUris the URIs to set as document variable
     * @throws CorbException if batch size is greater than 1 or parsing fails
     */
    protected void setDocRequestVariable(Request request, String... inputUris) throws CorbException {
        String batchSize = properties.getProperty(BATCH_SIZE);
        //XCC does not allow sequences for request parameters
        if (batchSize != null && Integer.parseInt(batchSize) > 1) {
            throw new CorbException("Cannot set BATCH-SIZE > 1 with REQUEST-VARIABLE-DOC. XCC does not allow sequences for request parameters.");
        }
        XdmItem[] xdmItems = toXdmItems(inputUris);

        XName name = new XName(REQUEST_VARIABLE_DOC);
        request.setVariable(ValueFactory.newVariable(name, xdmItems[0]));
    }

    /**
     * Converts input URI strings to XdmItem array.
     * Attempts to parse as XML documents; falls back to text nodes if parsing fails.
     *
     * @param inputUris the input URI strings
     * @return array of XdmItem representing the inputs
     * @throws CorbException if document creation fails
     */
    protected XdmItem[] toXdmItems(String... inputUris) throws CorbException {
        List<XdmItem> docs = new ArrayList<>(inputUris.length);
        try {
            DocumentBuilder builder = XmlUtils.newSecureDocumentBuilderFactoryInstance().newDocumentBuilder();
            for (String input : inputUris) {
                XdmItem doc = toXdmItem(builder, input);
                docs.add(doc);
            }
        } catch (ParserConfigurationException ex) {
            throw new CorbException("Unable to parse loader document", ex);
        }
        return docs.toArray(new XdmItem[0]);
    }

    /**
     * Converts a single input string to an XdmItem.
     * Tries to parse as XML if input looks like XML (starts with '&lt;' and ends with '&gt;').
     * Falls back to creating a text document node if XML parsing fails.
     *
     * @param builder the DocumentBuilder for XML parsing
     * @param input the input string
     * @return XdmItem representation of the input
     * @throws CorbException if document creation fails
     */
    protected XdmItem toXdmItem(DocumentBuilder builder, String input) throws CorbException {
        String normalizedInput = StringUtils.trimToEmpty(input);
        XdmItem doc;
        //it appears to be XML, let's see if we can parse it
        if (normalizedInput.startsWith("<") && normalizedInput.endsWith(">")) {
            try {
                InputSource is = new InputSource(new StringReader(normalizedInput));
                Document dom = builder.parse(is);
                doc = ValueFactory.newDocumentNode(dom);
            } catch (SAXException | IOException ex) {
                LOG.log(WARNING, "Unable to parse URI as XML. Setting content as text.", ex);
                //guess not, lets just use it as-is
                doc = ValueFactory.newDocumentNode(input);
            }
        } else {
            //assume that it is just plain text, use the original value
            doc = ValueFactory.newDocumentNode(input);
        }
        return doc;
    }

    /**
     * Gets custom input property names for this module type.
     * Returns property names that start with "&lt;moduleType&gt;."
     * Searches both instance properties and system properties.
     *
     * @return set of custom property names
     */
    protected Set<String> getCustomInputPropertyNames() {
        Set<String> moduleCustomInputPropertyNames = new HashSet<>();
        if (moduleType == null) {
            return moduleCustomInputPropertyNames;
        }
        if (properties != null) {
            for (String propName : properties.stringPropertyNames()) {
                if (propName.startsWith(moduleType + '.')) {
                    moduleCustomInputPropertyNames.add(propName);
                }
            }
        }
        for (String propName : System.getProperties().stringPropertyNames()) {
            if (propName.startsWith(moduleType + '.')) {
                moduleCustomInputPropertyNames.add(propName);
            }
        }
        return moduleCustomInputPropertyNames;
    }

    /**
     * Determines if a RequestException should be retried.
     * Retryable conditions include:
     * <ul>
     *   <li>RetryableQueryException</li>
     *   <li>RequestPermissionException with retry advised</li>
     *   <li>QueryException with retryable error code</li>
     *   <li>Exception message matching QUERY_RETRY_ERROR_MESSAGE patterns</li>
     * </ul>
     *
     * @param requestException the exception to evaluate
     * @return true if the exception should be retried
     */
    protected boolean shouldRetry(RequestException requestException) {
        return requestException instanceof RetryableQueryException
                || requestException instanceof RequestPermissionException && shouldRetry((RequestPermissionException) requestException)
                || requestException instanceof QueryException && shouldRetry((QueryException) requestException)
                || hasRetryableMessage(requestException);
    }

    /**
     * Checks if the exception message matches configured retryable message patterns.
     *
     * @param requestException the exception to check
     * @return true if the message contains a retryable pattern
     */
    protected boolean hasRetryableMessage(RequestException requestException) {
        String message = requestException.getMessage();
        List<String> retryableMessages = commaSeparatedValuesToList(getProperty(QUERY_RETRY_ERROR_MESSAGE));
        for (String messageFragment : retryableMessages) {
            if (message.contains(messageFragment)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines if a QueryException should be retried.
     * Checks if the exception is marked retryable or if the error code
     * matches QUERY_RETRY_ERROR_CODES configuration.
     *
     * @param queryException the exception to evaluate
     * @return true if the exception should be retried
     */
    protected boolean shouldRetry(QueryException queryException) {
        String errorCode = queryException.getCode();
        List<String> retryableErrorCodes = commaSeparatedValuesToList(getProperty(QUERY_RETRY_ERROR_CODES));
        return queryException.isRetryable() || retryableErrorCodes.contains(errorCode);
    }

    /**
     * Determines if a RequestPermissionException should be retried.
     *
     * @param requestPermissionException the exception to evaluate
     * @return true if retry is advised
     */
    protected boolean shouldRetry(RequestPermissionException requestPermissionException) {
        return requestPermissionException.isRetryAdvised();
    }

    /**
     * Handles a RequestException based on retry eligibility and failOnError setting.
     * <ul>
     *   <li>Retries if shouldRetry returns true and retry limit not exceeded</li>
     *   <li>Throws exception if ServerConnectionException or failOnError is true</li>
     *   <li>Logs warning and writes to error file if failOnError is false</li>
     * </ul>
     *
     * @param requestException the exception to handle
     * @return array of input URIs
     * @throws CorbException if exception cannot be handled
     */
    protected String[] handleRequestException(RequestException requestException) throws CorbException {
        if (shouldRetry(requestException)) {
            return handleRetry(requestException);
        } else if (requestException instanceof ServerConnectionException || failOnError) {
            Thread.currentThread().setName(FAILED_URI_TOKEN + Thread.currentThread().getName());
            throw wrapProcessException(requestException, inputUris);
        } else {
            String exceptionName = requestException.getClass().getSimpleName();
            String code = requestException instanceof QueryException ? ((QueryException)requestException).getCode() : null;
            String message = requestException.getMessage();
            String errorMessage = message;
            if (message != null && code != null) {
                errorMessage = code + ':' + message;
            } else if (code != null) {
                errorMessage = code;
            }
            LOG.log(WARNING, failOnErrorIsFalseMessage(exceptionName, inputUris), requestException);
            writeToErrorFile(inputUris, errorMessage);
            Thread.currentThread().setName(FAILED_URI_TOKEN + Thread.currentThread().getName());
            return inputUris;
        }
    }

    /**
     * Handles retry logic for a RequestException.
     * Waits for the configured retry interval before retrying.
     * If retry limit is exceeded, delegates to handleProcessException.
     *
     * @param requestException the exception that triggered the retry
     * @return array of input URIs if retry succeeds
     * @throws CorbException if retry limit exceeded or retry fails
     */
    protected String[] handleRetry(RequestException requestException) throws CorbException {
        String exceptionName = requestException.getClass().getSimpleName();
        int retryInterval = this.getQueryRetryInterval();
        if (retryCount < getQueryRetryLimit()) {
            retryCount++;

            String errorCode = requestException instanceof QueryException ? ((QueryException)requestException).getCode() + ':' : "";
            LOG.log(WARNING,
                "Encountered {0} from MarkLogic Server. Retrying attempt {1} after {2} seconds..: {3}{4}{5}{6}",
                new Object[]{exceptionName, retryCount, retryInterval, errorCode, requestException.getMessage(), AT_URI, urisAsString(inputUris)});
            try {
                Thread.sleep(retryInterval * 1000L);
            } catch (InterruptedException ex) {
                LOG.log(WARNING, "Interrupted!", ex);
                Thread.currentThread().interrupt();
            }
            return invokeModule();
        } else {
            return handleProcessException(requestException);
        }
    }

    /**
     * Handles a general processing exception based on failOnError setting.
     * Throws CorbException if failOnError is true, otherwise logs and writes to error file.
     *
     * @param ex the exception to handle
     * @return array of input URIs
     * @throws CorbException if failOnError is true
     */
    protected String[] handleProcessException(Exception ex) throws CorbException {
        String exceptionName = ex.getClass().getSimpleName();
        writeCompletedUrisToRestartState(inputUris);
        if (failOnError) {
            Thread.currentThread().setName(FAILED_URI_TOKEN + Thread.currentThread().getName());
            throw wrapProcessException(ex, inputUris);
        } else {
            LOG.log(WARNING, failOnErrorIsFalseMessage(exceptionName, inputUris), ex);
            writeToErrorFile(inputUris, ex.getMessage());
            return inputUris;
        }
    }

    /**
     * Wraps an exception with URI information into a CorbException.
     *
     * @param ex the original exception
     * @param inputUris the URIs being processed when the exception occurred
     * @return CorbException with URI information appended to message
     */
    protected CorbException wrapProcessException(Exception ex, String... inputUris) {
        return new CorbException(ex.getMessage() + AT_URI + urisAsString(inputUris), ex);
    }

    /**
     * Formats a warning message when failOnError is false.
     *
     * @param name the exception class simple name
     * @param inputUris the URIs being processed
     * @return formatted message string
     */
    private String failOnErrorIsFalseMessage(final String name, final String... inputUris) {
        return "failOnError is false. Encountered " + name + AT_URI + urisAsString(inputUris);
    }

    /**
     * Converts URIs array to a comma-separated string.
     * Returns empty string if URIs is null or URIS_REDACTED option is true.
     *
     * @param uris the URIs to join
     * @return comma-separated URI string, or empty string if redacted or null
     */
    protected String urisAsString(String... uris) {
        return (uris == null || StringUtils.stringToBoolean(getProperty(Options.URIS_REDACTED)) ) ? "" : StringUtils.join(uris, ",");
    }

    /**
     * Processes the result sequence from module execution.
     * Subclasses must implement this to handle specific result processing logic.
     *
     * @param seq the ResultSequence returned from MarkLogic
     * @return processing result as string
     * @throws CorbException if result processing fails
     */
    protected abstract String processResult(ResultSequence seq) throws CorbException;

    /**
     * Releases task resources.
     * Called in finally block after task execution completes.
     * Sets all fields to null to aid garbage collection.
     */
    protected void cleanup() {
        // release resources
        csp = null;
        moduleType = null;
        moduleUri = null;
        properties = null;
        inputUris = null;
        adhocQuery = null;
        language = null;
        timeZone = null;
        exportDir = null;
    }

    /**
     * Retrieves a property value by key.
     * Delegates to Options.findOption for precedence handling.
     *
     * @param key the property key
     * @return the property value, or null if not found
     */
    public String getProperty(String key) {
        return Options.findOption(properties, key);
    }

    /**
     * Converts an XdmItem to a byte array.
     * Returns binary data for XdmBinary items, UTF-8 encoded string for others.
     *
     * @param item the XdmItem to convert
     * @return byte array representation, or empty array if item is null
     */
    protected static byte[] getValueAsBytes(XdmItem item) {
        if (item instanceof XdmBinary) {
            return ((XdmBinary) item).asBinaryData();
        } else if (item != null) {
            return item.asString().getBytes(StandardCharsets.UTF_8);
        } else {
            return EMPTY_BYTE_ARRAY.clone();
        }
    }

    /**
     * Gets the query retry limit from configuration.
     *
     * @return retry limit, or DEFAULT_QUERY_RETRY_LIMIT if not configured or negative
     */
    private int getQueryRetryLimit() {
        int queryRetryLimit = getIntProperty(QUERY_RETRY_LIMIT);
        return queryRetryLimit < 0 ? DEFAULT_QUERY_RETRY_LIMIT : queryRetryLimit;
    }

    /**
     * Gets the query retry interval in seconds from configuration.
     *
     * @return retry interval in seconds, or DEFAULT_QUERY_RETRY_INTERVAL if not configured or negative
     */
    private int getQueryRetryInterval() {
        int queryRetryInterval = getIntProperty(QUERY_RETRY_INTERVAL);
        return queryRetryInterval < 0 ? DEFAULT_QUERY_RETRY_INTERVAL : queryRetryInterval;
    }

    /**
     * Gets the batch URI delimiter from configuration.
     *
     * @return delimiter string, or DEFAULT_BATCH_URI_DELIM if not configured
     */
    private String getBatchUriDelimiter() {
        String delim = getProperty(BATCH_URI_DELIM);
        if (isEmpty(delim)) {
            delim = DEFAULT_BATCH_URI_DELIM;
        }
        return delim;
    }

    /**
     * Retrieves an integer property value.
     * Returns -1 if the property is not found or cannot be parsed as an integer.
     *
     * @param key the property key name
     * @return the integer value, or -1 if not found or invalid
     */
    protected int getIntProperty(String key) {
        int intVal = -1;
        String value = getProperty(key);
        if (isNotEmpty(value)) {
            try {
                intVal = Integer.parseInt(value);
            } catch (Exception exc) {
                LOG.log(WARNING, MessageFormat.format("Unable to parse `{0}` value `{1}` as an int", key, value), exc);
            }
        }
        return intVal;
    }

    /**
     * Writes failed URIs and error messages to the error file.
     * Thread-safe operation using synchronization.
     * Format: URI[delimiter]message[newline]
     *
     * @param uris the URIs that failed processing
     * @param message the error message to write
     */
    private void writeToErrorFile(String[] uris, String message) {
        if (uris == null || uris.length == 0) {
            return;
        }

        String errorFileName = getProperty(ERROR_FILE_NAME);
        if (isEmpty(errorFileName)) {
            return;
        }

        String delim = getProperty(BATCH_URI_DELIM);
        if (isEmpty(delim)) {
            delim = DEFAULT_BATCH_URI_DELIM;
        }

        synchronized (ERROR_SYNC_OBJ) {
            try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, errorFileName), true))) {
                for (String uri : uris) {
                    writer.write(uri.getBytes(StandardCharsets.UTF_8));
                    if (isNotEmpty(message)) {
                        writer.write(delim.getBytes(StandardCharsets.UTF_8));
                        writer.write(message.getBytes(StandardCharsets.UTF_8));
                    }
                    writer.write(NEWLINE);
                }
                writer.flush();
            } catch (Exception exc) {
                LOG.log(SEVERE, "Problem writing uris to " + ERROR_FILE_NAME, exc);
            }
        }
    }

    private void writeCompletedUrisToRestartState(String[] uris) throws CorbException {
        if (uris == null || uris.length == 0 || !StringUtils.stringToBoolean(getProperty(Options.RESTARTABLE))) {
            return;
        }
        String restartStateDir = getResolvedRestartStateDir();
        if (isEmpty(restartStateDir)) {
            throw new CorbException("Unable to resolve restart state directory");
        }
        synchronized (RESTART_STATE_SYNC_OBJ) {
            try {
                RestartableJobState.appendCompletedUris(new File(restartStateDir), uris);
            } catch (IOException ex) {
                throw new CorbException("Unable to persist restart state", ex);
            }
        }
    }

    /**
     * Resolves the directory to use for restart state persistence.
     * Checks RESTART_STATE_DIR property first, then TEMP_DIR, and finally falls back to system temp directory.
     *
     * @return the resolved directory path for restart state
     */
    protected String getResolvedRestartStateDir() {
        String restartStateDir = getProperty(Options.RESTART_STATE_DIR);
        if (isNotEmpty(restartStateDir)) {
            return restartStateDir;
        }
        String tempDir = getProperty(Options.TEMP_DIR);
        if (isNotEmpty(tempDir)) {
            return tempDir;
        }
        return System.getProperty("java.io.tmpdir");
    }

}
