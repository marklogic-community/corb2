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

import static com.marklogic.developer.corb.Manager.DEFAULT_BATCH_URI_DELIM;
import static com.marklogic.developer.corb.Manager.URIS_BATCH_REF;
import static com.marklogic.developer.corb.Options.BATCH_URI_DELIM;
import static com.marklogic.developer.corb.Options.ERROR_FILE_NAME;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_LIMIT;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_ERROR_CODES;
import static com.marklogic.developer.corb.Options.QUERY_RETRY_ERROR_MESSAGE;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_INTERVAL;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_RETRY_LIMIT;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.commaSeparatedValuesToList;
import static com.marklogic.developer.corb.util.StringUtils.isEmpty;
import static com.marklogic.developer.corb.util.StringUtils.isNotEmpty;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.QueryException;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import com.marklogic.xcc.exceptions.RetryableQueryException;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import java.util.logging.Logger;

/**
 *
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 *
 */
public abstract class AbstractTask implements Task {

    private static final Object ERROR_SYNC_OBJ = new Object();

    protected static final String TRUE = "true";
    protected static final String FALSE = "false";
    private static final String URI = "URI";
    protected static final byte[] NEWLINE
            = System.getProperty("line.separator") != null ? System.getProperty("line.separator").getBytes() : "\n".getBytes();
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    protected ContentSource cs;
    protected String moduleType;
    protected String moduleUri;
    protected Properties properties = new Properties();
    protected String[] inputUris;

    protected String adhocQuery;
    protected String language;
    protected TimeZone timeZone;
    protected String exportDir;

    private static final Object SYNC_OBJ = new Object();
    protected static final Map<String, Set<String>> MODULE_PROPS = new HashMap<>();

    protected static final int DEFAULT_CONNECTION_RETRY_INTERVAL = 60;
    protected static final int DEFAULT_CONNECTION_RETRY_LIMIT = 3;
    protected static final int DEFAULT_QUERY_RETRY_INTERVAL = 20;
    protected static final int DEFAULT_QUERY_RETRY_LIMIT = 2;

    protected int retryCount = 0;
    protected boolean failOnError = true;

    private static final Logger LOG = Logger.getLogger(AbstractTask.class.getName());
    private static final String AT_URI = " at URI: ";

    @Override
    public void setContentSource(ContentSource cs) {
        this.cs = cs;
    }

    @Override
    public void setModuleType(String moduleType) {
        this.moduleType = moduleType;
    }

    @Override
    public void setModuleURI(String moduleUri) {
        this.moduleUri = moduleUri;
    }

    @Override
    public void setAdhocQuery(String adhocQuery) {
        this.adhocQuery = adhocQuery;
    }

    @Override
    public void setQueryLanguage(String language) {
        this.language = language;
    }

    @Override
    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void setInputURI(String... inputUri) {
        this.inputUris = inputUri != null ? inputUri.clone() : new String[]{};
    }

    @Override
    public void setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
    }

    @Override
    public void setExportDir(String exportFileDir) {
        this.exportDir = exportFileDir;
    }

    public String getExportDir() {
        return this.exportDir;
    }

    public Session newSession() {
        return cs.newSession();
    }

    @Override
    public String[] call() throws Exception {
        try {
            return invokeModule();
        } finally {
            cleanup();
        }
    }

    protected String[] invokeModule() throws CorbException {
        if (moduleUri == null && adhocQuery == null) {
            return new String[0];
        }

        Session session = null;
        ResultSequence seq = null;
        Thread.yield();// try to avoid thread starvation
        try {
            session = newSession();
            Request request;

            Set<String> modulePropNames = MODULE_PROPS.get(moduleType);
            if (modulePropNames == null) {
                synchronized (SYNC_OBJ) {
                    modulePropNames = MODULE_PROPS.get(moduleType);
                    if (modulePropNames == null) {
                        Set<String> propSet = new HashSet<>();
                        if (properties != null) {
                            for (String propName : properties.stringPropertyNames()) {
                                if (propName.startsWith(moduleType + ".")) {
                                    propSet.add(propName);
                                }
                            }
                        }
                        for (String propName : System.getProperties().stringPropertyNames()) {
                            if (propName.startsWith(moduleType + ".")) {
                                propSet.add(propName);
                            }
                        }
                        modulePropNames = propSet;
                        MODULE_PROPS.put(moduleType, modulePropNames);
                    }
                }
            }

            if (moduleUri == null) {
                request = session.newAdhocQuery(adhocQuery);
            } else {
                request = session.newModuleInvoke(moduleUri);
            }
            RequestOptions requestOptions = request.getOptions();
            if (language != null) {
                requestOptions.setQueryLanguage(language);
            }
            if (timeZone != null) {
                requestOptions.setTimeZone(timeZone);
            }
            if (inputUris != null && inputUris.length > 0) {
                if (inputUris.length == 1) {
                    request.setNewStringVariable(URI, inputUris[0]);
                } else {
                    String delim = getProperty(BATCH_URI_DELIM);
                    if (isEmpty(delim)) {
                        delim = DEFAULT_BATCH_URI_DELIM;
                    }
                    request.setNewStringVariable(URI, StringUtils.join(inputUris, delim));
                }
            }

            if (properties != null && properties.containsKey(URIS_BATCH_REF)) {
                request.setNewStringVariable(URIS_BATCH_REF, properties.getProperty(URIS_BATCH_REF));
            }

            for (String propName : modulePropNames) {
                if (propName.startsWith(moduleType + ".")) {
                    String varName = propName.substring(moduleType.length() + 1);
                    String value = getProperty(propName);
                    if (value != null) {
                        request.setNewStringVariable(varName, value);
                    }
                }
            }

            Thread.yield();// try to avoid thread starvation
            seq = session.submitRequest(request);
            retryCount = 0;
            // no need to hold on to the session as results will be cached.
            session.close();
            Thread.yield();// try to avoid thread starvation

            processResult(seq);
            seq.close();
            Thread.yield();// try to avoid thread starvation

            return inputUris;
        } catch (RequestException exc) {
            return handleRequestException(exc);
        } catch (Exception exc) {
            throw new CorbException(exc.getMessage() + AT_URI + asString(inputUris), exc);
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
    }

    protected boolean shouldRetry(RequestException requestException) {
        return requestException instanceof ServerConnectionException
                || requestException instanceof RetryableQueryException
                || requestException instanceof RequestPermissionException && shouldRetry((RequestPermissionException) requestException)
                || requestException instanceof QueryException && shouldRetry((QueryException) requestException)
                || hasRetryableMessage(requestException);
    }

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

    protected boolean shouldRetry(QueryException queryException) {
        String errorCode = queryException.getCode();
        List<String> retryableErrorCodes = commaSeparatedValuesToList(getProperty(QUERY_RETRY_ERROR_CODES));
        return queryException.isRetryable() || retryableErrorCodes.contains(errorCode);
    }

    protected boolean shouldRetry(RequestPermissionException requestPermissionException) {
        return requestPermissionException.isRetryAdvised();
    }

    protected String[] handleRequestException(RequestException requestException) throws CorbException {
        String name = requestException.getClass().getSimpleName();

        if (shouldRetry(requestException)) {
            int retryLimit = requestException instanceof ServerConnectionException ? this.getConnectRetryLimit() : this.getQueryRetryLimit();
            int retryInterval = requestException instanceof ServerConnectionException ? this.getConnectRetryInterval() : this.getQueryRetryInterval();
            if (retryCount < retryLimit) {
                retryCount++;
                LOG.log(WARNING,
                        "Encountered " + name + " from Marklogic Server. Retrying attempt {0} after {1} seconds..: {2}{3}{4}",
                        new Object[]{retryCount, retryInterval, requestException.getMessage(), AT_URI, asString(inputUris)});
                try {
                    Thread.sleep(retryInterval * 1000L);
                } catch (Exception ex) {
                }
                return invokeModule();
            } else if (requestException instanceof ServerConnectionException || failOnError) {
                throw new CorbException(requestException.getMessage() + AT_URI + asString(inputUris), requestException);
            } else {
                LOG.log(WARNING, failOnErrorIsFalseMessage(name, inputUris), requestException);
                writeToErrorFile(inputUris, requestException.getMessage());
                return inputUris;
            }
        } else if (failOnError) {
            throw new CorbException(requestException.getMessage() + AT_URI + asString(inputUris), requestException);
        } else {
            LOG.log(WARNING, failOnErrorIsFalseMessage(name, inputUris), requestException);
            writeToErrorFile(inputUris, requestException.getMessage());
            return inputUris;
        }
    }

    private String failOnErrorIsFalseMessage(final String name, final String... inputUris) {
        return "failOnError is false. Encountered " + name + AT_URI + asString(inputUris);
    }

    protected String asString(String... uris) {
        return uris == null ? "" : StringUtils.join(uris, ",");
    }

    protected abstract String processResult(ResultSequence seq) throws CorbException;

    protected void cleanup() {
        // release resources
        cs = null;
        moduleType = null;
        moduleUri = null;
        properties = null;
        inputUris = null;
        adhocQuery = null;
        language = null;
        timeZone = null;
        exportDir = null;
    }

    public String getProperty(String key) {
        String val = System.getProperty(key);
        if (val == null && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }

    protected static byte[] getValueAsBytes(XdmItem item) {
        if (item instanceof XdmBinary) {
            return ((XdmBinary) item).asBinaryData();
        } else if (item != null) {
            return item.asString().getBytes();
        } else {
            return EMPTY_BYTE_ARRAY.clone();
        }
    }

    private int getConnectRetryLimit() {
        int connectRetryLimit = getIntProperty(XCC_CONNECTION_RETRY_LIMIT);
        return connectRetryLimit < 0 ? DEFAULT_CONNECTION_RETRY_LIMIT : connectRetryLimit;
    }

    private int getConnectRetryInterval() {
        int connectRetryInterval = getIntProperty(XCC_CONNECTION_RETRY_INTERVAL);
        return connectRetryInterval < 0 ? DEFAULT_CONNECTION_RETRY_INTERVAL : connectRetryInterval;
    }

    private int getQueryRetryLimit() {
        int queryRetryLimit = getIntProperty(QUERY_RETRY_LIMIT);
        return queryRetryLimit < 0 ? DEFAULT_QUERY_RETRY_LIMIT : queryRetryLimit;
    }

    private int getQueryRetryInterval() {
        int queryRetryInterval = getIntProperty(QUERY_RETRY_INTERVAL);
        return queryRetryInterval < 0 ? DEFAULT_QUERY_RETRY_INTERVAL : queryRetryInterval;
    }

    /**
     * Retrieves an int value.
     *
     * @param key The key name.
     * @return The requested value ({@code -1} if not found or could not
     * parse value as int).
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
            try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(new File(exportDir, errorFileName), true))){
                for (String uri : uris) {
                    writer.write(uri.getBytes());
                    if (isNotEmpty(message)) {
                        writer.write(delim.getBytes());
                        writer.write(message.getBytes());
                    }
                    writer.write(NEWLINE);
                }
                writer.flush();
            } catch (Exception exc) {
                LOG.log(SEVERE, "Problem writing uris to " + ERROR_FILE_NAME, exc);
            }
        }
    }

}
