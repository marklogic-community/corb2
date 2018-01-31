package com.marklogic.developer.corb;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.*;
import com.marklogic.developer.corb.util.*;

import static com.marklogic.developer.corb.Manager.URIS_BATCH_REF;
import static com.marklogic.developer.corb.Options.LOADER_VARIABLE;
import static com.marklogic.developer.corb.Options.URIS_MODULE;
import static com.marklogic.developer.corb.util.StringUtils.buildModulePath;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static java.util.logging.Level.INFO;
import java.util.logging.Logger;

public class DataMovementTransform extends Transform {

    private DatabaseClient databaseClient;
    private static final java.util.logging.Logger LOG = Logger.getLogger(DataMovementTransform.class.getName());

    @Override
    protected String processResult(java.util.Iterator iterator) throws CorbException {
        return TRUE;
    }

    @Override
    protected String[] invokeModule() throws CorbException {
        if (moduleUri == null && adhocQuery == null) {
            return new String[0];
        }

        ServerEvaluationCall serverEvaluationCall = databaseClient.newServerEval();

        Thread.yield();// try to avoid thread starvation
        EvalResultIterator evalResultIterator = null;
        try {
            //determine whether this is an eval or execution of installed module
            if (moduleUri == null) {
                if ("JavaScript".equalsIgnoreCase(language)) {
                    serverEvaluationCall.javascript(adhocQuery);
                } else {
                    serverEvaluationCall.xquery(adhocQuery);
                }
            } else {
                serverEvaluationCall.modulePath(moduleUri);
            }
            //no option for setting timeZone, as we can with XCC
            if (timeZone != null) {
                LOG.log(INFO, String.format("Unable to set timeZone {0} for Java client API", timeZone));
            }

            if (inputUris != null && inputUris.length > 0) {
                String delim = getBatchUriDelimiter();
                String uriValue = StringUtils.join(inputUris, delim);
                String variableName = REQUEST_VARIABLE_DOC.equalsIgnoreCase(properties.getProperty(LOADER_VARIABLE)) ?
                    REQUEST_VARIABLE_DOC : REQUEST_VARIABLE_URI;
                serverEvaluationCall.addVariable(variableName, uriValue);
            }

            if (properties != null && properties.containsKey(URIS_BATCH_REF)) {
                serverEvaluationCall.addVariable(URIS_BATCH_REF, properties.getProperty(URIS_BATCH_REF));
            }

            //set custom inputs
            for (String customInputPropertyName : getCustomInputPropertyNames()) {
                String varName = customInputPropertyName.substring(moduleType.length() + 1);
                String value = getProperty(customInputPropertyName);
                if (value != null) {
                    serverEvaluationCall.addVariable(varName, value);
                }
            }
            //This is how the long running uris can be populated
            Thread.currentThread().setName(urisAsString(inputUris));

            Thread.yield();// try to avoid thread starvation
            evalResultIterator = serverEvaluationCall.eval();
            retryCount = 0;

            Thread.yield();// try to avoid thread starvation
            processResult(evalResultIterator);
            evalResultIterator.close();
            Thread.yield();// try to avoid thread starvation

            return inputUris;
        } catch (Exception exc) { //TODO: retryable HTTP exceptions?
            throw  wrapProcessException(exc, inputUris);
        } finally {
            if (null != evalResultIterator) {
                evalResultIterator.close();
            }
            Thread.yield();// try to avoid thread starvation
        }
    }

    public void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }
}
