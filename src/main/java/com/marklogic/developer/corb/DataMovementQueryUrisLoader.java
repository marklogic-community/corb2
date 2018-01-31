package com.marklogic.developer.corb;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.query.QueryManager;
import com.marklogic.developer.corb.util.DataMovementUtils;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.types.*;

import java.security.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.URIS_MODULE;
import static com.marklogic.developer.corb.Options.XQUERY_MODULE;
import static com.marklogic.developer.corb.util.StringUtils.*;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

public class DataMovementQueryUrisLoader extends QueryUrisLoader {

    private static final Logger LOG = Logger.getLogger(DataMovementQueryUrisLoader.class.getName());
    private DatabaseClient databaseClient;
    private EvalResultIterator resultIterator;
    private Queue<String> queue;

    DataMovementQueryUrisLoader(DatabaseClient client) {
        this.databaseClient = client;
    }

    public void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @Override
    public void open() throws CorbException {
        parseUriReplacePatterns();

        try {
            RequestOptions opts = new RequestOptions();
            LOG.log(INFO, () -> MessageFormat.format("buffer size = {0}, caching = {1}",
                opts.getResultBufferSize(), opts.getCacheResult()));

            ServerEvaluationCall serverEvaluationCall = databaseClient.newServerEval();

            String urisModule = options.getUrisModule();
            if (isInlineOrAdhoc(urisModule)) {
                DataMovementUtils.setAdhocQuery(serverEvaluationCall, urisModule);
            } else {
                String root = options.getModuleRoot();
                String modulePath = buildModulePath(root, urisModule);
                LOG.log(INFO, () -> MessageFormat.format("Invoking {0} {1}", URIS_MODULE, modulePath));
                serverEvaluationCall.modulePath(modulePath);
            }

            // NOTE: collection will be treated as a CWSV
            serverEvaluationCall.addVariable("URIS", collection);
            // TODO support DIRECTORY as type
            serverEvaluationCall.addVariable("TYPE", TransformOptions.COLLECTION_TYPE);
            serverEvaluationCall.addVariable("PATTERN", "[,\\s]+");

            setCustomInputs(serverEvaluationCall);

            resultIterator = serverEvaluationCall.eval();

            preProcess(resultIterator);

            queue = createAndPopulateQueue(resultIterator);

        } catch (Exception exc) {
            throw new CorbException("While invoking " + URIS_MODULE, exc);
        }
    }

    /**
     * Collect all {@value Options#URIS_MODULE} properties from the properties and
     * System.properties (in that order, so System.properties will take
     * precedence over properties) and set as NewStringVariable for each
     * property the Request object.
     *
     * @param serverEvaluationCall
     */
    protected void setCustomInputs(ServerEvaluationCall serverEvaluationCall) {
        List<String> propertyNames = new ArrayList<>();
        // gather all of the property names
        if (properties != null) {
            propertyNames.addAll(properties.stringPropertyNames());
        }
        propertyNames.addAll(System.getProperties().stringPropertyNames());
        // custom inputs
        for (String propName : propertyNames) {
            if (propName.startsWith(URIS_MODULE + '.')) {
                String varName = propName.substring((URIS_MODULE + '.').length());
                String value = getProperty(propName);
                if (value != null) {
                    serverEvaluationCall.addVariable(varName, value);
                }
            }
        }
    }

    protected void preProcess(EvalResultIterator resultSequence) throws CorbException {
        EvalResult nextResultItem = collectCustomInputs(resultSequence);
        try {
            setTotalCount(Integer.parseInt(nextResultItem.getString()));  //TODO .getNumber().intValue()
        } catch (NumberFormatException exc) {
            throw new CorbException(URIS_MODULE + " " + options.getUrisModule() + " does not return total URI count");
        }
    }

    /**
     * Collect any custom input options or batchRef value in the ResultSequence
     * that precede the count of URIs.
     *
     * @param resultSequence
     * @return the next ResultItem to retrieve from the ResultSequence
     */
    protected EvalResult collectCustomInputs(EvalResultIterator resultSequence) {
        EvalResult nextResultItem = resultSequence.next();
        int maxOpts = this.getMaxOptionsFromModule();
        for (int i = 0; i < maxOpts
            && nextResultItem != null
            && getBatchRef() == null
            && !(nextResultItem.getString().matches("\\d+")); i++) {
            String value = nextResultItem.getString();
            if (MODULE_CUSTOM_INPUT.matcher(value).matches()) {
                int idx = value.indexOf('=');
                properties.put(value.substring(0, idx).replace(XQUERY_MODULE + '.', PROCESS_MODULE + '.'), value.substring(idx + 1));
            } else {
                setBatchRef(value);
            }
            nextResultItem = resultSequence.next();
        }
        return nextResultItem;
    }

    /**
     * Instantiate a new queue and populate with items from the ResultSequence.
     *
     * @param resultSequence
     * @return
     */
    protected Queue<String> createAndPopulateQueue(EvalResultIterator resultSequence) {
        Queue<String> uriQueue = createQueue();
        return populateQueue(uriQueue, resultSequence);
    }

    protected String resultItemAsString(Object results) {
        if (results instanceof EvalResult) {
            return ((EvalResult) results).getString();
        } else {
            throw new InvalidParameterException("Results must be of type " + EvalResult.class.getName());
        }
    }

}
