package com.marklogic.developer.corb;

import com.marklogic.client.*;
import com.marklogic.client.eval.*;
import com.marklogic.developer.corb.util.*;

import java.util.logging.*;

import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static java.util.logging.Level.SEVERE;

public class DataMovementJobStats extends JobStats {
    private static final Logger LOG = Logger.getLogger(DataMovementJobStats.class.getName());
    private DatabaseClient databaseClient;

    public DataMovementJobStats(DataMovementManager manager) {
        super(manager);
        databaseClient = manager.getDatabaseClient();
    }

    protected void logToServer(String message, String metrics) {
        if (databaseClient != null) {
            try {
                logToServer(databaseClient, message, metrics);
            } catch (Exception  ex) {
                LOG.log(SEVERE, "logToServer request failed", ex);
            }
        }
    }

    protected void logToServer(DatabaseClient databaseClient, String message, String metrics) {
        TransformOptions options = getOptions();
        String logLevel = options.getLogMetricsToServerLog();
        if (options.isMetricsLoggingEnabled(logLevel)) {
            ServerEvaluationCall serverEvaluationCall = databaseClient.newServerEval();
                String xquery = XQUERY_VERSION_ML
                    + (message != null
                    ? String.format(XDMP_LOG_FORMAT, message, logLevel.toLowerCase()) + ','
                    : "")
                    + String.format(XDMP_LOG_FORMAT, metrics, logLevel.toLowerCase());

            serverEvaluationCall.xquery(xquery);
            serverEvaluationCall.eval();
        }
    }

    protected void executeModule(String metrics) {
        TransformOptions options = getOptions();
        String metricsDatabase = options.getMetricsDatabase();
        if (metricsDatabase != null && databaseClient != null) {
            ServerEvaluationCall serverEvaluationCall = databaseClient.newServerEval();
            DataMovementUtils.setAdhocQuery(serverEvaluationCall, metrics);

            String uriRoot = getOptions().getMetricsRoot();
            String collections = options.getMetricsCollections();
            String processModule = options.getMetricsModule();

            serverEvaluationCall.addVariable(METRICS_DB_NAME_PARAM, metricsDatabase);
            serverEvaluationCall.addVariable(METRICS_URI_ROOT_PARAM, uriRoot != null ? uriRoot : NOT_APPLICABLE);
            serverEvaluationCall.addVariable(METRICS_COLLECTIONS_PARAM, collections != null ? collections : NOT_APPLICABLE);
            if (isJavaScriptModule(processModule)) {
                serverEvaluationCall.addVariable(METRICS_DOCUMENT_STR_PARAM, metrics == null ? toJSON() : metrics);
            } else {
                serverEvaluationCall.addVariable(METRICS_DOCUMENT_STR_PARAM, metrics == null ? toXmlString() : metrics);
            }

            String results = serverEvaluationCall.evalAs(String.class);
            if (results != null) {
                setMetricsDocUri(results);
            }
        }
    }
}
