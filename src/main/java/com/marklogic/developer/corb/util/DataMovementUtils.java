package com.marklogic.developer.corb.util;

import com.marklogic.client.eval.*;
import com.marklogic.developer.corb.AbstractManager;
import com.marklogic.developer.corb.DataMovementManager;
import com.marklogic.developer.corb.TransformOptions;
import com.marklogic.xcc.*;
import com.marklogic.xcc.impl.*;
import com.marklogic.xcc.types.*;

import java.text.MessageFormat;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.URIS_MODULE;
import static com.marklogic.developer.corb.util.StringUtils.*;
import static java.util.logging.Level.INFO;

public class DataMovementUtils {

    private static Logger LOG = Logger.getLogger(DataMovementUtils.class.getName());
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Evaluate the module string to determine whether it is an INLINE adhoc query, or a reference to a module to be
     * read from the filesystem, and then determine the language and set the evaluation call.
     * @param serverEvaluationCall
     * @param module
     */
    public static void setAdhocQuery(ServerEvaluationCall serverEvaluationCall, String module) {
        String adhocQuery;
        if (isInlineModule(module)) {
            adhocQuery = getInlineModuleCode(module);
            if (isEmpty(adhocQuery)) {
                throw new IllegalStateException("Unable to read inline module");
            }
            LOG.log(INFO, "Invoking inline {0}", URIS_MODULE);
        } else {
            int pipeIndex = module.indexOf('|');
            if (pipeIndex > -1) {
                String queryPath = module.substring(0, pipeIndex);
                adhocQuery = AbstractManager.getAdhocQuery(queryPath);
                if (isEmpty(adhocQuery)) {
                    throw new IllegalStateException("Unable to read adhoc query " + queryPath + " from classpath or filesystem");
                }
                LOG.log(INFO, () -> MessageFormat.format("Invoking adhoc {0} {1}", URIS_MODULE, queryPath));
            } else {
                adhocQuery = module;
            }
        }
        if (isJavaScriptModule(module)) {
            serverEvaluationCall.javascript(adhocQuery);
        } else {
            serverEvaluationCall.xquery(adhocQuery);
        }
    }

    public static ResultItem asResultItem(EvalResult evalResult){
        byte[] bytes = asBytes(evalResult);
        XdmBinary xdmBinary = ValueFactory.newBinaryNode(bytes);
        return new ResultItemImpl(xdmBinary, 1, null, null);
    }

    public static byte[] asBytes(EvalResult evalResult) {
        if (evalResult.getType().equals(EvalResult.Type.BINARY)) {
            return evalResult.getAs(com.marklogic.client.io.BytesHandle.class).get();
        } else if (evalResult != null) {
            return evalResult.getString().getBytes();
        } else {
            return EMPTY_BYTE_ARRAY.clone();
        }
    }
}
