package com.marklogic.developer.corb;

import com.marklogic.client.eval.*;
import com.marklogic.developer.corb.util.*;
import com.marklogic.xcc.*;

import java.util.*;

public class DataMovemenResultItemIteratorFacade implements Iterator<ResultItem> {
    private final EvalResultIterator evalResult;

    public DataMovemenResultItemIteratorFacade(EvalResultIterator resultItemIterator) {
        evalResult = resultItemIterator;
    }

    @Override
    public ResultItem next() {
        return DataMovementUtils.asResultItem(evalResult.next());
    }

    @Override
    public boolean hasNext() {
        if (evalResult == null) {
            return false;
        }
        return evalResult.hasNext();
    }
}
