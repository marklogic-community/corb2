package com.marklogic.developer.corb;

import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An Iterator to walk through all results returned from calls to
 * {@link com.marklogic.xcc.Session#submitRequest(Request)}}.
 */
public class ResultItemIterator implements Iterable<ResultItem>, Iterator<ResultItem>, Closeable {

    private final ResultSequence resultSequence;

    public ResultItemIterator(ResultSequence resultSequence) {
        this.resultSequence = resultSequence;
    }

    @Override
    public ResultItem next() {
        return resultSequence.next();
    }

    @Override
    public boolean hasNext() {
        if (resultSequence == null) {
            return false;
        }
        return resultSequence.hasNext();
    }

    public final Iterator<ResultItem> iterator() {
        return this;
    }

    @Override
    public void close() {
        if ( resultSequence != null ) {
            resultSequence.close();
        }
    }
}
