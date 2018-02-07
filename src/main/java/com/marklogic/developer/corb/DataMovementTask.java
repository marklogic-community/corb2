package com.marklogic.developer.corb;

import com.marklogic.client.eval.*;
import com.marklogic.xcc.*;

import java.util.*;

public class DataMovementTask extends DataMovementTransform {

    private Task task;

    public DataMovementTask(Task task) {
        this.task = task;
    }

    @Override
    protected String processResult(Iterator iterator) throws CorbException {
        String result = TRUE;
        if (task != null) {
            Iterator<ResultItem> results = new DataMovemenResultItemIteratorFacade((EvalResultIterator)iterator); //TODO: need to test if instanceof
            if (task instanceof ExportBatchToFileTask) {
                result = ((ExportBatchToFileTask) task).processResult(results);
            } else if (task instanceof ExportToFileTask) {
                result = ((ExportToFileTask) task).processResult(results);
            }
        }
        return result;
    }

    protected Task getTask() {
        return task;
    }
}
