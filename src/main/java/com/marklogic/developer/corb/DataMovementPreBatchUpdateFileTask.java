package com.marklogic.developer.corb;

public class DataMovementPreBatchUpdateFileTask extends DataMovementTask {

    //private PreBatchUpdateFileTask task;

    public DataMovementPreBatchUpdateFileTask(Task task) {
        super(task);
        if (!(task instanceof PreBatchUpdateFileTask)) {
           throw new IllegalArgumentException("Only PreBatchUpdateFileTask may be used to instantiate DataMovementPreBatchUpdateFileTask");
        }
    }

    @Override
    public String[] call() throws Exception {
        PreBatchUpdateFileTask preBatchTask = (PreBatchUpdateFileTask) getTask();
        try {
            preBatchTask.deleteFileIfExists();
            preBatchTask.writeTopContent();
            invokeModule();
            preBatchTask.addLineCountToProps();
            return new String[0];
        } finally {
            preBatchTask.cleanup();
        }
    }
}
