package com.marklogic.developer.corb;

public class DataMovementPostBatchUpdateFileTask extends DataMovementTask {

    public DataMovementPostBatchUpdateFileTask(Task task) {
        super(task);
        if (!(task instanceof PostBatchUpdateFileTask)) {
            throw new IllegalArgumentException("Only PostBatchUpdateFileTask may be used to instantiate DataMovementPostBatchUpdateFileTask");
        }
    }

    @Override
    public String[] call() throws Exception {
        PostBatchUpdateFileTask postBatchTask = (PostBatchUpdateFileTask) getTask();
        try {
            postBatchTask.sortAndRemoveDuplicates();
            invokeModule();
            postBatchTask.writeBottomContent();
            postBatchTask.moveFile();
            postBatchTask.compressFile();
            return new String[0];
        } finally {
            postBatchTask.cleanup();
        }
    }
}
