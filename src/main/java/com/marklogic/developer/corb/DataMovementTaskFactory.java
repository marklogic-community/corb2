package com.marklogic.developer.corb;

import com.marklogic.client.DatabaseClient;

import static com.marklogic.developer.corb.Options.*;

public class DataMovementTaskFactory extends TaskFactory {

    private DatabaseClient databaseClient;

    public DataMovementTaskFactory(DataMovementManager manager){
        super(manager);
        this.databaseClient = manager.getDatabaseClient();
    }

    /*
    public void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }
    */

    protected Transform newDataMovementTransform() {
        DataMovementTransform transform = new DataMovementTransform();
        setDatabaseClientFor(transform);
        return transform;
    }

    @Override
    public Transform newDefaultTransform() {
        return newDataMovementTransform();
    }

    @Override
    public Task newPreBatchTask() {
        TransformOptions options = manager.getOptions();
        if (null == options.getPreBatchTaskClass() && null == options.getPreBatchModule()) {
            return null;
        }
        if (null != options.getPreBatchModule() && databaseClient == null) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getPreBatchTaskClass() == null ? newDefaultTransform() : options.getPreBatchTaskClass().newInstance();
            setupTask(task, PRE_BATCH_MODULE, options.getPreBatchModule());
            setDatabaseClientFor(task);


            if (options.getProcessTaskClass() != null && task instanceof PreBatchUpdateFileTask) {
                task = new DataMovementPreBatchUpdateFileTask(task);
                setDatabaseClientFor(task);
                setupTask(task, PRE_BATCH_MODULE, options.getPreBatchModule());
            }
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    @Override
    public Task newPostBatchTask() {
        TransformOptions options = manager.getOptions();
        if (null == options.getPostBatchTaskClass() && null == options.getPostBatchModule()) {
            return null;
        }
        if (null != options.getPostBatchModule() && databaseClient == null) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getPostBatchTaskClass() == null ? newDefaultTransform() : options.getPostBatchTaskClass().newInstance();
            setupTask(task, POST_BATCH_MODULE, options.getPostBatchModule());
            setDatabaseClientFor(task);

            if (options.getPostBatchTaskClass() != null && task instanceof PostBatchUpdateFileTask) {
                task = new DataMovementPostBatchUpdateFileTask(task);
                setDatabaseClientFor(task);
                setupTask(task, PROCESS_MODULE, options.getPostBatchModule());
            }
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    @Override
    public Task newInitTask() {
        TransformOptions options = manager.getOptions();
        if (null == manager.getOptions().getInitTaskClass() && null == options.getInitModule()) {
            return null;
        }
        if (null != options.getInitModule()) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT);
        }
        try {
            Task task = options.getInitTaskClass() == null ? newDefaultTransform() : options.getInitTaskClass().newInstance();
            setupTask(task, INIT_MODULE, options.getInitModule());
            setDatabaseClientFor(task);
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    @Override
    public Task newProcessTask(String[] uris, boolean failOnError) {
        TransformOptions options = manager.getOptions();
        if (null == options.getProcessTaskClass() && null == options.getProcessModule()) {
            throw new NullPointerException("null process task and xquery module");
        }
        if (null != options.getProcessModule()
            && (null == uris || uris.length == 0 )) {
            throw new NullPointerException(EXCEPTION_MSG_NULL_CONTENT + " or input uri");
        }
        try {
            Task task = options.getProcessTaskClass() == null ? newDefaultTransform() : options.getProcessTaskClass().newInstance();
            setDatabaseClientFor(task);
            setupTask(task, PROCESS_MODULE, options.getProcessModule(), uris, failOnError);

            if (options.getProcessTaskClass() != null) {
                task = new DataMovementTask(task);
                setDatabaseClientFor(task);
                setupTask(task, PROCESS_MODULE, options.getProcessModule(), uris, failOnError);
            }
            return task;
        } catch (Exception exc) {
            throw new IllegalArgumentException(exc.getMessage(), exc);
        }
    }

    private void setDatabaseClientFor(Task task) {
        if (task instanceof DataMovementTransform) {
            ((DataMovementTransform)task).setDatabaseClient(databaseClient);
        }
    }

}
