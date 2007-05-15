/*
 * Copyright (c)2005-2007 Mark Logic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.marklogic.developer.SimpleLogger;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.exceptions.XccException;
import com.marklogic.xcc.types.XSInteger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Manager implements Runnable {

    /**
     * 
     */
    private static final String NAME = Manager.class.getName();

    public static String VERSION = "2007-05-07.1";

    private URI connectionUri;

    private String collection;

    private String modulePath;

    private TransformOptions options = new TransformOptions();

    private ThreadPoolExecutor pool = null;

    private ContentSource contentSource;

    private Task[] transforms;

    private Monitor monitor;

    private SimpleLogger logger;

    private String moduleUri;

    /**
     * @param connectionUri
     * @param collection
     * @param modulePath
     */
    public Manager(URI connectionUri, String collection, String modulePath) {
        this.connectionUri = connectionUri;
        this.collection = collection;
        this.modulePath = modulePath;
    }

    /**
     * @param args
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws URISyntaxException {
        if (args.length < 3) {
            usage();
            return;
        }

        // gather inputs
        URI connectionUri = new URI(args[0]);
        String collection = args[1];
        String moduleUri = args[2];

        Manager tm = new Manager(connectionUri, collection, moduleUri);
        if (args.length > 3) {
            // options
            TransformOptions options = tm.getOptions();
            options.setThreadCount(Integer.parseInt(args[3]));
        }
        tm.run();
    }

    /**
     * @return
     */
    private TransformOptions getOptions() {
        return options;
    }

    /**
     * 
     */
    private static void usage() {
        PrintStream err = System.err;
        err.println("\nusage:");
        err.println("\t" + NAME
                + " xcc://user:password@host:port/[ database ]"
                + " input-collection" + " module-name.xqy"
                + " [ thread-count ]");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        configureLogger();
        logger.info(NAME + " starting: version = " + VERSION);

        prepareContentSource();
        prepareModules();
        Thread monitorThread = preparePool();

        try {
            populateQueue();

            while (monitorThread.isAlive()) {
                try {
                    monitorThread.join();
                } catch (InterruptedException e) {
                    logger.logException(
                            "interrupted while waiting for monitor", e);
                }
            }
        } catch (XccException e) {
            logger.logException(connectionUri.toString(), e);
            // fatal
            throw new RuntimeException(e);
        } finally {
            stop();
        }
    }

    /**
     * @return
     */
    private Thread preparePool() {
        pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(options
                .getThreadCount());
        monitor = new Monitor(pool, this);
        Thread monitorThread = new Thread(monitor);
        monitorThread.start();
        return monitorThread;
    }

    /**
     * @throws IOException
     * @throws RequestException
     * 
     */
    private void prepareModules() {
        // bootstrap xqy modules from package's getResourceAsInputStream()
        String modulesDatabase = options.getModulesDatabase();
        logger
                .info("preparing modules using database "
                        + modulesDatabase);
        Session session = contentSource.newSession(modulesDatabase);
        InputStream is = null;
        Content c = null;
        ContentCreateOptions opts = ContentCreateOptions
                .newTextInstance();
        String[] resourceModules = new String[] { TransformOptions.URIS_MODULE };
        try {
            for (String uri : resourceModules) {
                logger.info("inserting module " + uri);
                // use a relative path
                is = this.getClass().getResourceAsStream(
                        new File(uri).getName());
                if (null == is) {
                    throw new NullPointerException(uri
                            + " could not be found in package resources");
                }
                // use an absolute path
                c = ContentFactory.newContent(
                        TransformOptions.MODULE_ROOT + uri, is, opts);
                session.insertContent(c);
            }

            // now the workload module: check files first, then resources
            File f = new File(modulePath);
            if (f.exists()) {
                moduleUri = TransformOptions.MODULE_ROOT + f.getName();
                c = ContentFactory.newContent(moduleUri, f, opts);
            } else {
                logger.warning("looking for " + modulePath
                        + " as resource");
                moduleUri = TransformOptions.MODULE_ROOT + modulePath;
                is = this.getClass().getResourceAsStream(modulePath);
                if (null == is) {
                    throw new NullPointerException(modulePath
                            + " could not be found on the filesystem,"
                            + " or in package resources");
                }
                c = ContentFactory.newContent(moduleUri, is, opts);
            }
            session.insertContent(c);
        } catch (IOException e) {
            logger.logException("fatal error", e);
            throw new RuntimeException(e);
        } catch (RequestException e) {
            logger.logException("fatal error", e);
            throw new RuntimeException(e);
        } finally {
            session.close();
        }
    }

    /**
     * 
     */
    private void prepareContentSource() {
        logger.info("using content source " + connectionUri);
        try {
            contentSource = ContentSourceFactory
                    .newContentSource(connectionUri);
        } catch (XccConfigException e1) {
            logger.logException(connectionUri.toString(), e1);
            throw new RuntimeException(e1);
        }
    }

    private void populateQueue() throws XccException {
        logger.info("populating queue");

        // configure the task factory
        TaskFactory.setContentSource(contentSource);
        // trim off the leading slash for the XDBC library path
        TaskFactory.setModuleUri(moduleUri
                .substring(TransformOptions.XDBC_ROOT.length()));

        // must run uncached, or we'll quickly run out of memory
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setCacheResult(false);

        Session session = null;
        int count = 0;
        int total = -1;

        try {
            session = contentSource.newSession();
            // trim off the leading slash for the XDBC library path
            String urisModule = TransformOptions.MODULE_ROOT
                    .substring(TransformOptions.XDBC_ROOT.length())
                    + TransformOptions.URIS_MODULE;
            logger.info("invoking module " + urisModule);
            Request req = session.newModuleInvoke(urisModule);
            // NOTE: collection will be treated as a CWSV
            req.setNewStringVariable("URIS", collection);
            // TODO support DIRECTORY, QUERY as types
            req.setNewStringVariable("TYPE", TransformOptions.COLLECTION_TYPE);
            req.setNewStringVariable("PATTERN", "[,\\s]+");
            req.setOptions(requestOptions);

            ResultSequence res = session.submitRequest(req);

            // like a pascal string, the first item will be the count
            total = ((XSInteger) res.next().getItem()).asPrimitiveInt();
            logger.info("expecting total " + total);
            transforms = new Task[total];

            // the monitor needs access to this structure too
            monitor.setTasks(transforms);

            // this may return millions of items:
            // try to be memory-efficient
            count = 0;
            Task transform;
            String uri;
            // check pool occasionally, for fast-fail
            while (res.hasNext() && null != pool) {
                uri = res.next().asString();
                transform = TaskFactory.newTask(uri);
                transforms[count] = transform;
                if (null == pool) {
                    break;
                }
                pool.submit(transform);
                count++;
                logger.finest("adding " + count + " of " + total + ": "
                        + uri);
            }
        } finally {
            if (null != session) {
                session.close();
            }
        }
        // if the pool went away, the monitor stopped it: bail out.
        if (null == pool) {
            return;
        }
        assert total == count;

        // there won't be any more tasks
        pool.shutdown();
        logger.info("queue is fully populated with " + total + " tasks");
    }

    private void configureLogger() {
        if (logger == null) {
            logger = SimpleLogger.getSimpleLogger();
        }
        Properties props = new Properties();
        props.setProperty("LOG_LEVEL", options.getLogLevel());
        props.setProperty("LOG_HANDLER", options.getLogHandler());
        logger.configureLogger(props);
        Monitor.setLogger(logger);
    }

    /**
     * @param e
     */
    public void stop() {
        if (pool != null) {
            List remaining = pool.shutdownNow();
            if (remaining.size() > 0) {
                logger.warning("thread pool was shut down with "
                        + remaining.size() + " pending tasks");
            }
            monitor.setPool(null);
            pool = null;
        }
    }

    /**
     * @param e
     * @param transform
     */
    public void stop(ExecutionException e, Transform transform) {
        // fatal error
        logger.logException("fatal error at result for input document "
                + transform.getUri(), e.getCause());
        logger.warning("exiting due to fatal error");
        stop();
    }
}
