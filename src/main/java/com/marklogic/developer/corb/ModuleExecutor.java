/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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

import static com.marklogic.developer.corb.AbstractTask.TRUE;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_DIR;
import static com.marklogic.developer.corb.Options.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.Options.MODULES_DATABASE;
import static com.marklogic.developer.corb.Options.MODULE_ROOT;
import static com.marklogic.developer.corb.Options.OPTIONS_FILE;
import static com.marklogic.developer.corb.Options.PROCESS_MODULE;
import static com.marklogic.developer.corb.Options.XCC_CONNECTION_URI;
import static com.marklogic.developer.corb.Options.XQUERY_MODULE;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.developer.corb.util.StringUtils;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isJavaScriptModule;
import static com.marklogic.developer.corb.util.StringUtils.trim;

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import java.util.logging.Logger;

/**
 * This class can be used to execute either an XQuery or JavaScript module in
 * MarkLogic.
 *
 * @author matthew.heckel MarkLogic Corportation
 *
 */
public class ModuleExecutor extends AbstractManager {

    protected static final String NAME = ModuleExecutor.class.getSimpleName();
    protected static final String PROPERTY_LINE_SEPARATOR = "line.separator";
    private static final byte[] NEWLINE
            = System.getProperty(PROPERTY_LINE_SEPARATOR) != null ? System.getProperty(PROPERTY_LINE_SEPARATOR).getBytes() : "\n".getBytes();
    protected static final Logger LOG = Logger.getLogger(ModuleExecutor.class.getName());
    private static final String TAB = "\t";

    /**
     * Execute an XQuery or JavaScript module in MarkLogic
     *
     * @param args
     */
    public static void main(String... args) {
        ModuleExecutor moduleExecutor = new ModuleExecutor();
        try {
            moduleExecutor.init(args);
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error initializing ModuleExecutor", exc);
            moduleExecutor.usage();
            LOG.log(INFO, () -> "init error - exiting with code " + EXIT_CODE_INIT_ERROR);
            System.exit(EXIT_CODE_INIT_ERROR);
        }

        try {
            moduleExecutor.run();
            LOG.log(INFO, () -> "success - exiting with code " + EXIT_CODE_SUCCESS);
            System.exit(EXIT_CODE_SUCCESS);
        } catch (Exception exc) {
            LOG.log(SEVERE, "Error while running CORB", exc);
            LOG.log(INFO, () -> "processing error - exiting with code " + EXIT_CODE_PROCESSING_ERROR);
            System.exit(EXIT_CODE_PROCESSING_ERROR);
        }
    }

    /**
     *
     * @param args
     * @throws com.marklogic.developer.corb.CorbException
     */
    @Override
    protected void initOptions(String... args) throws CorbException {
        super.initOptions(args);
        String processModule = getOption(args, 1, PROCESS_MODULE);
        String moduleRoot = getOption(args, 2, MODULE_ROOT);
        String modulesDatabase = getOption(args, 3, MODULES_DATABASE);
        String exportFileDir = getOption(args, 4, EXPORT_FILE_DIR);
        String exportFileName = getOption(args, 5, EXPORT_FILE_NAME);

        //Check legacy properties keys, for backwards compatibility
        if (processModule == null) {
            processModule = getOption(XQUERY_MODULE);
        }
        if (processModule != null) {
            options.setProcessModule(processModule);
        }
        if (null == options.getProcessModule()) {
            throw new CorbException(PROCESS_MODULE + " must be specified");
        }

        if (modulesDatabase != null) {
            options.setModulesDatabase(modulesDatabase);
        }
        if (moduleRoot != null) {
            options.setModuleRoot(moduleRoot);
        }

        if (!this.properties.containsKey(EXPORT_FILE_DIR) && exportFileDir != null) {
            this.properties.put(EXPORT_FILE_DIR, exportFileDir);
        }
        if (!this.properties.containsKey(EXPORT_FILE_NAME) && exportFileName != null) {
            this.properties.put(EXPORT_FILE_NAME, exportFileName);
        }

        if (exportFileDir != null) {
            File dirFile = new File(exportFileDir);
            if (dirFile.exists() && dirFile.canWrite()) {
                options.setExportFileDir(exportFileDir);
            } else {
                throw new IllegalArgumentException("Cannot write to export folder " + exportFileDir);
            }
        }

        FileUtils.deleteFileQuietly(exportFileDir, exportFileName);
    }

    @Override
    protected void usage() {
        super.usage();
        List<String> args = new ArrayList<>(5);
        String xccConnectionUri = "xcc://user:password@host:port/[ database ]";
        String optionsFile = "myjob.properties";
        PrintStream err = System.err; // NOPMD

        err.println("usage 1:"); // NOPMD
        args.add(NAME);
        args.add(xccConnectionUri);
        args.add("process-module [module-root [modules-database [ export-file-name ] ] ]");
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD

        err.println("\nusage 2:"); // NOPMD
        args.clear();
        args.add(buildSystemPropertyArg(XCC_CONNECTION_URI, xccConnectionUri));
        args.add(buildSystemPropertyArg(PROCESS_MODULE, "module-name.xqy"));
        args.add(buildSystemPropertyArg("...", null));
        args.add(NAME);
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD

        err.println("\nusage 3:"); // NOPMD
        args.clear();
        args.add(buildSystemPropertyArg(OPTIONS_FILE, optionsFile));
        args.add(NAME);
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD

        err.println("\nusage 4:"); // NOPMD
        args.clear();
        args.add(buildSystemPropertyArg(OPTIONS_FILE, optionsFile));
        args.add(NAME);
        args.add(xccConnectionUri);
        err.println(TAB + StringUtils.join(args, SPACE)); // NOPMD
    }

    @Override
    protected void logOptions() {
        LOG.log(INFO, () -> MessageFormat.format("Configured modules db: {0}", options.getModulesDatabase()));
        LOG.log(INFO, () -> MessageFormat.format("Configured modules root: {0}", options.getModuleRoot()));
        LOG.log(INFO, () -> MessageFormat.format("Configured process module: {0}", options.getProcessModule()));
    }

    public void run() throws Exception {
        LOG.log(INFO, () -> MessageFormat.format("{0} starting: {1}", NAME, VERSION_MSG));
        long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
        LOG.log(INFO, () -> MessageFormat.format("maximum heap size = {0} MiB", maxMemory));

        Request request;
        ResultSequence resultSequence = null;

        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setCacheResult(false);

        List<String> propertyNames = new ArrayList<>(properties.stringPropertyNames());
        propertyNames.addAll(System.getProperties().stringPropertyNames());
        String processModule = options.getProcessModule();

        ContentSource contentSource = csp.get();
        try (Session session = contentSource.newSession()) {
            request = getRequestForModule(processModule, session);
            if (isJavaScriptModule(processModule)) {
		        requestOptions.setQueryLanguage("javascript");
		    }
            // custom inputs
            for (String propName : propertyNames) {
                if (propName.startsWith(PROCESS_MODULE + '.')) {
                    String varName = propName.substring((PROCESS_MODULE + '.').length());
                    String value = getProperty(propName);
                    if (value != null) {
                        request.setNewStringVariable(varName, value);
                    }
                }
            }

            request.setOptions(requestOptions);

            resultSequence = session.submitRequest(request);
            processResult(resultSequence);

            LOG.info("Done");

        } catch (Exception exc) {
            LOG.log(SEVERE, exc.getMessage(), exc);
            throw exc;
        } finally {
            if (null != resultSequence && !resultSequence.isClosed()) {
                resultSequence.close();
            }
        }
    }

    public String getProperty(String key) {
        String val = System.getProperty(key);
        if (isBlank(val) && properties != null) {
            val = properties.getProperty(key);
        }
        return trim(val);
    }

    protected String processResult(ResultSequence seq) throws CorbException {
        try {
            writeToFile(seq);
            return TRUE;
        } catch (IOException exc) {
            throw new CorbException(exc.getMessage(), exc);
        }
    }

    protected void writeToFile(ResultSequence seq) throws IOException {
        if (seq == null || !seq.hasNext()) {
            return;
        }
        String fileDir = getProperty(EXPORT_FILE_DIR);
        String fileName = getProperty(EXPORT_FILE_NAME);
        if (StringUtils.isEmpty(fileName)) {
            return;
        }
        LOG.info("Writing output to file");

        File file = new File(fileDir, fileName);
        if (file.getParentFile() != null) {
            file.getAbsoluteFile().getParentFile().mkdirs();
        }
        try (BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file))) {
            while (seq.hasNext()) {
                writer.write(AbstractTask.getValueAsBytes(seq.next().getItem()));
                writer.write(NEWLINE);
            }
            writer.flush();
        }
    }

}
