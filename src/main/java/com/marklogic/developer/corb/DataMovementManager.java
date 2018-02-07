package com.marklogic.developer.corb;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.admin.*;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.*;
import com.marklogic.client.eval.*;
import com.marklogic.client.io.*;
import com.marklogic.client.query.*;
import com.marklogic.developer.corb.util.*;
import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.*;
import com.marklogic.xcc.types.*;

import javax.print.URIException;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.util.StringUtils.isInlineOrAdhoc;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

public class DataMovementManager extends Manager {

    private static Logger LOG = Logger.getLogger(DataMovementManager.class.getName());

    private DatabaseClient databaseClient;

    public static void main(String... args) {
        try (Manager manager = new DataMovementManager()) {
            try {
                manager.init(args);
            } catch (Exception exc) {
                LOG.log(SEVERE, "Error initializing CORB " + exc.getMessage(), exc);
                manager.usage();
                LOG.log(INFO, () -> "init error - exiting with code " + EXIT_CODE_INIT_ERROR);
                System.exit(EXIT_CODE_INIT_ERROR);
            }
            //now we can start corb.
            try {
                long count = manager.run();
                if (manager.execError) {
                    LOG.log(INFO, () -> "processing error - exiting with code " + EXIT_CODE_PROCESSING_ERROR);
                    System.exit(EXIT_CODE_PROCESSING_ERROR);
                } else if (manager.stopCommand) {
                    LOG.log(INFO, () -> "stop command - exiting with code " + EXIT_CODE_STOP_COMMAND);
                    System.exit(EXIT_CODE_STOP_COMMAND);
                } else if (count == 0) {
                    LOG.log(INFO, () -> "no uris found - exiting with code " + EXIT_CODE_NO_URIS);
                    System.exit(EXIT_CODE_NO_URIS);
                } else {
                    LOG.log(INFO, () -> "success - exiting with code " + EXIT_CODE_SUCCESS);
                    System.exit(EXIT_CODE_SUCCESS);
                }
            } catch (Exception exc) {
                LOG.log(SEVERE, "Error while running CORB", exc);
                LOG.log(INFO, () -> "unexpected error - exiting with code " + EXIT_CODE_PROCESSING_ERROR);
                System.exit(EXIT_CODE_PROCESSING_ERROR);
            }
        }
    }

    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    @Override
    public void init(String... args) throws CorbException {
        init(args, null);
    }

    public void initDatabaseClient(String... args) throws CorbException {
        List<String> connectionUris = parseUris(args.length > 0 ? args[0] : null);
        URL xccUrl = null;
        String[] userPass;
        String user = null;
        String password = null;
        String database = null;
        String xccConnectionUri = connectionUris.stream().findFirst().orElseThrow(NullPointerException::new);
        if (xccConnectionUri.startsWith("xcc")){
            xccConnectionUri = xccConnectionUri.replaceFirst("^xcc", "http");
        }
        try {
            xccUrl = new URL(xccConnectionUri);
            userPass = xccConnectionUri.substring(xccConnectionUri.indexOf("://") + 3, xccConnectionUri.indexOf("@")).split(":");
            user = userPass[0];
            password = userPass[1];
            if (isNotBlank(xccUrl.getPath())) {
                if (xccUrl.getPath().startsWith("/")) {
                    database = xccUrl.getPath().substring(1);
                }
            }
            /**
             * TODO: need new options and means of using different Context: (BasicAuthContext, DigestAuthContext, KerberosAuthContext, CertificateAuthContext)
             * XXX_AUTH_CONTEXT = Basic, Digest, Kerberos, Certificate
             new DatabaseClientFactory.BasicAuthContext(user, password);

             These need keystore info (identity and trus jks locations(default to use jdk) and passwords (encryption)
             new DatabaseClientFactory.KerberosAuthContext(sslContext, x509TrustManager)
             new DatabaseClientFactory.CertificateAuthContext(sslContext)
             */

            DatabaseClientFactory.SecurityContext securityContext = new DatabaseClientFactory.DigestAuthContext(user, password);

            if (isNotBlank(database)) {
                databaseClient = DatabaseClientFactory.newClient(xccUrl.getHost(), xccUrl.getPort(), database, securityContext);
            } else {
                databaseClient = DatabaseClientFactory.newClient(xccUrl.getHost(), xccUrl.getPort(), securityContext);
            }

        } catch (MalformedURLException ex) {
            throw new CorbException(MessageFormat.format("Unable to parse connection URI {0}", xccConnectionUri), ex);
        }
    }

    @Override
    public void init(String[] commandlineArgs, Properties props) throws CorbException {
        String[] args = commandlineArgs;
        if (args == null) {
            args = new String[0];
        }

        if (props == null || props.isEmpty()) {
            try {
                initPropertiesFromOptionsFile();
            } catch (IOException ex) {
                throw new CorbException("Failed to initialized properties from options file", ex);
            }
        } else {
            this.properties = props;
        }
        initDecrypter();
        initSSLConfig();
        initContentSourcePool(args.length > 0 ? args[0] : null);

        initOptions(args);
        logRuntimeArgs();
        initDatabaseClient(args);

        registerStatusInfo();

        prepareModules();

        String collectionName = getOption(args, 1, COLLECTION_NAME);
        collection = collectionName == null ? "" : collectionName;

        EXIT_CODE_NO_URIS = NumberUtils.toInt(getOption(Options.EXIT_CODE_NO_URIS));

        scheduledExecutor = Executors.newScheduledThreadPool(2);
    }

    @Override
    protected void initContentSourcePool(String uriArg) throws CorbException {
        System.out.println("ContentSourcePool is not needed for DataMovement");
        //this.csp = new DefaultContentSourcePool();
    }

    @Override
    protected void registerStatusInfo() throws CorbException {

        String module = XQUERY_VERSION_ML + DECLARE_NAMESPACE_MLSS_XDMP_STATUS_SERVER
            + "let $status := xdmp:server-status(xdmp:host(), xdmp:server())\n"
            + "let $modules := $status/mlss:modules\n"
            + "let $root := $status/mlss:root\n"
            + "return (data($modules), data($root))";

        ServerEvaluationCall serverEvaluationCall = databaseClient.newServerEval();
        DataMovementUtils.setAdhocQuery(serverEvaluationCall, module);

        //TODO: move into try with resources when implements closable https://github.com/marklogic/java-client-api/issues/874
        EvalResultIterator results = serverEvaluationCall.eval();
        int index = 0;

        while (null != results && results.hasNext()) {
            EvalResult evalResult = results.next();

            if (index == 0 && "0".equals(evalResult.getString())) {
                options.setModulesDatabase("");
            }
            if (index == 1) {
                options.setXDBC_ROOT(evalResult.getString());
            }
            index++;
        }
        results.close();

        logOptions();
        logProperties();
    }

    @Override
    protected JobStats newJobStats(){
        return new DataMovementJobStats(this);
    }

    @Override
    protected TaskFactory newTaskFactory() {
        return new DataMovementTaskFactory(this);
    }

    @Override
    protected UrisLoader newQueryUrisLoader() {
        DataMovementQueryUrisLoader queryUrisLoader = new DataMovementQueryUrisLoader(databaseClient);
        queryUrisLoader.setDatabaseClient(databaseClient);
        return queryUrisLoader;
    }

    @Override
    protected void prepareModules() throws CorbException {
        String[] resourceModules = new String[]{options.getInitModule(), options.getUrisModule(),
            options.getProcessModule(), options.getPreBatchModule(), options.getPostBatchModule()};
        String modulesDatabase = options.getModulesDatabase();
        LOG.log(INFO, () -> MessageFormat.format("checking modules, database: {0}", modulesDatabase));

        //ContentSource contentSource = csp.get();
        ExtensionLibrariesManager libManager = databaseClient.newServerConfigManager().newExtensionLibrariesManager();

        for (String resourceModule : resourceModules) {
            insertModule(libManager, resourceModule);
        }
    }

    protected void insertModule(ExtensionLibrariesManager libManager, String resourceModule) throws CorbException {
        if (resourceModule == null || isInlineOrAdhoc(resourceModule)) {
            return;
        }
        try {
            // Start by checking install flag.
            if (!options.isDoInstall()) {
                LOG.log(INFO, () -> MessageFormat.format("Skipping module installation: {0}", resourceModule));
            } // Next check: if XCC is configured for the filesystem, warn user
            else if (options.getModulesDatabase().isEmpty()) {
                LOG.warning("Modules configured for the filesystem: please install modules manually");
            } // Finally, if it's configured for a database, install.
            else {
                File file = new File(resourceModule);

                // If not installed, are the specified files on the filesystem?
                if (file.exists()) {
                    String moduleUri = options.getModuleRoot() + file.getName();
                    FileHandle fileHandle = new FileHandle(file).withFormat(Format.TEXT);
                    libManager.write(moduleUri, fileHandle);
                } // finally, check package
                else {
                    LOG.log(WARNING, () -> MessageFormat.format("looking for {0} as resource", resourceModule));
                    String moduleUri = options.getModuleRoot() + resourceModule;

                    try (InputStream is = this.getClass().getResourceAsStream('/' + resourceModule)) {
                        if (null == is) {
                            throw new NullPointerException(resourceModule + " could not be found on the filesystem," + " or in package resources");
                        }
                        InputStreamHandle inputStreamHandle = new InputStreamHandle(is);
                        libManager.write(moduleUri, inputStreamHandle);
                    }
                }
            }
        } catch (IOException e) {
            throw new CorbException(MessageFormat.format("error while reading module {0}", resourceModule), e);
        }
    }

    @Override
    protected void setJobServer(JobServer jobServer) {
        this.jobServer = jobServer;
        options.setJobServerPort(jobServer.getAddress().getPort());
        if (jobStats == null) {
            jobStats = new DataMovementJobStats(this);
        }
    }
}
