package com.marklogic.developer.corb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.text.MessageFormat;

import java.util.Collections;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Level.INFO;

public class JobServer {

    private static final Logger LOG = Logger.getLogger(JobServer.class.getName());
    private static final String SEPARATOR = "*****************************************************************************************";

    private static final String CLASSPATH_FOLDER_WITH_RESOURCES = "web";
    public static final String HTTP_RESOURCE_PATH = "/web";
    public static final String JOB_SERVICE_PATH = "/corb";

    private void JobServer() {}

    public static HttpServer create(Integer port) throws IOException {
        return JobServer.create(Collections.singleton(port), null);
    }

    public static HttpServer create(Set<Integer> portCandidates, Manager manager) throws IOException {
        int requestQueue = 100; //how many HTTP requests to queue before rejecting requests
        return create(portCandidates, requestQueue, manager);
    }

    public static HttpServer create(Set<Integer> portCandidates, int requestQueue, Manager manager) throws IOException {

        HttpServer jobServer = HttpServer.create();

        InetSocketAddress socket;
        for (int portCandidate : portCandidates) {
            socket = new InetSocketAddress(portCandidate);
            try {
                jobServer.bind(socket, requestQueue);
                break; //port is available
            } catch (IOException ex) {
                LOG.log(Level.FINE, () -> MessageFormat.format("Port {0,number} is not available, trying the next candidate", portCandidate));
            }
        }
        if (manager != null) {
            jobServer.createContext(JOB_SERVICE_PATH, new JobServicesHandler(manager));
        }

        jobServer.createContext(HTTP_RESOURCE_PATH, new HttpHandler() {

            @Override
            public void handle(HttpExchange httpExchange) throws IOException {

                String path = httpExchange.getRequestURI().getPath();
                String relativePath = path.substring(HTTP_RESOURCE_PATH.length());
                // filename isn't necessary, let's use clean URLs
                if (relativePath.equals("/")){
                    relativePath += "index.html";
                }
                String resourcePath = CLASSPATH_FOLDER_WITH_RESOURCES + relativePath;

                try (InputStream is = Manager.class.getResourceAsStream("/" + resourcePath);
                     OutputStream output = httpExchange.getResponseBody()) {

                    if (is == null) {
                        String response = "Error 404 File not found.";
                        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, response.length());
                        output.write(response.getBytes());
                    } else {
                        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                        httpExchange.getResponseHeaders().set("Content-Type", getContentType(relativePath));

                        final byte[] buffer = new byte[0x10000];
                        int count = 0;
                        while ((count = is.read(buffer)) >= 0) {
                            output.write(buffer, 0, count);
                        }
                    }
                    output.flush();
                    output.close();
                } catch (IOException ex) {
                    LOG.log(Level.WARNING, "Unable to open file", ex);
                }
            }
        });
        jobServer.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        jobServer.start();
        logUsage(jobServer);
        return jobServer;
    }

    public static String getContentType(String path) {
        String contentType = "text/html; charset=utf-8";
        if (path != null) {
            if (path.endsWith(".js")) {
                contentType = "application/javascript";
            } else if (path.endsWith(".css")) {
                contentType = "text/css";
            }
        }
        return contentType;
    }

    public static void logUsage(HttpServer server) {
        LOG.log(INFO, SEPARATOR);
        LOG.log(INFO, () -> MessageFormat.format("Job Server has started and can be access using http://localhost:{0,number,#}{1}/", server.getAddress().getPort(), HTTP_RESOURCE_PATH));
        LOG.log(INFO, () -> MessageFormat.format("Visit http://localhost:{0,number,#}{1} to fetch the metrics data", server.getAddress().getPort(), JOB_SERVICE_PATH));
        LOG.log(INFO, SEPARATOR);
    }

}
