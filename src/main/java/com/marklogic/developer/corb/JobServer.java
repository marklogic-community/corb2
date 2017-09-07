package com.marklogic.developer.corb;

import com.sun.net.httpserver.HttpContext;
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
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Level.INFO;

public class JobServer {

    private static final Logger LOG = Logger.getLogger(JobServer.class.getName());
    private static final int DEFAULT_REQUEST_QUEUE_SIZE = 100;
    private static final String SEPARATOR = "*****************************************************************************************";
    private static final String CLASSPATH_FOLDER_WITH_RESOURCES = "web";
    public static final String HTTP_RESOURCE_PATH = "/";
    public static final String SERVICE_PATH = "/stats";
    public static final String MONITOR_PATH = "/jobs";

    private HttpServer server;
    private boolean hasManager;

    public static JobServer create(Integer port) throws IOException {
        return JobServer.create(Collections.singleton(port), null);
    }

    public static JobServer create(Set<Integer> portCandidates, Manager manager) throws IOException {
        return create(portCandidates, DEFAULT_REQUEST_QUEUE_SIZE, manager);
    }

    public static JobServer create(Set<Integer> portCandidates, int requestQueueSize, Manager manager) throws IOException {
        return new JobServer(portCandidates, requestQueueSize, manager);
    }

    protected JobServer(Set<Integer> portCandidates, int requestQueueSize, Manager manager) throws IOException {

        server = HttpServer.create();
        bindFirstAvailablePort(portCandidates, requestQueueSize);
        setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        setManager(manager);

        createContext(HTTP_RESOURCE_PATH, (HttpExchange httpExchange) -> {
            String path = httpExchange.getRequestURI().getPath();
            // filename and extensions aren't necessary in the request, but now we need to find the file
            if (path.isEmpty() || HTTP_RESOURCE_PATH.equals(path) ){
                path += "index.html";
            } else if (MONITOR_PATH.equals(path)) {
                path += ".html";
            }
            String resourcePath = CLASSPATH_FOLDER_WITH_RESOURCES + path;

            try (InputStream is = Manager.class.getResourceAsStream("/" + resourcePath);
                 OutputStream output = httpExchange.getResponseBody()) {

                if (is == null) {
                    String response = "Error 404 File not found.";
                    httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, response.length());
                    output.write(response.getBytes());
                } else {
                    httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                    httpExchange.getResponseHeaders().set("Content-Type", getContentType(path));

                    final byte[] buffer = new byte[0x10000];
                    int count = 0;
                    while ((count = is.read(buffer)) >= 0) {
                        output.write(buffer, 0, count);
                    }
                }
                output.flush();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "Unable to open file", ex);
            }
        });
    }

    public void bindFirstAvailablePort(Set<Integer> portCandidates, int requestQueueSize) {
        InetSocketAddress socket;
        for (int portCandidate : portCandidates) {
            socket = new InetSocketAddress(portCandidate);
            try {
                bind(socket, requestQueueSize);
                break; //port is available
            } catch (IOException ex) {
                LOG.log(Level.FINE, () -> MessageFormat.format("Port {0,number} is not available, trying the next candidate", portCandidate));
            }
        }
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

    public void setManager(Manager manager) {
        if (manager != null) {
            createContext(SERVICE_PATH, new JobServicesHandler(manager));
            hasManager = true;
        }
    }

    public void logUsage() {
        LOG.log(INFO, SEPARATOR);
        LOG.log(INFO,  "Job Server has started");
        if (hasManager) {
            LOG.log(INFO, () -> MessageFormat.format("Monitor and manage the job at http://localhost:{0,number,#}{1}", server.getAddress().getPort(), HTTP_RESOURCE_PATH));
            LOG.log(INFO, () -> MessageFormat.format("Retrieve job metrics data at http://localhost:{0,number,#}{1}", server.getAddress().getPort(), SERVICE_PATH));
        }
        LOG.log(INFO, () -> MessageFormat.format("Monitor the status of multiple jobs at http://localhost:{0,number,#}{1}", server.getAddress().getPort(), MONITOR_PATH));
        LOG.log(INFO, SEPARATOR);
    }

    public HttpContext createContext(String context, HttpHandler handler) {
        return server.createContext(context, handler);
    }

    public HttpContext createContext(String context) {
        return server.createContext(context);
    }

    public InetSocketAddress getAddress() {
        return server.getAddress();
    }

    public void start() {
        server.start();
        logUsage();
    }

    public void stop(int delayMillis) {
        server.stop(delayMillis);
    }

    public void bind(InetSocketAddress inetSocketAddress, int i) throws IOException {
        server.bind(inetSocketAddress, i);
    }

    public void setExecutor(Executor executor) {
        server.setExecutor(executor);
    }

    public Executor getExecutor() {
        return server.getExecutor();
    }

    public void removeContext(String s) throws IllegalArgumentException {
        server.removeContext(s);
    }

    public void removeContext(HttpContext httpContext) {
        server.removeContext(httpContext);
    }

}
