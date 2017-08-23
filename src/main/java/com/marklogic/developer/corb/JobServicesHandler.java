package com.marklogic.developer.corb;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;


import java.util.logging.Level;
import java.util.logging.Logger;

public class JobServicesHandler implements HttpHandler {

    private static final Logger LOG = Logger.getLogger(JobServicesHandler.class.getName());
    private Manager manager;

    JobServicesHandler(Manager manager) {
        this.manager = manager;
    }

    public void handle(HttpExchange httpExchange) throws IOException {
        String querystring = httpExchange.getRequestURI().getQuery();
        Map<String,String> params = querystringToMap(querystring);
        String method = httpExchange.getRequestMethod();
        if ("GET".equals(method)) {
            doGet(httpExchange, params);
        } else if ("POST".equals(method)) {
            doPost(httpExchange, params);
        } else {
            LOG.log(Level.WARNING, "Unsupported method {0}", method);
            httpExchange.sendResponseHeaders(400, 0l);
        }
    }

    protected void doGet(HttpExchange httpExchange, Map<String, String> params) throws IOException {
        writeMetricsOut(httpExchange, params);
    }

    protected void doPost(HttpExchange httpExchange, Map<String, String> params) throws IOException {
        pauseResumeJob(params);
        updateThreads(params);
        doGet(httpExchange, params);
    }

    protected void writeMetricsOut(HttpExchange httpExchange, Map<String, String> params) throws IOException {
        boolean concise = params.containsKey("concise") || params.containsKey("CONCISE");
        String response;
        if (params.containsKey("xml") || params.containsKey("XML")) {
            httpExchange.getResponseHeaders().add("Content-Type", "application/xml");
            response = manager.jobStats.toXMLString(concise);
        } else {
            httpExchange.getResponseHeaders().add("Content-Type", "application/json");
            response =  manager.jobStats.toJSONString(concise);
        }
        alowXSS(httpExchange);
        httpExchange.sendResponseHeaders(200, response.length());
        httpExchange.getResponseBody().write(response.getBytes(Charset.forName("UTF-8")));
    }

    public static Map<String, String> querystringToMap(String query){
        Map<String, String> result = new HashMap<>();
        if (query != null) {
            for (String param : query.split("&")) {
                String pair[] = param.split("=");
                if (pair.length > 1) {
                    result.put(pair[0], pair[1]);
                } else {
                    result.put(pair[0], "");
                }
            }
        }
        return result;
    }

    protected void pauseResumeJob(Map<String, String> params) {
        if (params.containsKey("paused") || params.containsKey("PAUSED")) {
            String value = params.get("paused");
            value = value == null ? params.get("PAUSED") : value;
            if (value != null && value.equalsIgnoreCase("true")) {
                manager.pause();
            } else if (value != null && value.equalsIgnoreCase("false")) {
                manager.resume();
            }
        }
    }

    protected void updateThreads(Map<String, String> params) {
        if (params.containsKey("threads") || params.containsKey("THREADS")) {
            String value = params.get("threads");
            value = value == null ? params.get("THREADS") : value;
            if (value != null) {
                try {
                    int threadCount = Integer.parseInt(value);
                    if (threadCount > 0) {
                        manager.setThreadCount(threadCount);
                    }
                } catch (NumberFormatException e) {
                    LOG.log(Level.WARNING, "THREADS value not numeric", e);
                }
            }
        }
        manager.jobStats.setPaused(String.valueOf(manager.isPaused()));
    }

    protected void alowXSS(HttpExchange httpExchange) {
        Headers headers = httpExchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET,POST");
        headers.add("Access-Control-Max-Age", "3600");
        headers.add("Access-Control-Allow-Headers", "Content-Type");
    }
}
