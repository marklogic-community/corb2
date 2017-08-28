package com.marklogic.developer.corb;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


import java.util.logging.Level;
import java.util.logging.Logger;

public class JobServicesHandler implements HttpHandler {

    private static final Logger LOG = Logger.getLogger(JobServicesHandler.class.getName());
    public static final String PARAM_CONCISE = "concise";
    protected static final String HEADER_CONTENT_TYPE = "Content-Type";
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
            httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0l);
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
        boolean concise = hasParameter(params, PARAM_CONCISE);
        String response;
        if (hasParameter(params,"xml")) { //TODO: why not ?format=xml ?
            httpExchange.getResponseHeaders().add(HEADER_CONTENT_TYPE, "application/xml");
            response = manager.jobStats.toXMLString(concise);
        } else {
            httpExchange.getResponseHeaders().add(HEADER_CONTENT_TYPE, "application/json");
            response =  manager.jobStats.toJSONString(concise);
        }
        alowXSS(httpExchange);
        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length());
        try (OutputStream out = httpExchange.getResponseBody()) {
            out.write(response.getBytes(Charset.forName("UTF-8")));
            out.flush();
            out.close();
        }
    }

    public static Map<String, String> querystringToMap(String query){
        Map<String, String> result = new HashMap<>();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
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
        String value = value = getParameter(params, Options.COMMAND);
        if ("PAUSE".equalsIgnoreCase(value)) {
            manager.pause();
        } else if ("RESUME".equalsIgnoreCase(value)) {
            manager.resume();
        }
    }

    protected void updateThreads(Map<String, String> params) {
        String value = getParameter(params, Options.THREAD_COUNT);
        if (value != null) {
            try {
                int threadCount = Integer.parseInt(value);
                if (threadCount > 0) {
                    manager.setThreadCount(threadCount);
                }
            } catch (NumberFormatException e) {
                LOG.log(Level.WARNING, MessageFormat.format("{} value not numeric", Options.THREAD_COUNT), e);
            }
        }
        manager.jobStats.setPaused(String.valueOf(manager.isPaused()));
    }

    protected String getParameter(Map<String, String> map, String key) {
        String value = null;
        String caseSensitiveKey = key.toLowerCase(Locale.ENGLISH);
        if (map.containsKey(caseSensitiveKey)) {
            value = map.get(caseSensitiveKey);
        } else {
            caseSensitiveKey = key.toUpperCase(Locale.ENGLISH);
            if (map.containsKey(caseSensitiveKey)) {
                value = map.get(caseSensitiveKey);
            }
        }
        return value;
    }

    protected boolean hasParameter(Map<String, String>params, String key){
        return getParameter(params, key) != null;
    }

    protected void alowXSS(HttpExchange httpExchange) {
        Headers headers = httpExchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET,POST");
        headers.add("Access-Control-Max-Age", "3600");
        headers.add("Access-Control-Allow-Headers", HEADER_CONTENT_TYPE);
    }
}
