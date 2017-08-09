package com.marklogic.developer.corb;

import java.io.IOException;
import java.util.Map;

import com.marklogic.developer.corb.HTTPServer.Request;
import com.marklogic.developer.corb.HTTPServer.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobServicesHandler implements HTTPServer.ContextHandler {

    private static final Logger LOG = Logger.getLogger(JobServicesHandler.class.getName());
    private Manager manager;

    JobServicesHandler(Manager manager) {
        this.manager = manager;
    }

    @Override
    public int serve(Request req, Response resp) throws IOException {
        String method = req.getMethod();
        if (method.equalsIgnoreCase("post")) {
            return doPost(req, resp);
        } else {
            return doGet(req, resp);
        }
    }

    private void writeMetricsOut(Response resp, Map<String, String> params) throws IOException {
        boolean concise = params.containsKey("concise") || params.containsKey("CONCISE");
        alowXSS(resp);
        if (params.containsKey("xml") || params.containsKey("XML")) {
            resp.getHeaders().add("Content-Type", "application/xml");
            resp.send(200, manager.jobStats.toXMLString(concise));
        } else {
            resp.getHeaders().add("Content-Type", "application/json");
            resp.send(200, manager.jobStats.toJSONString(concise));
        }

    }

    private int doGet(Request req, Response resp) throws IOException {
        writeMetricsOut(resp, req.getParams());
        return 0;
    }

    private int doPost(Request req, Response resp) throws IOException {
        Map<String, String> params = req.getParams();
        pauseResumeJob(params);
        updateThreads(params);

        return doGet(req, resp);
    }

    private void pauseResumeJob(Map<String, String> params) {
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

    private void updateThreads(Map<String, String> params) {
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

    private void alowXSS(Response resp) {
        resp.getHeaders().add("Access-Control-Allow-Origin", "*");
        resp.getHeaders().add("Access-Control-Allow-Methods", "GET,POST");
        resp.getHeaders().add("Access-Control-Max-Age", "3600");
        resp.getHeaders().add("Access-Control-Allow-Headers", "Content-Type");
    }
}
