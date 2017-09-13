package com.marklogic.developer.corb;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobServicesHandlerTest {

    @Test
    public void handle() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobServer.METRICS_PATH));
        when(exchange.getRequestMethod()).thenReturn("GET");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        OutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);
        Manager manager = mock(Manager.class);
        when(manager.getOptions()).thenReturn(new TransformOptions());
        manager.jobStats = new JobStats(manager);
        JobServicesHandler handler = new JobServicesHandler(manager);
        handler.handle(exchange);
        assertTrue(out.toString().startsWith("{"));
    }

    @Test
    public void handlePost() throws Exception {
        Headers headers = new Headers();
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(JobServer.METRICS_PATH));
        when(exchange.getRequestMethod()).thenReturn("POST");
        when(exchange.getResponseHeaders()).thenReturn(headers);
        OutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseBody()).thenReturn(out);

        Manager manager = mock(Manager.class);
        when(manager.getOptions()).thenReturn(new TransformOptions());
        manager.jobStats = new JobStats(manager);

        JobServicesHandler handler = new JobServicesHandler(manager);
        handler.handle(exchange);
        assertTrue(out.toString().startsWith("{"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void handleUnsupportedMethod() throws Exception {
        HttpExchange exchange = mock(HttpExchange.class);
        when(exchange.getRequestURI()).thenReturn(URI.create(""));
        when(exchange.getRequestMethod()).thenReturn("DELETE");
        Mockito.doThrow(new UnsupportedOperationException()).when(exchange).sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0l);
        Manager manager = mock(Manager.class);
        JobServicesHandler handler = new JobServicesHandler(manager);
        handler.handle(exchange);
    }

    @Test
    public void doGet() throws Exception {
    }

    @Test
    public void doPost() throws Exception {
    }

    @Test
    public void writeMetricsOut() throws Exception {
        Manager manager = new Manager();
        manager.jobStats = new JobStats(manager);
        JobServicesHandler handler = new JobServicesHandler(manager);
        Map<String, String> params = new HashMap<>();
        HttpExchange exchange = mock(HttpExchange.class);
        Headers headers = new Headers();
        OutputStream out = new ByteArrayOutputStream();
        when(exchange.getResponseHeaders()).thenReturn(headers);
        when(exchange.getResponseBody()).thenReturn(out);

        handler.writeMetricsOut(exchange, params, manager);
        assertEquals(1, headers.size());
        assertEquals(1, headers.get(JobServer.HEADER_CONTENT_TYPE).size());
        assertTrue(headers.get(JobServer.HEADER_CONTENT_TYPE).contains("application/json"));

        params.put(JobServicesHandler.PARAM_FORMAT, "xml");
        handler.writeMetricsOut(exchange, params, manager);
        assertEquals( 1, headers.size());
        assertEquals(2, headers.get(JobServer.HEADER_CONTENT_TYPE).size());
        assertTrue(headers.get(JobServer.HEADER_CONTENT_TYPE).contains("application/xml"));
    }

    @Test
    public void querystringToMap() throws Exception {
        Map<String, String> params = JobServicesHandler.querystringToMap(Options.THREAD_COUNT + "=1&thread-count=2&" + Options.COMMAND + "=pause&blank=&empty");
        assertEquals(5, params.size());
        assertEquals(Integer.toString(1), params.get(Options.THREAD_COUNT));
        assertEquals(Integer.toString(2), params.get("thread-count"));
        assertTrue(params.get("blank").isEmpty());
        assertTrue(params.get("empty").isEmpty());
    }

    @Test
    public void pauseResumeJobPause() throws Exception {
        Manager manager = new Manager();
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isPaused()).thenReturn(true);
        manager.pool = pool;
        Mockito.doThrow(new AssertionError()).when(pool).resume();

        JobServicesHandler handler = new JobServicesHandler(manager);
        Map<String, String> params = new HashMap<>();
        params.put(Options.COMMAND, "pause");
        handler.pauseResumeJob(params);
    }

    @Test
    public void pauseResumeJobResume() throws Exception {
        Manager manager = new Manager();
        PausableThreadPoolExecutor pool = mock(PausableThreadPoolExecutor.class);
        when(pool.isPaused()).thenReturn(false);
        Mockito.doThrow(new AssertionError()).when(pool).pause();
        manager.pool = pool;

        JobServicesHandler handler = new JobServicesHandler(manager);
        Map<String, String> params = new HashMap<>();
        params.put(Options.COMMAND, "resume");
        handler.pauseResumeJob(params);
    }

    @Test
    public void updateThreads() throws Exception {
        Map<String, String> parameters = new HashMap<>(2);
        parameters.put(Options.THREAD_COUNT, Integer.toString(80));
        Manager manager = new Manager();
        manager.options = new TransformOptions();
        manager.jobStats = new JobStats(manager);
        JobServicesHandler handler = new JobServicesHandler(manager);
        handler.updateThreads(parameters);
        assertEquals(80, manager.options.getThreadCount());
    }

    @Test
    public void getParameter() {
        Map<String, String> parameters = new HashMap<>(2);
        parameters.put(Options.THREAD_COUNT, Integer.toString(8));
        parameters.put(Options.COMMAND, "pause");

        assertEquals("pause", JobServer.getParameter(parameters, Options.COMMAND));
    }

    @Test
    public void getParameterDoesNotExist() {
        Map<String, String> parameters = new HashMap<>(2);
        Manager manager = new Manager();
        JobServicesHandler handler = new JobServicesHandler(manager);

        assertNull(JobServer.getParameter(parameters, "doesNotExist"));
    }

    @Test
    public void getParameterLowerCaseFirst() {
        Map<String, String> parameters = new HashMap<>(2);
        parameters.put(Options.THREAD_COUNT, Integer.toString(8));
        Manager manager = new Manager();
        JobServicesHandler handler = new JobServicesHandler(manager);
        assertEquals(Integer.toString(8), JobServer.getParameter(parameters, Options.THREAD_COUNT));

        parameters.put(Options.THREAD_COUNT.toLowerCase(Locale.ENGLISH), Integer.toString(4));
        assertEquals(Integer.toString(4), JobServer.getParameter(parameters, Options.THREAD_COUNT));
    }
}
