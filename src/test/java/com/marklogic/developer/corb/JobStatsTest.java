/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 * *
 * * The use of the Apache License does not indicate that this project is
 * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.CompletionService;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.marklogic.developer.corb.util.XmlUtils;
import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.impl.SessionImpl;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.*;

class JobStatsTest {
    private static final String FOO = "foo";
    private static final String BAR = "bar";
    private static final String METRICS_DB = "metricsDB";

    @Test
    void testGetAverageTransactionTimeNoCompletedTasks() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        double average = jobStats.getAverageTransactionTime(5000L, 0L, 0L);
        assertFalse(Double.isInfinite(average));
        assertEquals(0, (int) average);
    }

    @Test
    void testGetAverageTransactionNoValues() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        double average = jobStats.getAverageTransactionTime(0L, 0L, 0L);
        assertFalse(Double.isInfinite(average));
        assertEquals(0, average, 0.00);
    }

    @Test
    void testGetAverageTransactionTimeNoFailed() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        assertFalse(Double.isInfinite(jobStats.getAverageTransactionTime(50L, 0L, 3000L)));
    }

    @Test
    void testGetAverageTransactionTimeNoSuccessful() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        double answer = jobStats.getAverageTransactionTime(50L, 300L, 0L);
        assertFalse(Double.isInfinite(answer));

        Double averageOfOne = jobStats.getAverageTransactionTime(3L, 2L, 1L);
        assertEquals(1d, averageOfOne, 0.00);

        Double averageMoreSuccess = jobStats.getAverageTransactionTime(3L, 0L, 3L);
        Double averageMoreFailed = jobStats.getAverageTransactionTime(3L, 3L, 0L);
        assertEquals(averageMoreSuccess, averageMoreFailed);
        assertEquals(1d, averageMoreFailed, 0.00);
        assertEquals(averageOfOne, averageMoreSuccess);
    }

    @Test
    void getHostName() {
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setJobName("JobStatsTest");
        Manager manager = new Manager();
        manager.options = transformOptions;
        JobStats jobStats = new JobStats(manager);
        assertNotNull(jobStats.getHost());
    }

    @Test
    void epochMillisAsFormattedDateString() {
        assertEquals(20, JobStats.epochMillisAsFormattedDateString(0).length());
    }

    @Test
    void testToString() {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        assertTrue(jobStats.toString().startsWith("{"));
        assertTrue(jobStats.toString().endsWith("}"));
    }

    @Test
	void testNullName() {
	    Manager manager = new Manager();
		JobStats jobStats = new JobStats(manager);

		assertXMLJSONNotNull(jobStats);
	}

	@Test
	void testFailedUris() {
        Manager manager = new Manager();
        TransformOptions options = new TransformOptions();
        options.setJobName("myJob");
        manager.options = options;
		JobStats jobStats = new JobStats(manager);

		assertXMLJSONNotNull(jobStats);
	}

	private void assertXMLJSONNotNull(JobStats jobStats) {
		String xml = jobStats.toXmlString();
        assertNotNull(xml);
        assertNotNull(jobStats.toJSON());
        assertNotNull(jobStats.toJSON(true));
	}

	@Test
	void testXMLRanks() throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
		Map<String, Long> nodeVal = new HashMap<>();
        nodeVal.put("URI6", 1L);
        nodeVal.put("URI3", 4L);
        nodeVal.put("URI1", 6L);
		nodeVal.put("URI2", 5L);
		nodeVal.put("URI4", 3L);
		nodeVal.put("URI5", 2L);

		Manager manager = new Manager();
		PausableThreadPoolExecutor threadPoolExecutor = mock(PausableThreadPoolExecutor.class);
		when(threadPoolExecutor.getTopUris()).thenReturn(nodeVal);
		Monitor monitor = new Monitor(threadPoolExecutor, mock(CompletionService.class), manager);
		monitor.setTaskCount(1);
        manager.monitor = monitor;
        JobStats jobStats = new JobStats(manager);

		assertXMLJSONNotNull(jobStats);
		DocumentBuilderFactory dbFactory = XmlUtils.newSecureDocumentBuilderFactoryInstance();
		dbFactory.setNamespaceAware(true);
		DocumentBuilder dBuilder;
		dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(new InputSource(new StringReader(jobStats.toXmlString())));
        XPathFactory factory = XPathFactory.newInstance();
        NamespaceContext nsContext = new NamespaceContext() {
            public String getNamespaceURI(String prefix) {
                return "c".equals(prefix) ? JobStats.CORB_NAMESPACE : null;
            }
            public Iterator getPrefixes(String val) {
                return null;
            }
            public String getPrefix(String uri) {
                return null;
            }
        };
        XPath xpath = factory.newXPath();
        xpath.setNamespaceContext(nsContext);

        String uri = (String) xpath.evaluate("/c:job/c:slowTransactions/c:Uri[1]/c:uri/text()", doc, XPathConstants.STRING);
        String rank = (String) xpath.evaluate("/c:job/c:slowTransactions/c:Uri[1]/c:rank/text()", doc, XPathConstants.STRING);
        assertEquals("1", rank);
        assertEquals(uri, "URI" + rank);

        uri = (String) xpath.evaluate("/c:job/c:slowTransactions/c:Uri[last()]/c:uri/text()", doc, XPathConstants.STRING);
        rank = (String) xpath.evaluate("/c:job/c:slowTransactions/c:Uri[last()]/c:rank/text()", doc, XPathConstants.STRING);
        assertEquals("6", rank);
        assertEquals(uri, "URI" + rank);
	}

	@Test
    void testToXMLWithEmptyJobStats() {
        DocumentBuilderFactory documentBuilderFactory = XmlUtils.newSecureDocumentBuilderFactoryInstance();
        List<JobStats> jobStatsList = new ArrayList<>();
        Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, true);
        assertNotNull(doc);
        assertEquals(0,doc.getDocumentElement().getChildNodes().getLength());
    }

    @Test
    void testToXMLRedactUris() {
        DocumentBuilderFactory documentBuilderFactory = XmlUtils.newSecureDocumentBuilderFactoryInstance();
        List<JobStats> jobStatsList = new ArrayList<>();
        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setShouldRedactUris(true);
        when(manager.getOptions()).thenReturn(transformOptions);
        JobStats jobStat = new JobStats(manager);
        PausableThreadPoolExecutor threadPool = mock(PausableThreadPoolExecutor.class);
        List<String> failed = new ArrayList<>();
        failed.add("uri1");
        when(threadPool.getFailedUris()).thenReturn(failed);

        Map<String, Long> top = new HashMap<>();
        top.put("uri", 10000L);
        when(threadPool.getTopUris()).thenReturn(top);
        jobStat.refreshThreadPoolExecutorStats(threadPool);
        jobStatsList.add(jobStat);
        Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, false);
        assertNotNull(doc);
        Element documentElement = doc.getDocumentElement();
        assertNotNull(documentElement);
        assertEquals(0, documentElement.getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "failedTransactions").getLength());
        assertEquals(0, documentElement.getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "slowTransactions").getLength());
    }

    @Test
    void testToXMLDoNotRedactUris() {
        DocumentBuilderFactory documentBuilderFactory = XmlUtils.newSecureDocumentBuilderFactoryInstance();
        List<JobStats> jobStatsList = new ArrayList<>();
        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setShouldRedactUris(false);
        when(manager.getOptions()).thenReturn(transformOptions);
        JobStats jobStat = new JobStats(manager);
        PausableThreadPoolExecutor threadPool = mock(PausableThreadPoolExecutor.class);
        List<String> failed = new ArrayList<>();
        failed.add("uri1");
        when(threadPool.getFailedUris()).thenReturn(failed);

        Map<String, Long> top = new HashMap<>();
        top.put("uri", 10000L);
        when(threadPool.getTopUris()).thenReturn(top);

        jobStat.refreshThreadPoolExecutorStats(threadPool);
        jobStatsList.add(jobStat);
        Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, false);
        assertNotNull(doc);
        Element documentElement = doc.getDocumentElement();
        assertNotNull(documentElement);
        assertEquals(1, documentElement.getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "failedTransactions").getLength());
        assertEquals(1, documentElement.getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "slowTransactions").getLength());
    }

    @Test
    void testToXMLParserConfigurationException() {
        DocumentBuilderFactory documentBuilderFactory = mock(DocumentBuilderFactory.class);
        try {
            when(documentBuilderFactory.newDocumentBuilder()).thenThrow(ParserConfigurationException.class);
            List<JobStats> jobStatsList = new ArrayList<>();
            Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, true);
            assertNull(doc);
        } catch (ParserConfigurationException ex) {
            fail();
        }
    }

    @Test
    void testToXML() {
        DocumentBuilderFactory documentBuilderFactory = XmlUtils.newSecureDocumentBuilderFactoryInstance();
        List<JobStats> jobStatsList = new ArrayList<>();

        Manager manager = mock(Manager.class);
        jobStatsList.add(new JobStats(manager));
        jobStatsList.add(null);
        jobStatsList.add(new JobStats(manager));

        Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, true);
        assertNotNull(doc);
        assertEquals(2, doc.getDocumentElement().getChildNodes().getLength());
    }

	@Test
    void testLogToServerNullContentSource() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        try {
            when(csp.get()).thenReturn(null);
            when(manager.getContentSourcePool()).thenReturn(csp);
            when(manager.getOptions()).thenReturn(mock(TransformOptions.class));
            JobStats jobStats = new JobStats(manager);
            jobStats.logToServer(FOO, BAR);
            verify(contentSource, never()).newSession();
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testLogToServerException() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        try {
            when(csp.get()).thenReturn(null);
            when(manager.getContentSourcePool()).thenReturn(csp);
            when(manager.getOptions()).thenReturn(mock(TransformOptions.class));

            JobStats jobStats = new JobStats(manager);
            jobStats.logToServer(FOO, BAR);
            verify(contentSource, never()).newSession();
        } catch (CorbException ex) {
            fail();
        }
    }

	@Test
    void testLogToServerDefaultLevelNone() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        when(manager.getOptions()).thenReturn(mock(TransformOptions.class));
        JobStats jobStats = new JobStats(manager);
        try {
            jobStats.logToServer(contentSource, FOO, BAR);
            verify(contentSource, never()).newSession();
        } catch (RequestException ex) {
            fail();
        }
    }

    @Test
    void testLogToServerWithLogLevel() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setLogMetricsToServerLog("info");
        when(manager.getOptions()).thenReturn(transformOptions);
        when(contentSource.newSession()).thenReturn(mock(Session.class));
        JobStats jobStats = new JobStats(manager);
        try {
            jobStats.logToServer(contentSource, FOO, BAR);
            verify(contentSource, times(1)).newSession();
        } catch (RequestException ex) {
            fail();
        }
    }

    @Test
    void testLogToServerWithNullMessage() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setLogMetricsToServerLog("info");
        when(manager.getOptions()).thenReturn(transformOptions);
        when(contentSource.newSession()).thenReturn(mock(Session.class));
        JobStats jobStats = new JobStats(manager);
        try {
            jobStats.logToServer(contentSource, null, BAR);
            verify(contentSource, times(1)).newSession();
        } catch (RequestException ex) {
            fail();
        }
    }

    @Test
    void testLogToServerWithRequestException() throws RequestException {
        ContentSource contentSource = mock(ContentSource.class);

        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        Request request = mock(Request.class);
        SessionImpl session = mock(SessionImpl.class);
        transformOptions.setLogMetricsToServerLog("info");
        when(manager.getOptions()).thenReturn(transformOptions);
        when(session.getServerVersion()).thenReturn("MarkLogic TEST.0");
        when(session.newAdhocQuery(anyString())).thenReturn(mock(AdhocQuery.class));
        when(request.getSession()).thenReturn(session);
        when(contentSource.newSession()).thenReturn(session);
        when(session.submitRequest(any())).thenThrow(new RequestException("Boom!", request));
        JobStats jobStats = new JobStats(manager);

        assertThrows(RequestException.class, () -> jobStats.logToServer(contentSource, null, BAR));
    }

    @Test
    void testExecuteModuleWithoutDatabaseConfigured() {
        ContentSource contentSource = mock(ContentSource.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        when(manager.getOptions()).thenReturn(transformOptions);
        when(manager.getContentSourcePool()).thenReturn(csp);
        try {
            when(csp.get()).thenReturn(contentSource);
            when(contentSource.newSession()).thenReturn(mock(Session.class));
            JobStats jobStats = new JobStats(manager);
            jobStats.executeModule(FOO);
            verify(contentSource, never()).newSession();
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testExecuteModule() {
        ContentSource contentSource = mock(ContentSource.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        Manager manager = mock(Manager.class);
        when(manager.getContentSourcePool()).thenReturn(csp);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setMetricsDatabase(METRICS_DB);
        when(manager.getOptions()).thenReturn(transformOptions);
        when(manager.getRequestForModule(any(),any())).thenReturn(mock(Request.class));
        Session session = mock(Session.class);

        try {
            when(session.submitRequest(any())).thenReturn(mock(ResultSequence.class));
            when(csp.get()).thenReturn(contentSource);
            when(contentSource.newSession()).thenReturn(session);
            JobStats jobStats = new JobStats(manager);
            jobStats.executeModule(FOO);
            verify(contentSource, times(1)).newSession();
        } catch (CorbException | RequestException ex) {
            fail();
        }
    }

    @Test
    void testExecuteModuleWithoutCSP() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setMetricsDatabase(METRICS_DB);
        when(manager.getOptions()).thenReturn(transformOptions);
        when(contentSource.newSession()).thenReturn(mock(Session.class));
        JobStats jobStats = new JobStats(manager);
        jobStats.executeModule(FOO);
        verify(contentSource, never()).newSession();
    }

    @Test
    void testExecuteModuleWithNullContentSource() {
        ContentSource contentSource = mock(ContentSource.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        Manager manager = mock(Manager.class);
        when(manager.getContentSourcePool()).thenReturn(csp);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setMetricsDatabase(METRICS_DB);
        when(manager.getOptions()).thenReturn(transformOptions);
        try {
            when(csp.get()).thenReturn(null);
            JobStats jobStats = new JobStats(manager);
            jobStats.executeModule(FOO);
            verify(contentSource, never()).newSession();
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testExecuteModuleContentSourcePoolGetThrows()  {
        ContentSource contentSource = mock(ContentSource.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        Manager manager = mock(Manager.class);
        when(manager.getContentSourcePool()).thenReturn(csp);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setMetricsDatabase(METRICS_DB);
        when(manager.getOptions()).thenReturn(transformOptions);
        try {
            when(csp.get()).thenThrow(CorbException.class);

            JobStats jobStats = new JobStats(manager);
            jobStats.executeModule(FOO);
            verify(contentSource, never()).newSession();
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void testRefreshMonitorStatsWhenDone() {
        Manager manager = mock(Manager.class);
        Monitor monitor = mock(Monitor.class);
        when(monitor.getTaskCount()).thenReturn(1L);
        when(monitor.getCompletedCount()).thenReturn(1L);
        when(manager.getEndMillis()).thenReturn(5L);
        when(manager.getStartMillis()).thenReturn(1L);
        JobStats jobStats = new JobStats(manager);
        jobStats.refreshMonitorStats(monitor);
        assertNull(jobStats.estimatedTimeOfCompletion);
    }

    @Test
    void testAddFailedUris() {
        Manager manager = mock(Manager.class);
        PausableThreadPoolExecutor threadPoolExecutor = mock(PausableThreadPoolExecutor.class);
        List<String> uris = new ArrayList<>(1);
        uris.add("uri1");
        when(threadPoolExecutor.getFailedUris()).thenReturn(uris);
        JobStats jobStats = new JobStats(manager);
        jobStats.refreshThreadPoolExecutorStats(threadPoolExecutor);
        try {
            Document document = XmlUtils.newSecureDocumentBuilderFactoryInstance().newDocumentBuilder().newDocument();
            Element element = document.createElement("foo");
            document.appendChild(element);
            jobStats.addFailedUris(element);
            assertTrue(element.hasChildNodes());
        } catch (ParserConfigurationException ex) {
            fail();
        }
    }

    @Test
    void testLogMetrics() throws CorbException {
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setLogMetricsToServerLog("INFO");
        Manager manager = mock(Manager.class);
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        when(contentSourcePool.get()).thenThrow(new RuntimeException("boom!"));
        when(manager.getOptions()).thenReturn(transformOptions);
        when(manager.getContentSourcePool()).thenReturn(contentSourcePool);

        JobStats jobStats = new JobStats(manager);
        assertThrows(RuntimeException.class, () -> jobStats.logMetrics("test", true, true));
    }

    @Test
    void testLogMetricsWhenMetricsLogLevelNone() throws CorbException {
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setLogMetricsToServerLog("NONE");
        Manager manager = mock(Manager.class);
        ContentSourcePool contentSourcePool = mock(ContentSourcePool.class);
        when(contentSourcePool.get()).thenThrow(new RuntimeException("boom!"));
        when(manager.getOptions()).thenReturn(transformOptions);
        when(manager.getContentSourcePool()).thenReturn(contentSourcePool);
        JobStats jobStats = new JobStats(manager);
        assertDoesNotThrow( () -> jobStats.logMetrics("test", true, true));
    }
}
