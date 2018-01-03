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

import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class JobStatsTest {
    private static final String FOO = "foo";
    private static final String BAR = "bar";
    private static final String METRICS_DB = "metricsDB";

    @Test
    public void testGetAverageTransactionTimeNoCompletedTasks() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        long totalTime = 5000L;
        long failedTasks = 0L;
        long successfulTasks = 0L;
        assertFalse(Double.isInfinite(jobStats.getAverageTransactionTime(totalTime, failedTasks, successfulTasks)));
    }

    @Test
    public void testGetAverageTransactionNoValues() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        long totalTime = 0L;
        long failedTasks = 0L;
        long successfulTasks = 0L;
        assertFalse(Double.isInfinite(jobStats.getAverageTransactionTime(totalTime, failedTasks, successfulTasks)));
    }

    @Test
    public void testGetAverageTransactionTimeNoFailed() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        long totalTime = 50L;
        long failedTasks = 0L;
        long successfulTasks = 3000L;
        assertFalse(Double.isInfinite(jobStats.getAverageTransactionTime(totalTime, failedTasks, successfulTasks)));
    }

    @Test
    public void testGetAverageTransactionTimeNoSuccessful() {
        Manager manager = mock(Manager.class);
        JobStats jobStats = new JobStats(manager);
        long totalTime = 50L;
        long failedTasks = 300L;
        long successfulTasks = 0L;
        assertFalse(Double.isInfinite(jobStats.getAverageTransactionTime(totalTime, failedTasks, successfulTasks)));
    }

    @Test
    public void getHostName() throws Exception {
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setJobName("JobStatsTest");
        Manager manager = new Manager();
        manager.options = transformOptions;
        JobStats jobStats = new JobStats(manager);
        assertNotNull(jobStats.getHost());
    }

    @Test
    public void epochMillisAsFormattedDateString() throws Exception {
        assertEquals(20, JobStats.epochMillisAsFormattedDateString(0).length());
    }

    @Test
    public void testToString() {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        assertTrue(jobStats.toString().startsWith("{"));
        assertTrue(jobStats.toString().endsWith("}"));
    }

    @Test
	public void testNullName() {
	    Manager manager = new Manager();
		JobStats jobStats = new JobStats(manager);

		assertXMLJSONNotNull(jobStats);
	}

	@Test
	public void testFailedUris() {
        Manager manager = new Manager();
        TransformOptions options = new TransformOptions();
        options.setJobName("myJob");
        manager.options = options;
		JobStats jobStats = new JobStats(manager);

		assertXMLJSONNotNull(jobStats);
	}

	private void assertXMLJSONNotNull(JobStats jobStats) {
		String xml = jobStats.toXmlString();
		assertTrue("Empty Failed URIs", xml != null);
		assertTrue("Job is  null", jobStats.toJSON() != null);
		assertTrue("Job is  null", jobStats.toJSON(true) != null);
	}

	@Test
	public void testXMLRanks() throws ParserConfigurationException, SAXException, IOException, XPathExpressionException {
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
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
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
        assertEquals("Rank is Correct", "1", rank);
        assertTrue("Rank is Correct", uri.equals("URI" + rank));

        uri = (String) xpath.evaluate("/c:job/c:slowTransactions/c:Uri[last()]/c:uri/text()", doc, XPathConstants.STRING);
        rank = (String) xpath.evaluate("/c:job/c:slowTransactions/c:Uri[last()]/c:rank/text()", doc, XPathConstants.STRING);
        assertEquals("Rank is Correct", "6", rank);
        assertTrue("Rank is Correct", uri.equals("URI" + rank));
	}

	@Test
    public void testToXMLWithEmptyJobStats() {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        List<JobStats> jobStatsList = new ArrayList<>();
        Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, true);
        assertNotNull(doc);
        assertEquals(0,doc.getDocumentElement().getChildNodes().getLength());
    }

    @Test
    public void testToXMLRedactUris() {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
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

        assertEquals(0, doc.getDocumentElement().getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "failedTransactions").getLength());
        assertEquals(0, doc.getDocumentElement().getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "slowTransactions").getLength());
    }

    @Test
    public void testToXMLDoNotRedactUris() {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
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

        assertEquals(1, doc.getDocumentElement().getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "failedTransactions").getLength());
        assertEquals(1, doc.getDocumentElement().getElementsByTagNameNS(JobStats.CORB_NAMESPACE, "slowTransactions").getLength());
    }

    @Test
    public void testToXMLParserConfigurationException() {
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
    public void testToXML() {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        List<JobStats> jobStatsList = new ArrayList<>();

        Manager manager = mock(Manager.class);
        jobStatsList.add(new JobStats(manager));
        jobStatsList.add(null);
        jobStatsList.add(new JobStats(manager));

        Document doc = JobStats.toXML(documentBuilderFactory, jobStatsList, true);
        assertEquals(2, doc.getDocumentElement().getChildNodes().getLength());
    }

	@Test
    public void testLogToServerNullContentSource() {
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
    public void testLogToServerException() {
        ContentSource contentSource = mock(ContentSource.class);
        Manager manager = mock(Manager.class);
        ContentSourcePool csp = mock(ContentSourcePool.class);
        try {
            when(csp.get()).thenThrow(RequestException.class);
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
    public void testLogToServerDefaultLevelNone() {
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
    public void testLogToServerWithLogLevel() {
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
    public void testLogToServerWithNullMessage() {
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

    @Test (expected = RequestException.class)
    public void testLogToServerWithRequestException() throws RequestException {
        ContentSource contentSource = mock(ContentSource.class);

        Manager manager = mock(Manager.class);
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setLogMetricsToServerLog("info");
        when(manager.getOptions()).thenReturn(transformOptions);
        when(contentSource.newSession()).thenThrow(RequestException.class);
        JobStats jobStats = new JobStats(manager);

        jobStats.logToServer(contentSource, null, BAR);
    }

    @Test
    public void testExecuteModuleWithoutDatabaseConfigured() {
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
    public void testExecuteModule() {
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
    public void testExecuteModuleWithoutCSP() {
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
    public void testExecuteModuleWithNullContentSource() {
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
    public void testExecuteModuleContentSourcePoolGetThrows()  {
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
    public void testRefreshMonitorStatsWhenDone() {
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
    public void testAddFailedUris() {

    }

    @Test
    public void testAddFailedUrisWhenNull() {
        Manager manager = mock(Manager.class);
        PausableThreadPoolExecutor threadPoolExecutor = mock(PausableThreadPoolExecutor.class);
        List<String> uris = new ArrayList<>(1);
        uris.add("uri1");
        when(threadPoolExecutor.getFailedUris()).thenReturn(uris);
        JobStats jobStats = new JobStats(manager);
        jobStats.refreshThreadPoolExecutorStats(threadPoolExecutor);
        try {
            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element element = document.createElement("foo");
            document.appendChild(element);
            jobStats.addFailedUris(element);
            assertTrue(element.hasChildNodes());
        } catch (ParserConfigurationException ex) {
            fail();
        }
    }
	/*
	 * 1: Log once at the start and once at the end XML
	 * 		Log to DB Document
	 * 		Log to ML Error Log
	 * 		Log to Java console
	 * 2: Log once at the start and once at the end JSON
	 * 		Log to DB Document
	 * 		Log to ML Error Log
	 * 		Log to Java console
	 * 3: Log periodically XML or JSON
	 * 		Fewer fields in concise mode for periodic logging
	 * 		End time and average traansaction time is shown on the last entry
	 * 		Should only have one entry with end time
	 * 		Should pause syncing when job is paused
	 * 		Should log one extra full record when pausing
	 * 5: COLLECTION Name
	 * 6: Root directory
	 * 7: Job run location can't be found and Job name not specified?
	 * 8: Job Server Port
	 * 		Specify single port
	 * 		Specify range
	 * 		Specify multiple ranges or multiple ports
	 * 9: UI Validation
	 * 		All fields
	 */
}
