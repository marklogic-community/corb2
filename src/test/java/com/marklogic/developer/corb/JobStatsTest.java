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

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobStatsTest {
    private static final String FOO = "foo";

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
        assertEquals(jobStats.toString(), jobStats.toString(true));
//        assertNotEquals(jobStats.toString(), jobStats.toString(false));
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
        nodeVal.put("URI6", 1l);
        nodeVal.put("URI3", 4l);
        nodeVal.put("URI1", 6l);
		nodeVal.put("URI2", 5l);
		nodeVal.put("URI4", 3l);
		nodeVal.put("URI5", 2l);

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
                return prefix.equals("c") ? JobStats.CORB_NAMESPACE : null;
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
