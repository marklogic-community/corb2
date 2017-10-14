package com.marklogic.developer.corb;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

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
    public void isNumeric() throws Exception {
        assertTrue(JobStats.isNumeric("1.0"));
        assertFalse(JobStats.isNumeric("one"));
    }

    @Test
    public void testXmlNodeInteger() {
        Integer value = null;
        assertEquals("", JobStats.xmlNode(FOO, value));
        value = -1;
        assertEquals("", JobStats.xmlNode(FOO, value));
        value = 0;
        assertEquals("<foo>0</foo>", JobStats.xmlNode(FOO, value));
        value = 5;
        assertEquals("<foo>5</foo>", JobStats.xmlNode(FOO, value));
    }

    @Test
    public void testXmlNodeDouble() {
        Double value = null;
        assertEquals("", JobStats.xmlNode(FOO, value));
        value = -1.0d;
        assertEquals("", JobStats.xmlNode(FOO, value));
        value = 0.0d;
        assertEquals("<foo>0.0</foo>", JobStats.xmlNode(FOO, value));
        value = 5d;
        assertEquals("<foo>5.0</foo>", JobStats.xmlNode(FOO, value));
    }

    @Test
    public void testToString() {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        assertEquals(jobStats.toString(), jobStats.toString(true));
        assertNotEquals(jobStats.toString(), jobStats.toString(false));
    }

    @Test
    public void testXmlNodeArray() {
        List<String> value = new ArrayList<>(3);
        value.add("");
        value.add("a");
        value.add("1");
        assertEquals("<foo><foo>a</foo><foo>1</foo></foo>", JobStats.xmlNodeArray(FOO, FOO, value));
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
		String xml = jobStats.toXMLString();
		assertTrue("Empty Failed URIs", xml != null);
		assertTrue("Job is  null", jobStats.toJSONString() != null);
		assertTrue("Job is  null", jobStats.toJSONString(true) != null);
	}

	@Test
	public void testXMLRanks() throws ParserConfigurationException, SAXException, IOException {
		Map<String, Long> nodeVal = new HashMap<>();
		nodeVal.put("URI1", 6L);
		nodeVal.put("URI2", 5L);
		nodeVal.put("URI3", 4L);
		nodeVal.put("URI4", 3L);
		nodeVal.put("URI5", 2L);
		nodeVal.put("URI6", 1L);
		Manager manager = new Manager();
		PausableThreadPoolExecutor threadPoolExecutor = mock(PausableThreadPoolExecutor.class);
		when(threadPoolExecutor.getTopUris()).thenReturn(nodeVal);
		Monitor monitor = new Monitor(threadPoolExecutor, mock(CompletionService.class), manager);
		monitor.setTaskCount(1);
        manager.monitor = monitor;
        JobStats jobStats = new JobStats(manager);

		assertXMLJSONNotNull(jobStats);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(new InputSource(new StringReader(jobStats.toXMLString())));
		String uri = doc.getElementsByTagName("Uri").item(0).getFirstChild().getFirstChild().getNodeValue();
		String rank = doc.getElementsByTagName("Uri").item(0).getFirstChild().getNextSibling().getFirstChild().getNodeValue();
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
