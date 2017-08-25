package com.marklogic.developer.corb;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import static org.junit.Assert.*;

public class JobStatsTest {
    private static final String FOO = "foo";

    @Test
    public void getHostName() throws Exception {
        TransformOptions transformOptions = new TransformOptions();
        transformOptions.setJobName("JobStatsTest");
        Manager manager = new Manager();
        manager.options = transformOptions;
        JobStats jobStats = new JobStats(manager);
        assertNotNull(jobStats.getHostName());
        assertEquals(transformOptions.getJobName(), jobStats.getJobName());
    }

    @Test
    public void epochMillisAsFormattedDateString() throws Exception {
        assertEquals(20, JobStats.epochMillisAsFormattedDateString(0).length());
    }

    @Test
    public void getJobName() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setJobName(FOO);
        assertEquals(FOO, jobStats.getJobName());
    }

    @Test
    public void getJobRunLocation() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setJobRunLocation(FOO);
        assertEquals(FOO, jobStats.getJobRunLocation());
    }

    @Test
    public void getUserProvidedOptions() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        Map<String, String> options = new HashMap<>();
        options.put(FOO, "bar");
        jobStats.setUserProvidedOptions(options);
        assertEquals(options, jobStats.getUserProvidedOptions());
    }

    @Test
    public void getStartTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setStartTime(FOO);
        assertEquals(FOO, jobStats.getStartTime());
    }

    @Test
    public void getEndTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setEndTime(FOO);
        assertEquals(FOO, jobStats.getEndTime());
    }

    @Test
    public void getHost() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setHost(FOO);
        assertEquals(FOO, jobStats.getHost());
    }

    @Test
    public void getNumberOfFailedTasks() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setNumberOfFailedTasks(6l);
        assertEquals(6l, jobStats.getNumberOfFailedTasks(), 0.001);

        jobStats.setNumberOfFailedTasks(7);
        assertEquals(7l, jobStats.getNumberOfFailedTasks(), 0.001);
    }

    @Test
    public void getAverageTransactionTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setAverageTransactionTime(7d);
        assertEquals(7d, jobStats.getAverageTransactionTime(), 0.001);
    }

    @Test
    public void getUrisLoadTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setUrisLoadTime(8l);
        assertEquals(8l, jobStats.getUrisLoadTime(), 0.001);
    }

    @Test
    public void getTopTimeTakingUris() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        Map<String, Long> uris = new HashMap<>(1);
        uris.put("/some/uri", Long.valueOf(9l));
        jobStats.setTopTimeTakingUris(uris);
        assertEquals(uris, jobStats.getTopTimeTakingUris());
    }

    @Test
    public void isNumeric() throws Exception {
        assertTrue(JobStats.isNumeric("1.0"));
        assertFalse(JobStats.isNumeric("one"));
    }

    @Test
    public void setFailedUris() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        List<String> uris = new ArrayList();
        uris.add("/some/uri");
        jobStats.setFailedUris(uris);
        assertEquals(uris, jobStats.getFailedUris());
    }

    @Test
    public void setPreBatchRunTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setPreBatchRunTime(1l);
        assertEquals(Long.valueOf(1l), jobStats.getPreBatchRunTime());
    }

    @Test
    public void setPostBatchRunTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setPostBatchRunTime(1l);
        assertEquals(Long.valueOf(1l), jobStats.getPostBatchRunTime());
    }

    @Test
    public void setInitTaskRunTime() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setInitTaskRunTime(1l);
        assertEquals(Long.valueOf(1l), jobStats.getInitTaskRunTime());
    }

    @Test
    public void setNumberOfSucceededTasks() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setNumberOfSucceededTasks(1l);
        assertEquals(Long.valueOf(1l), jobStats.getNumberOfSucceededTasks());

        jobStats.setNumberOfSucceededTasks(2);
        assertEquals(Long.valueOf(2l), jobStats.getNumberOfSucceededTasks());
    }

    @Test
    public void setTotalRunTimeInMillis() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setTotalRunTimeInMillis(1l);
        assertEquals(Long.valueOf(1l), jobStats.getTotalRunTimeInMillis());
    }

    @Test
    public void setUri() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setUri("/some/uri");
        assertEquals("/some/uri", jobStats.getUri());
    }

    @Test
    public void setPaused() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setPaused("true");
        assertEquals("true", jobStats.getPaused());
    }

    @Test
    public void setCurrentThreadCount() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setCurrentThreadCount(1l);
        assertEquals(Long.valueOf(1l), jobStats.getCurrentThreadCount());
    }

    @Test
    public void setCurrentTps() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setCurrentTps(1d);
        assertEquals(1d, jobStats.getCurrentTps(), 0.001);
    }
    @Test
    public void setAvgTps() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setAvgTps(1d);
        assertEquals(1d, jobStats.getAvgTps(), 0.001);
    }

    @Test
    public void setEstimatedTimeOfCompletion() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setEstimatedTimeOfCompletion(FOO);
        assertEquals(FOO, jobStats.getEstimatedTimeOfCompletion());
    }

    @Test
    public void setJobServerPort() throws Exception {
        Manager manager = new Manager();
        JobStats jobStats = new JobStats(manager);
        jobStats.setJobServerPort(1l);
        assertEquals(Long.valueOf(1l), jobStats.getJobServerPort());
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
		jobStats.setJobName(null);
		assertXMLJSONNotNull(jobStats);
	}

	@Test
	public void testFailedUris() {
        Manager manager = new Manager();
		JobStats jobStats = new JobStats(manager);
		jobStats.setJobName("Name");
		jobStats.setFailedUris(new ArrayList<>());
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
		nodeVal.put("URI1", 6l);
		nodeVal.put("URI2", 5l);
		nodeVal.put("URI3", 4l);
		nodeVal.put("URI4", 3l);
		nodeVal.put("URI5", 2l);
		nodeVal.put("URI6", 1l);
		Manager manager = new Manager();
		JobStats jobStats = new JobStats(manager);
		jobStats.setTopTimeTakingUris(nodeVal);
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
