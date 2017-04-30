package com.marklogic.developer.corb;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class JobStatsTest {
	@Test
	public void testNullName() {
		JobStats jobStats = new JobStats();
		jobStats.setJobName(null);
		assertXMLJSONNotNull(jobStats);
	}

	@Test
	public void testFailedUris() {
		JobStats jobStats = new JobStats();
		jobStats.setJobName("Name");
		jobStats.setFailedUris(new ArrayList<String>());
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
		Map<String, Long> nodeVal = new HashMap<String, Long>();
		nodeVal.put("URI1", 6l);
		nodeVal.put("URI2", 5l);
		nodeVal.put("URI3", 4l);
		nodeVal.put("URI4", 3l);
		nodeVal.put("URI5", 2l);
		nodeVal.put("URI6", 1l);
		JobStats jobStats = new JobStats();
		jobStats.setTopTimeTakingUris(nodeVal);
		assertXMLJSONNotNull(jobStats);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(new InputSource(new StringReader(jobStats.toXMLString())));
		String uri = (doc.getElementsByTagName("Uri").item(0).getFirstChild().getFirstChild().getNodeValue()
				.toString());
		String rank = (doc.getElementsByTagName("Uri").item(0).getFirstChild().getNextSibling().getFirstChild()
				.getNodeValue().toString());
		assertTrue("Rank is Correct", uri.equals("URI" + rank));
	}
}
