package com.marklogic.developer.corb;

import static java.util.logging.Level.INFO;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class JobStats {
	private static final String CLOSE_SQUARE = "]";
	private static final String GT = ">";
	private static final String LT = "<";
	private static final String OPEN_CURLY = "{";
	private static final String CLOSE_CURLY = "}";
	private static final String COMA = ",";
	private static final String URI = "uri";
	private static final String JOB_NAME = "name";
	private static final String JOB_ROOT = "job";
	private static final String DEF_NS = "http://marklogic.github.io/corb/";
	private static final String LONG_RUNNING_URIS = "slowTransactions";
	private static final String FAILED_URIS = "failedTransactions";	
	private static final String URIS_LOAD_TIME = "urisLoadTimeInMillis";
	private static final String INIT_TASK_TIME = "initTaskTimeInMillis";
	private static final String PRE_BATCH_RUN_TIME = "preBatchRunTimeInMillis";
	private static final String POST_BATCH_RUN_TIME = "postBatchRunTimeInMillis";
	private static final String AVERAGE_TRANSACTION_TIME = "averageTransactionTimeInMillis";
	private static final String TOTAL_NUMBER_OF_TASKS = "totalNumberOfTasks";
	private static final String NUMBER_OF_FAILED_TASKS = "numberOfFailedTasks";
	
	private static final String HOST = "host";
	private static final String END_TIME = "endTime";
	private static final String USER_PROVIDED_OPTIONS = "userProvidedOptions";
	private static final String JOB_LOCATION = "runLocation";
	protected Map<String, String> userProvidedOptions = new HashMap<String, String>();
	protected String startTime = null;
	protected String endTime = null;
	protected String host = null;
	protected Long totalNumberOfTasks = null;
	protected Long numberOfFailedTasks = null;
	protected Double averageTransactionTime = null;
	protected Long urisLoadTime = null;
	private Long preBatchRunTime = 0l;
    private Long postBatchRunTime = 0l;
    private Long initTaskRunTime = 0l;
	protected String jobRunLocation = null;
	protected String jobName = null;
	protected Map<String, Long> longRunningUris = new HashMap<String, Long>();
	protected List<String> failedUris = null;

	private static final Logger LOG = Logger.getLogger(JobStats.class.getName());

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobRunLocation() {
		return jobRunLocation;
	}

	public void setJobRunLocation(String jobRunLocation) {
		this.jobRunLocation = jobRunLocation;
	}

	public Map<String, String> getUserProvidedOptions() {
		return userProvidedOptions;
	}

	public void setUserProvidedOptions(Map<String, String> userProvidedOptions) {
		this.userProvidedOptions = userProvidedOptions;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Long getTotalNumberOfTasks() {
		return totalNumberOfTasks;
	}

	public void setTotalNumberOfTasks(Long totalNumberOfTasks) {
		this.totalNumberOfTasks = totalNumberOfTasks;
	}

	public Long getNumberOfFailedTasks() {
		return numberOfFailedTasks;
	}

	public void setNumberOfFailedTasks(Long numberOfFailedTasks) {
		this.numberOfFailedTasks = numberOfFailedTasks;
	}

	public Double getAverageTransactionTime() {
		return averageTransactionTime;
	}

	public void setAverageTransactionTime(Double averageTransactionTime) {
		this.averageTransactionTime = averageTransactionTime;
	}

	public Long getUrisLoadTime() {
		return urisLoadTime;
	}

	public void setUrisLoadTime(Long urisLoadTime) {
		this.urisLoadTime = urisLoadTime;
	}

	public Map<String, Long> getTopTimeTakingUris() {
		return longRunningUris;
	}

	public void setTopTimeTakingUris(Map<String, Long> topTimeTakingUris) {
		this.longRunningUris = topTimeTakingUris;
	}
	@Override
	public String toString(){
		return toString(true);
	}
	public String toString(boolean concise) {
		return toJSONString(concise);
	}
	public String toXMLString() {
		return toXMLString(false);
	}
	public String toXMLString(boolean concise) {
		StringBuffer strBuff = new StringBuffer();
		strBuff.append(xmlNode(JOB_LOCATION, this.jobRunLocation)).append(xmlNode(JOB_NAME, this.jobName))
				.append(concise?"":xmlNode(USER_PROVIDED_OPTIONS, userProvidedOptions)).append(xmlNode("StartTime", startTime))
				.append(xmlNode(END_TIME, endTime)).append(xmlNode(HOST, host))
				.append(xmlNode(TOTAL_NUMBER_OF_TASKS, totalNumberOfTasks))
				.append(xmlNode(NUMBER_OF_FAILED_TASKS, numberOfFailedTasks))
				.append(xmlNode(AVERAGE_TRANSACTION_TIME, averageTransactionTime))
				.append(xmlNode(URIS_LOAD_TIME, urisLoadTime))
				.append(xmlNode(PRE_BATCH_RUN_TIME, preBatchRunTime))
				.append(xmlNode(POST_BATCH_RUN_TIME, postBatchRunTime))
				.append(xmlNode(INIT_TASK_TIME, initTaskRunTime))
				.append(concise?"":xmlNodeArray(FAILED_URIS, URI,failedUris))
				.append(concise?"":xmlNodeRanks(LONG_RUNNING_URIS, longRunningUris));
		return xmlNode(JOB_ROOT, strBuff.toString(),DEF_NS);
	}
	public String toJSONString() {
		return toJSONString(false);
	}
	public String toJSONString(boolean concise) {
		String xml = toXMLString(concise);
		return getJsonFromXML(xml);
	}

	private String getJsonFromXML(String xml) {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new InputSource(new StringReader(xml)));

			StringBuffer strBuff = new StringBuffer();
			Node root = doc.getDocumentElement();
			strBuff.append("{\"").append(root.getNodeName()).append("\":{");
			strBuff.append(getJson(root));
			strBuff.append("}}");
			return strBuff.toString();
		} catch (Exception e) {
			// unable to generate json
			// Log and continue
			LOG.log(INFO, "Unable to generate JSON document", e);
		}
		return "";
	}

	private String getJson(Node node) {
		StringBuffer strBuff = new StringBuffer();

		NodeList nodeList = node.getChildNodes();
		for (int i = 0; i < nodeList.getLength(); i++) {
			if (i != 0) {
				strBuff.append(COMA);
			}
			Node child = nodeList.item(i);
			if (child.getNodeType() == Node.ELEMENT_NODE) {
				if (child.getNodeName().equals(LONG_RUNNING_URIS)) {
					strBuff.append("\"").append(child.getNodeName()).append("\":[");
					NodeList uris = child.getChildNodes();
					for (int j = 0; j < uris.getLength(); j++) {
						if (uris.item(j).hasChildNodes()) {
							if (j != 0) {
								strBuff.append(COMA);
							}
							strBuff.append(OPEN_CURLY);
							NodeList innerUris = uris.item(j).getChildNodes();
							for (int k = 0; k < innerUris.getLength(); k++) {
								Node uri = innerUris.item(k);
								if (k != 0) {
									strBuff.append(COMA);
								}
								strBuff.append("\"").append(uri.getNodeName()).append("\":\"")
										.append(uri.getTextContent()).append("\"");
							}
							strBuff.append(CLOSE_CURLY);
						}
					}
					strBuff.append(CLOSE_SQUARE);
				} else if (child.getNodeName().equals(FAILED_URIS)) {
					strBuff.append("\"").append(child.getNodeName()).append("\":[");
					NodeList uris = child.getChildNodes();
					for (int j = 0; j < uris.getLength(); j++) {
						if (uris.item(j).hasChildNodes()) {
							if (j != 0) {
								strBuff.append(COMA);
							}
							NodeList innerUris = uris.item(j).getChildNodes();
							for (int k = 0; k < innerUris.getLength(); k++) {
								Node uri = innerUris.item(k);
								if (k != 0) {
									strBuff.append(COMA);
								}
								strBuff.append("\"").append(uri.getTextContent()).append("\"");
							}
							
						}
					}
					strBuff.append(CLOSE_SQUARE);
				}
				else if (child.hasChildNodes() && child.getFirstChild().getNodeType() == Node.TEXT_NODE) {
					strBuff.append("\"").append(child.getNodeName()).append("\":\"").append(child.getTextContent())
							.append("\"");
				} else {
					strBuff.append("\"").append(child.getNodeName()).append("\":"
							+ OPEN_CURLY);
					strBuff.append(getJson(child));
					strBuff.append(CLOSE_CURLY);
				}
			}

		}
		return strBuff.toString();
	}
	private String xmlNode(String nodeName, String nodeVal) {
		return xmlNode(nodeName, nodeVal,null);
	}
	private String xmlNode(String nodeName, Number nodeVal) {
		if(nodeVal !=null && nodeVal.longValue()>0l){
			return xmlNode(nodeName, nodeVal.toString(),null);
		}
		else{
			return "";
		}
	}
	private String xmlNodeArray(String nodeName, String childNodeName,List<String> nodeVals) {
		if(nodeVals!=null && nodeVals.size()>0){
			
			StringBuffer strBuff = new StringBuffer();
			for (String nodeVal : nodeVals) {
				strBuff.append(xmlNode(childNodeName,nodeVal));
			}
			return xmlNode(nodeName, strBuff.toString(),null);
		}
		else{
			return "";
		}
		
	}
	private String xmlNode(String nodeName, String nodeVal,String defaultNS) {
		if (nodeVal != null) {
			StringBuffer strBuff = new StringBuffer();
			strBuff.append(LT).append(nodeName);
			if(defaultNS!=null){
				strBuff.append(" xmlns='").append(defaultNS).append("' ");
				
			}
			
			strBuff.append(GT).append(nodeVal).append("</").append(nodeName).append(GT);
			return strBuff.toString();
		} else {
			return "";
		}

	}

	private String xmlNode(String nodeName, Map<String, String> nodeVal) {
		if(nodeVal !=null && nodeVal.size()>0){
			StringBuffer strBuff = new StringBuffer();
			for (String key : nodeVal.keySet()) {
				strBuff.append(xmlNode(key, nodeVal.get(key)));
			}
			return xmlNode(nodeName, strBuff.toString());
		}
		else return "";
	}

	private String xmlNodeRanks(String nodeName, Map<String, Long> nodeVal) {
		if(nodeVal!=null && nodeVal.size()>0){
			StringBuffer strBuff = new StringBuffer();
			TreeSet<Long> ranks = new TreeSet<Long>();
			ranks.addAll(nodeVal.values());
			Map<Integer, String> rankToXML = new HashMap<Integer, String>();
			int numUris = nodeVal.keySet().size();
			for (String key : nodeVal.keySet()) {
				StringBuffer strBuff2 = new StringBuffer();
				Long time = nodeVal.get(key);
				Integer rank = numUris - ranks.headSet(time).size();
				String urisWithSameRank = rankToXML.get(rank);
				if (urisWithSameRank != null) {
					strBuff2.append(urisWithSameRank).append(xmlNode(URI, key));
				} else {
					strBuff2.append(xmlNode(URI, key)).append(xmlNode("rank", "" + rank))
							.append(xmlNode("timeInMillis", (time) + ""));
				}

				rankToXML.put(rank, strBuff2.toString());
			}
			for (Integer key : rankToXML.keySet()) {
				strBuff.append(xmlNode("Uri", rankToXML.get(key)));
			}
			return xmlNode(nodeName, strBuff.toString());	
		}
		else return "";
		
	}

	/**
	 * @param failedUris the failedUris to set
	 */
	public void setFailedUris(List<String> failedUris) {
		this.failedUris = failedUris;
	}

	/**
	 * @return the failedUris
	 */
	public List<String> getFailedUris() {
		return failedUris;
	}

	/**
	 * @return the preBatchRunTime
	 */
	public Long getPreBatchRunTime() {
		return preBatchRunTime;
	}

	/**
	 * @param preBatchRunTime the preBatchRunTime to set
	 */
	public void setPreBatchRunTime(Long preBatchRunTime) {
		this.preBatchRunTime = preBatchRunTime;
	}

	/**
	 * @return the postBatchRunTime
	 */
	public Long getPostBatchRunTime() {
		return postBatchRunTime;
	}

	/**
	 * @param postBatchRunTime the postBatchRunTime to set
	 */
	public void setPostBatchRunTime(Long postBatchRunTime) {
		this.postBatchRunTime = postBatchRunTime;
	}

	/**
	 * @return the initTaskRunTime
	 */
	public Long getInitTaskRunTime() {
		return initTaskRunTime;
	}

	/**
	 * @param initTaskRunTime the initTaskRunTime to set
	 */
	public void setInitTaskRunTime(Long initTaskRunTime) {
		this.initTaskRunTime = initTaskRunTime;
	}

}