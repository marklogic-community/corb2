package com.marklogic.developer.corb;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class JobStats {
    @Override
	public String toString() {
		return "JobLocation:"+this.jobRunLocation+"\n"
				+ "User Provided Options :\n" + userProvidedOptions + "\n Start Time :" + startTime + "\n End Time :"
				+ endTime + "\n Host : " + host + "\n Total Number Of Tasks : " + totalNumberOfTasks
				+ "\n Average Transaction Time :" + averageTransactionTime + "\n URIs Load Time :" + urisLoadTime
				+ "\n Top Time Taking Uris : " + topTimeTakingUris + "\n";
	}
    public String toXMLString() {
    	StringBuffer strBuff=new StringBuffer();
    	strBuff.append(xmlNode("JobLocation",this.jobRunLocation))
    	.append(xmlNode("JobName",this.jobName))
    	.append(xmlNode("UserProvidedOptions" , userProvidedOptions))
    	.append(xmlNode("StartTime" , startTime))
    	.append(xmlNode("EndTime",endTime))
    	.append(xmlNode("Host" , host))
    	.append(xmlNode("TotalNumberOfTasks" , totalNumberOfTasks.toString()))
    	.append(xmlNode("AverageTransactionTime" , averageTransactionTime.toString()))
    	.append(xmlNode("URIsLoadTime" , urisLoadTime.toString()))
    	.append(xmlNodeRanks("LongRunningUris" , topTimeTakingUris));
		return xmlNode("CoRBJob",strBuff.toString());
    	
	}
    private String xmlNode(String nodeName,String nodeVal){
    	if(nodeVal !=null){
    		StringBuffer strBuff=new StringBuffer();
    		strBuff.append("<").append(nodeName).append(">").append(nodeVal).append("</").append(nodeName).append(">");
        	return strBuff.toString();
    	}
    	else{
    		return "";
    	}
    	
    }
    private String xmlNode(String nodeName,Map<String, String> nodeVal){
    	StringBuffer strBuff=new StringBuffer();
    	for( String key: nodeVal.keySet()){
    		strBuff.append(xmlNode(key,nodeVal.get(key)));	
    	}
    	return xmlNode(nodeName,strBuff.toString());
    }
    private String xmlNodeRanks(String nodeName,Map<String, Long> nodeVal){
    	StringBuffer strBuff=new StringBuffer();
    	TreeSet<Long> ranks= new TreeSet<Long>();
    	ranks.addAll(nodeVal.values());
    	Map<Integer,String> rankToXML=new HashMap<Integer,String>(); 
    	int numUris=nodeVal.keySet().size();
    	System.out.println("---------------------NUM URIS---------------------"+numUris);
    	for( String key: nodeVal.keySet()){
    		StringBuffer strBuff2=new StringBuffer();
    		Long time=nodeVal.get(key);
    		Integer rank=numUris-ranks.headSet(time).size();
    		String urisWithSameRank=rankToXML.get(rank);
    		if(urisWithSameRank !=null){
    			strBuff2.append(urisWithSameRank).append(xmlNode("uri",key));
    		}
    		else{
    			strBuff2.append(xmlNode("uri",key)).append(xmlNode("rank",""+rank)).append(xmlNode("timeInMillis",(time)+""));
    		}
    		
    		rankToXML.put(rank, strBuff2.toString());
    	}
    	for( Integer key: rankToXML.keySet()){
    		strBuff.append(xmlNode("Uri",rankToXML.get(key)));
    	}
    	return xmlNode(nodeName,strBuff.toString());
    }
	protected Map<String, String> userProvidedOptions = new HashMap<String, String>();
    protected String startTime = null;
    protected String endTime = null;
    protected String host = null;
    protected Long totalNumberOfTasks = null;
    protected Long numberOfFailedTasks = null;
    protected Double averageTransactionTime = null;
    protected Long urisLoadTime = null;
    protected String jobRunLocation = null;
    protected String jobName = null;
    
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
	protected Map<String, Long> topTimeTakingUris = new HashMap<String, Long>();
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
		return topTimeTakingUris;
	}
	public void setTopTimeTakingUris(Map<String, Long> topTimeTakingUris) {
		this.topTimeTakingUris = topTimeTakingUris;
	}
}