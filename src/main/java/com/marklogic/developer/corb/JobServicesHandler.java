package com.marklogic.developer.corb;

import java.io.IOException;
import java.util.Map;

import com.marklogic.developer.corb.HTTPServer.Request;
import com.marklogic.developer.corb.HTTPServer.Response;

public class JobServicesHandler implements HTTPServer.ContextHandler{
	Manager manager;
	JobServicesHandler(Manager manager){
		this.manager=manager;
	}
	public int serve(Request req, Response resp) throws IOException {
		Map<String,String> params=req.getParams();
		if (params.containsKey("paused") || params.containsKey("PAUSED")) {
			String value = params.get("paused");
			value=value==null?params.get("PAUSED"):value;
			if(value !=null && value.equalsIgnoreCase("true")){
				manager.pause();
			}
			else if(value !=null && value.equalsIgnoreCase("false")){
				manager.resume();
			}		
		}
		if (params.containsKey("threads") || params.containsKey("THREADS")) {
			String value = params.get("threads");
			value=value==null?params.get("THREADS"):value;
			if(value !=null ){
				try {
					int threadCount = Integer.parseInt(value);
					if(threadCount>0){
						manager.setThreadCount(threadCount);
					}
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}
			}
				
		}
		manager.jobStats.setPaused(String.valueOf(manager.isPaused()));		
		boolean concise=params.containsKey("concise")||params.containsKey("CONCISE");
		if (params.containsKey("xml") || params.containsKey("XML")) {
			resp.getHeaders().add("Content-Type", "application/xml");
			resp.send(200, manager.jobStats.toXMLString(concise));
		}
		else {
			resp.getHeaders().add("Content-Type", "application/json");
			resp.send(200, manager.jobStats.toJSONString(concise));
		}  
		return 0;
	}
}
