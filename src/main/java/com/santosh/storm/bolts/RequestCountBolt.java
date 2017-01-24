package com.santosh.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.santosh.storm.databean.AccessLogDataBean;
import com.santosh.storm.spout.WebAccessLogSpout;

public class RequestCountBolt implements IRichBolt{

	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	
	public void cleanup() {
		
	}

	public void execute(Tuple input) {
		AccessLogDataBean bean=(AccessLogDataBean)input.getValue(0);
		String actionURL=bean.getRequesturl();
		if(actionURL.contains("?"))
		actionURL=actionURL.substring(0,actionURL.indexOf("?"));
		SolrQuery solquery = new SolrQuery();
		String query="appName:"+bean.getAppName() +"AND actionURL:"+"\""+actionURL+"\"";
		System.out.println(query);
		solquery.set("q",query);   
		solquery.set("defType", "edismax");
	    
	    QueryResponse response;
		try {
			response = WebAccessLogSpout.accesslogsolrServer.query(solquery);
			 SolrDocumentList results = response.getResults();
			    if(results.size()>0){
			    	//update the existing Count
			    	System.out.println("**********Partial Update*****************");
			    	SolrInputDocument doc = new SolrInputDocument();
			    	Map<String, String> partialUpdate = new HashMap<String, String>();
			    	partialUpdate.put("inc", "1");
			    	doc.addField("actionURL",actionURL);
			    	doc.addField("count", partialUpdate);
			    	WebAccessLogSpout.accesslogsolrServer.add(doc);
			    }else{
			    	// add new document
			    	System.out.println("**********New Update*****************");
			    	SolrInputDocument doc = new SolrInputDocument();
			        doc.addField("appName",bean.getAppName());
			        doc.addField("actionURL", actionURL);
			        doc.addField("count", 1);
			        WebAccessLogSpout.accesslogsolrServer.add(doc);
			    }
			    WebAccessLogSpout.accesslogsolrServer.commit(); 
		} catch (Exception exp) {
			
			exp.printStackTrace();
		}
	   
		
		
	}
	
	

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
