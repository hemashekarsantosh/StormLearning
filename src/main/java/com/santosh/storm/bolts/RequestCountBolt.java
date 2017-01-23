package com.santosh.storm.bolts;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
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
	private Map<String,Integer> requestCount;
	
	public void cleanup() {
		System.out.println("****************************** Request Count ****************************");
		for(Map.Entry<String,Integer> map:requestCount.entrySet()){
			System.out.println("Request URL - "+map.getKey()+ " Hits: "+map.getValue());
		}
		System.out.println("****************************** Request Count ****************************");
		
	}

	public void execute(Tuple input) {
		AccessLogDataBean bean=(AccessLogDataBean)input.getValue(0);
		String actionURL=bean.getRequesturl();
		/*if(!requestCount.containsKey(url)){
			requestCount.put(url, 1);
		}else{
			Integer newValue=requestCount.get(url)+1;
			requestCount.put(url, newValue);
		}*/
		SolrQuery solquery = new SolrQuery();
		String query="actionURL:"+"\""+actionURL+"\"";
		System.out.println(query);
		solquery.set("q",query);
	    //query.addFilterQuery("actionURL:"+actionURL);
	    //query.setFields("actionURL","count");
	    //query.setStart(0);    
		solquery.set("defType", "edismax");
	    
	    QueryResponse response;
		try {
			response = WebAccessLogSpout.solrServer.query(solquery);
			 SolrDocumentList results = response.getResults();
			    if(results.size()>0){
			    	//update the existing Count
			    	System.out.println("**********Partial Update*****************");
			    	SolrInputDocument doc = new SolrInputDocument();
			    	Map<String, String> partialUpdate = new HashMap<String, String>();
			    	partialUpdate.put("inc", "1");
			    	doc.addField("actionURL",actionURL);
			    	doc.addField("count", partialUpdate);
			    	WebAccessLogSpout.solrServer.deleteById(actionURL);
			    }else{
			    	// add new document
			    	System.out.println("**********New Update*****************");
			    	SolrInputDocument doc = new SolrInputDocument();
			        doc.addField("appName", "SRT");
			        doc.addField("actionURL", actionURL);
			        doc.addField("count", 1);
			        WebAccessLogSpout.solrServer.add(doc);
			    }
			    WebAccessLogSpout.solrServer.commit(); 
		} catch (Exception exp) {
			
			exp.printStackTrace();
		}
	   
		
		
	}
	
	

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.requestCount=new HashMap<String, Integer>();
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
