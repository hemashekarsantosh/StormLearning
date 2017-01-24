package com.santosh.storm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
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

public class RegionMetricsBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override
	public void cleanup() {
		
		
	}

	@Override
	public void execute(Tuple input) {
		AccessLogDataBean bean=(AccessLogDataBean)input.getValue(0);
		InetAddress ip;
		QueryResponse response;
		try {
			ip = InetAddress.getByName(bean.getRemoteHost());
			String hostname = ip.getHostName();
	        String host=hostname.substring(0, hostname.indexOf(".")).toUpperCase();
	        String tmpRegion=((hostname.substring(hostname.indexOf(".")+1)).substring(0, hostname.indexOf(".")).toUpperCase());
	        String region=tmpRegion.substring(0,tmpRegion.indexOf("."));
	        if(region.equalsIgnoreCase("SAC")){
	        	region="AMERICAS";
	        }
	        SolrQuery solquery = new SolrQuery();
			String query="appName:"+bean.getAppName()+"AND region:"+region;
			System.out.println(query);
			solquery.set("q",query);   
			solquery.set("defType", "edismax");
			response = WebAccessLogSpout.regionmetricssolrServer.query(solquery);
			SolrDocumentList results = response.getResults();
			if(results.size()>0){
		    	//update the existing Count
		    	System.out.println("**********Partial Update*****************");
		    	SolrInputDocument doc = new SolrInputDocument();
		    	Map<String, String> partialUpdate = new HashMap<String, String>();
		    	partialUpdate.put("inc", "1");
		    	doc.addField("region",region);
		    	doc.addField("count", partialUpdate);
		    	WebAccessLogSpout.regionmetricssolrServer.add(doc);
		    }else{
		    	// add new document
		    	System.out.println("**********New Update*****************");
		    	SolrInputDocument doc = new SolrInputDocument();
		        doc.addField("appName",bean.getAppName());
		        doc.addField("region", region);
		        doc.addField("count", 1);
		        WebAccessLogSpout.regionmetricssolrServer.add(doc);
		    }
		    WebAccessLogSpout.regionmetricssolrServer.commit(); 
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		
		
	}

	@Override
public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		this.collector=collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
