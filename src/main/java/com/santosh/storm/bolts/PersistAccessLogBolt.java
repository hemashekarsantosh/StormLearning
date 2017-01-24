package com.santosh.storm.bolts;

import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.santosh.storm.databean.AccessLogDataBean;
import com.santosh.storm.spout.WebAccessLogSpout;

public class PersistAccessLogBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		AccessLogDataBean bean=(AccessLogDataBean)input.getValue(0);
		try {
		SolrInputDocument doc = new SolrInputDocument();
        doc.addField("appName", bean.getAppName());
        doc.addField("remoteHost", bean.getRemoteHost());
        doc.addField("date", bean.getDate());
        doc.addField("clientformatDate", bean.getClientformatDate());
        doc.addField("authuser", bean.getAuthuser());
        doc.addField("requesturl", bean.getRequesturl());
        doc.addField("requestStatus", bean.getRequestStatus());
        doc.addField("requestMethod", bean.getRequestMethod());
        doc.addField("completeMessage", bean.getCompleteMessage());
        WebAccessLogSpout.messagessolrServer.add(doc);
        WebAccessLogSpout.messagessolrServer.commit(); 
		}catch(Exception exp){
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
