package com.santosh.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.santosh.storm.databean.AccessLogDataBean;

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
		String url=bean.getRequesturl();
		if(!requestCount.containsKey(url)){
			requestCount.put(url, 1);
		}else{
			Integer newValue=requestCount.get(url)+1;
			requestCount.put(url, newValue);
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
