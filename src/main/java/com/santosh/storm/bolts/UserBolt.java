package com.santosh.storm.bolts;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class UserBolt implements IRichBolt{

	private OutputCollector collector;
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple inputRec) {
		String user=inputRec.getString(0);
		System.out.println("*************** "+user+ " *******************");
		List<String> userData=Arrays.asList(user.split(","));
		System.out.println("*************** "+userData.size()+ " *******************");
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
