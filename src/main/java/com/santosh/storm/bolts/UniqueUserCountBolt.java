package com.santosh.storm.bolts;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class UniqueUserCountBolt implements IRichBolt{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Set<String> uniqueUser;
	
	public void cleanup() {
		System.out.println("******************************Unique User List************************");
		for(String set:uniqueUser){
			System.out.println(""+set.toString());
		}
		System.out.println("******************************Unique User List************************");		
	}

	public void execute(Tuple input) {
		String authUser=input.getStringByField("authuser");
		if(!uniqueUser.contains(authUser)){
			uniqueUser.add(authUser);
		}
		
	}

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		
		this.collector=collector;
		this.uniqueUser=new HashSet<String>();
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
