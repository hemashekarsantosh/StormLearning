package com.santosh.storm.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WordCountBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Map<String,Integer> wordCounter;
	
	public void cleanup() {
		for(Map.Entry<String,Integer> map:wordCounter.entrySet()){
			System.out.println(map.getKey()+" : " + map.getValue());
		}
		
	}

	public void execute(Tuple inputWord) {
		String word=inputWord.getStringByField("word");
		if(!wordCounter.containsKey(word)){
			wordCounter.put(word, 1);
		}else{
			Integer newValue=wordCounter.get(word)+1;
			wordCounter.put(word, newValue);
		}
		this.collector.ack(inputWord);
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		this.wordCounter=new HashMap<String,Integer>();
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
