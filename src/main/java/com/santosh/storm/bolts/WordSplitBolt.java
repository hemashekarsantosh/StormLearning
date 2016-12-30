package com.santosh.storm.bolts;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordSplitBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple inputline) {
		String line=inputline.getStringByField("line");
		String[] words=line.split(" ");
		for(String word:words){
			word=word.trim();
			if(!word.isEmpty()){
				word=word.toLowerCase();
				System.out.println("Word::"+word);
				collector.emit(new Values(word));
			}
		}
		collector.ack(inputline);
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
