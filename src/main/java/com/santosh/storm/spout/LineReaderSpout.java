package com.santosh.storm.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class LineReaderSpout implements IRichSpout {

	
	private static final long serialVersionUID = 1L;
	private FileReader filereader;
	private TopologyContext context;
	private SpoutOutputCollector collector;
	
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		String line=null;
		BufferedReader bufferReader=new BufferedReader(filereader);
		try{
			while(null != (line = bufferReader.readLine())){
				System.out.println("Line::"+line);
				this.collector.emit(new Values(line),line);
			}
		}catch(Exception exp){
			exp.printStackTrace();
		}
		
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		try{
			this.context=context;
			this.filereader=new FileReader(conf.get("inputFile").toString());
			this.collector=collector;
		}catch(FileNotFoundException fileNotExp){
			fileNotExp.printStackTrace();
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
