package com.santosh.storm.bolts;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.santosh.storm.databean.AccessLogDataBean;

public class AccessLogSplitBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple inputline) {
		String line=inputline.getString(0);
		String date="";
		//System.out.println("***********************LINE::: "+line);
		String[] words=line.split(" ");
		if(words.length>0 && !line.isEmpty() && words.length>=8){
			AccessLogDataBean bean=new AccessLogDataBean();
			bean.setCompleteMessage(line);
			bean.setAppName("SRT");
			bean.setRemoteHost(!words[0].isEmpty() ? words[0].toLowerCase().trim() : "NA");
			bean.setAuthuser(!words[2].isEmpty() ? words[2].toLowerCase().trim() : "NA");
			date=words[3]+words[4];
			date=date.substring(date.indexOf("[")+1,date.indexOf("]"));
			bean.setDate(date);
			SimpleDateFormat standardFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ssZ");
			try {
				Date clientDate=standardFormat.parse(date);
				bean.setClientformatDate(standardFormat.format(clientDate));
			} catch (ParseException e) {
				
				e.printStackTrace();
			}
			bean.setRequestMethod(!words[5].isEmpty() ? words[5].toUpperCase().substring(words[5].indexOf("\"")+1, words[5].length()) : "NA");
			bean.setRequesturl(!words[6].isEmpty() ? words[6].trim() : "NA");
			bean.setRequestStatus(!words[8].isEmpty() ? words[8].toLowerCase().trim() : "NA");
			if(bean.getRequesturl().contains("eBPM")){
				bean.setAppName("eBPM");
			}
			collector.emit("stream1",new Values(bean));
			//collector.emit("stream2",new Values(bean.getRemoteHost()));
			
			
			
		
		}
		collector.ack(inputline);
		
	}

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stream1",new Fields("bean"));
		//declarer.declareStream("stream2",new Fields("authuser"));
		
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
