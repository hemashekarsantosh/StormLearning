package com.santosh.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.santosh.storm.bolts.WordCountBolt;
import com.santosh.storm.bolts.WordSplitBolt;
import com.santosh.storm.spout.LineReaderSpout;

public class HelloWorldStorm {

	public static void main(String[] args) {
		
		String inputFile="D:/test.log";
		
		Config config=new Config();
		config.put("inputFile", inputFile);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.setDebug(true);
		
		TopologyBuilder topology=new TopologyBuilder();
		topology.setSpout("line-reader-spout", new LineReaderSpout());
		topology.setBolt("word-split-bolt", new WordSplitBolt()).shuffleGrouping("line-reader-spout");
		topology.setBolt("word-count-bolt", new WordCountBolt()).shuffleGrouping("word-split-bolt");
		
		try {
			StormSubmitter.submitTopology("HelloStorm", config, topology.createTopology());
		} catch (AlreadyAliveException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvalidTopologyException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (AuthorizationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		/*LocalCluster cluster=new LocalCluster();
		cluster.submitTopology("HelloStorm", config, topology.createTopology());
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();*/

	}

}
