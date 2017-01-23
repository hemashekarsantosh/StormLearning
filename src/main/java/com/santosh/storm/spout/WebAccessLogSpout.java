package com.santosh.storm.spout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.santosh.storm.bolts.AccessLogSplitBolt;
import com.santosh.storm.bolts.RequestCountBolt;
import com.santosh.storm.bolts.UniqueUserCountBolt;

public class WebAccessLogSpout {

	public static void main(String[] args) {
		
		BrokerHosts kafkaHost=new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig=new SpoutConfig(kafkaHost, "srt_webaccesslog", "/srt_webaccesslog","srt_webaccesslog");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 
		//spoutConfig.startOffsetTime(1);
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		Config config=new Config();
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.setDebug(true);
		
		TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout("accesslog-kafka-spout", kafkaSpout);
		topology.setBolt("accesslog-split-bolt", new AccessLogSplitBolt(),2).setNumTasks(4).setMaxTaskParallelism(2).shuffleGrouping("accesslog-kafka-spout");
		topology.setBolt("request-count-bolt", new RequestCountBolt()).shuffleGrouping("accesslog-split-bolt","stream1");
		topology.setBolt("user-count-bolt", new UniqueUserCountBolt()).shuffleGrouping("accesslog-split-bolt","stream2");

		
		LocalCluster cluster=new LocalCluster();
		cluster.submitTopology("srt_webaccesslog", config, topology.createTopology());
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();
	}

}
