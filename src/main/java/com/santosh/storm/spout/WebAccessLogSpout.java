package com.santosh.storm.spout;

import java.net.MalformedURLException;



import org.apache.solr.client.solrj.impl.HttpSolrClient;
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
	
	public static HttpSolrClient solrServer;
	public static String zkHost="localhost";
	public static String zkPort="2181";
	public static String solrUrl="http://localhost:8983/solr/";
	public static String solrCollection="mostactive";

	
	public static void main(String[] args) throws MalformedURLException {
		
		BrokerHosts kafkaHost=new ZkHosts(zkHost+":"+zkPort);
		SpoutConfig spoutConfig=new SpoutConfig(kafkaHost, "testing", "/kafkastorm","kafkaSpoutTest");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		Config config=new Config();
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.setDebug(true);
		
		solrServer = new HttpSolrClient.Builder(solrUrl+solrCollection).build();
		
		TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout("srt_webaccesslog-kafka-spout", kafkaSpout);
		topology.setBolt("srt_webaccesslog-split-bolt", new AccessLogSplitBolt()).shuffleGrouping("srt_webaccesslog-kafka-spout");
		topology.setBolt("srt_webaccesslog-count-bolt", new RequestCountBolt()).shuffleGrouping("srt_webaccesslog-split-bolt","stream1");
		topology.setBolt("srt_webaccesslog-uniqueuser-bolt", new UniqueUserCountBolt()).shuffleGrouping("srt_webaccesslog-split-bolt","stream2");

		
		LocalCluster cluster=new LocalCluster();
		cluster.submitTopology("kafkaSpoutTest", config, topology.createTopology());
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();
	}

}
