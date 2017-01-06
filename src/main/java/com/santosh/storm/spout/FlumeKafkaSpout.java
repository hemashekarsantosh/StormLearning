package com.santosh.storm.spout;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;


import com.santosh.storm.bolts.UserBolt;



public class FlumeKafkaSpout {

	public static void main(String[] args) {
		
		BrokerHosts kafkaHost=new ZkHosts("localhost:2181");
		SpoutConfig spoutUserConfig=new SpoutConfig(kafkaHost, "users", "/users","catusers");
		spoutUserConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 
		//spoutConfig.startOffsetTime(1);
		KafkaSpout kafkaUserSpout = new KafkaSpout(spoutUserConfig);
		
		SpoutConfig spoutRelUserRoleConfig=new SpoutConfig(kafkaHost, "reluserroles", "/reluserroles","relUserRoles");
		spoutRelUserRoleConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 
		//spoutConfig.startOffsetTime(1);
		KafkaSpout kafkaRelUserRoleSpout = new KafkaSpout(spoutRelUserRoleConfig);
		
		
		Config config=new Config();
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.setDebug(true);
		
		TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout("user-kafka-spout", kafkaUserSpout);
		topology.setSpout("reluserrole-kafka-spout", kafkaRelUserRoleSpout);
		topology.setBolt("user-bolt", new UserBolt()).shuffleGrouping("user-kafka-spout").shuffleGrouping("reluserrole-kafka-spout");
		//topology.setBolt("reluserrole-bolt", new UserBolt()).shuffleGrouping("reluserrole-kafka-spout");
		/*LocalCluster cluster=new LocalCluster();
		cluster.submitTopology("TeamHierachy", config, topology.createTopology());*/
		StormSubmitter submitter=new StormSubmitter();
		try {
			submitter.submitTopology("TeamHierachy", config, topology.createTopology());
			//Thread.sleep(100000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//cluster.shutdown();
	}

}
