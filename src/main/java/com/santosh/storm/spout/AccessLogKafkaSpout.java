package com.santosh.storm.spout;



import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
/*import org.apache.storm.solr.bolt.SolrUpdateBolt;
import org.apache.storm.solr.config.CountBasedCommit;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.mapper.SolrFieldsMapper;
import org.apache.storm.solr.mapper.SolrMapper;
import org.apache.storm.solr.schema.builder.RestJsonSchemaBuilder;
import org.apache.storm.solr.schema.builder.SchemaBuilder;*/
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import com.santosh.storm.bolts.AccessLogSplitBolt;
import com.santosh.storm.bolts.RequestCountBolt;
import com.santosh.storm.bolts.UniqueUserCountBolt;

public class AccessLogKafkaSpout {

	public static void main(String[] args) throws Exception {
		
	
		BrokerHosts kafkaHost=new ZkHosts("localhost:2181");
		SpoutConfig spoutConfig=new SpoutConfig(kafkaHost, "testing", "/kafkastorm","kafkaSpoutTest");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 
		//spoutConfig.startOffsetTime(1);
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		Config config=new Config();
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.setDebug(true);
		
		  // zkHostString for Solr 'gettingstarted' example
	   /* SolrConfig solrConfig = new SolrConfig("127.0.0.1:2181");
	 // builds the Schema object  
	    SchemaBuilder schemaBuilder = new RestJsonSchemaBuilder("localhost", "8983", "testingnew");
	    SolrMapper solrMapper = new SolrFieldsMapper.Builder(schemaBuilder, "testingnew").build();
	 // Acks every other five tuples. Setting to null acks every tuple
	    SolrCommitStrategy solrCommitStgy = new CountBasedCommit(5);  */
		
		TopologyBuilder topology = new TopologyBuilder();
		topology.setSpout("accesslog-kafka-spout", kafkaSpout);
		topology.setBolt("accesslog-split-bolt", new AccessLogSplitBolt(),2).setNumTasks(4).setMaxTaskParallelism(2).shuffleGrouping("accesslog-kafka-spout");
		topology.setBolt("request-count-bolt", new RequestCountBolt()).shuffleGrouping("accesslog-split-bolt","stream1");
		topology.setBolt("user-count-bolt", new UniqueUserCountBolt()).shuffleGrouping("accesslog-split-bolt","stream2");
		/*topology.setBolt("solr-access-log-bolt", new SolrUpdateBolt(solrConfig, solrMapper, solrCommitStgy)).shuffleGrouping("accesslog-split-bolt","stream3");*/
		
		LocalCluster cluster=new LocalCluster();
		cluster.submitTopology("KafkaStormTest", config, topology.createTopology());
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();

	}

}
