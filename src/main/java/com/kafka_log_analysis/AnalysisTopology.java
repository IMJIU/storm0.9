package com.kafka_log_analysis;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.kafka.Broker;
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.StaticHosts;
import storm.kafka.StringKeyValueScheme;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.spout.ITridentSpout.BatchCoordinator;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.tuple.TridentTuple;

public class AnalysisTopology {
	
	public static void main(String[] args) {
		Config conf = new Config();
		TridentTopology topology = new TridentTopology();
		GlobalPartitionInformation info = new GlobalPartitionInformation();
		info.addPartition(0, new Broker("192.168.72.128", 9092));
		StaticHosts kafkaHosts = new StaticHosts(info);

		TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, "log");
		spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
		spoutConf.forceFromStart = true;
		spoutConf.startOffsetTime = -2;//-1最后一条记录开始  -2.从新开始
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		Stream spoutStream = topology.newStream("kafka-stream", spout);
		spoutStream.each(new Fields("str"), new BaseFunction() {
			
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				String str = tuple.getString(0);
				System.out.println(str);
				if(str.indexOf("|@@|")>0){
					
				}
			}
		},new Fields(""));
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("log-analysis", conf, topology.build());
		} else {
			conf.setNumAckers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, topology.build());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
		Utils.sleep(200000);
	}

}
