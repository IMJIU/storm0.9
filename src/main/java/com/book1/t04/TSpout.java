package com.book1.t04;

import java.util.Arrays;

import com.book1.t04.EWMA.Time;
import com.book1.t04.filter.BooleanFilter;
import com.book1.t04.func.JsonProjectFunction;
import com.book1.t04.func.MovingAverageFunction;
import com.book1.t04.func.ThresholdFilterFunction;
import com.book1.t04.func.XMPPFunction;
import com.book1.t04.msg.NotifyMessageMapper;
import com.google.common.collect.ImmutableList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import kafka.server.KafkaConfig;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TSpout {
	public static void main(String[] args) {
		 buildTopology();
		Config conf = new Config();
		conf.put(XMPPFunction.XMPP_USER, "storm@dud.local");
		conf.put(XMPPFunction.XMPP_PASSWORD, "storm@dud.local");
		conf.put(XMPPFunction.XMPP_SERVER, "sdud.local");
		conf.put(XMPPFunction.XMPP_TO, "storm2@dud.local");
		conf.setMaxSpoutPending(5);
		if(args.length ==0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("log-analysis", conf, buildTopology());;
		}else{
			conf.setNumAckers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, buildTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
	}

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
//		StaticHosts kafkaHosts =  storm.kafka.KafkaConfig.StaticHosts.fromHostString(Arrays.asList(new String[]{"localhost"}));
		
		GlobalPartitionInformation info = new GlobalPartitionInformation();  
		info.addPartition(0, new Broker("10.1.110.24",9092));  
		info.addPartition(0, new Broker("10.1.110.21",9092));  
		StaticHosts kafkaHosts = new StaticHosts(info);  
		
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, "log-analysis");
		spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
		spoutConf.forceFromStart= true;
		spoutConf.startOffsetTime = -1;
//		
//		String kafkaZookeeper = "192.168.199.210:2181";
//		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
//		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "order", "/order", "id");
//		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//		kafkaConfig.zkServers =  ImmutableList.of("x00","x01","x02");
//		kafkaConfig.zkPort = 2181;
		
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		
		Stream spoutStream = topology.newStream("kafka-stream", spout);
		
		Fields jsonFields = new Fields("level","timestamp","message","logger");
		Stream parsedStream = spoutStream.each(new Fields("str"), new JsonProjectFunction(jsonFields),jsonFields);
		
		parsedStream = parsedStream.project(jsonFields);
		
		EWMA ewma = new EWMA().sliding(1.0, Time.MINUTES).withAlpha(EWMA.ONE_MINUTE_ALPHA);
		Stream averageStream = parsedStream.each(new Fields("timestamp"), new MovingAverageFunction(ewma, Time.MINUTES),new Fields("average"));
		
		ThresholdFilterFunction tff = new ThresholdFilterFunction(50d);
		Stream thresholdStream = averageStream.each(new Fields("average"), tff,new Fields("change","threshold"));
		Stream filteredStream = thresholdStream.each(new Fields("change"), new BooleanFilter());
		
		filteredStream.each(filteredStream.getOutputFields(), new XMPPFunction(new NotifyMessageMapper()),new Fields());
		return topology.build();
	}
}
