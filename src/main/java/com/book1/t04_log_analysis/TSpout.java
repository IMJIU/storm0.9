package com.book1.t04_log_analysis;

import java.util.Arrays;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t04_log_analysis.EWMA.Time;
import com.book1.t04_log_analysis.filter.BooleanFilter;
import com.book1.t04_log_analysis.func.JsonProjectFunction;
import com.book1.t04_log_analysis.func.MovingAverageFunction;
import com.book1.t04_log_analysis.func.PrintFunction;
import com.book1.t04_log_analysis.func.ThresholdFilterFunction;
import com.book1.t04_log_analysis.func.XMPPFunction;
import com.book1.t04_log_analysis.msg.NotifyMessageMapper;
import com.google.common.collect.ImmutableList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import kafka.server.KafkaConfig;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TSpout {
	private static final Logger log = LoggerFactory.getLogger(TSpout.class);
	public static void main(String[] args) {
		buildTopology();
		Config conf = new Config();
		conf.put(XMPPFunction.XMPP_USER, "storm@dud.local");
		conf.put(XMPPFunction.XMPP_PASSWORD, "storm@dud.local");
		conf.put(XMPPFunction.XMPP_SERVER, "sdud.local");
		conf.put(XMPPFunction.XMPP_TO, "storm2@dud.local");
		conf.setMaxSpoutPending(5);

		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("log-analysis", conf, buildTopology());
		} else {
			conf.setNumAckers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, buildTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
		Utils.sleep(200000);
	}

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		// StaticHosts kafkaHosts =
		// storm.kafka.KafkaConfig.StaticHosts.fromHostString(Arrays.asList(new
		// String[]{"localhost"}));

		GlobalPartitionInformation info = new GlobalPartitionInformation();
		info.addPartition(0, new Broker("192.168.199.210", 9092));
		// info.addPartition(0, new Broker("10.1.110.21", 9092));
		StaticHosts kafkaHosts = new StaticHosts(info);

		TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, "mytopic");
		spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
		spoutConf.forceFromStart = true;
		spoutConf.startOffsetTime = -1;//-1最后一条记录开始  -2.从新开始
		//
		// String kafkaZookeeper = "192.168.199.210:2181";
		// BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
		// SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "order",
		// "/order", "id");
		// kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// kafkaConfig.zkServers = ImmutableList.of("x00","x01","x02");
		// kafkaConfig.zkPort = 2181;

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		Stream spoutStream = topology.newStream("kafka-stream", spout);

		Fields jsonFields = new Fields("level", "timestamp", "message", "logger");
		Stream parsedStream = spoutStream.each(new Fields("str"), new JsonProjectFunction(jsonFields), jsonFields);
		log.debug("parsedStream.....");
		parsedStream = parsedStream.project(jsonFields);
		log.debug("averageStream.....");
		EWMA ewma = new EWMA().sliding(1.0, Time.MINUTES).withAlpha(EWMA.ONE_MINUTE_ALPHA);
		Stream averageStream = parsedStream.each(new Fields("timestamp"), new MovingAverageFunction(ewma, Time.MINUTES),new Fields("average"));
		log.debug("thresholdStream.....");
		Stream thresholdStream = averageStream.each(new Fields("average"), new ThresholdFilterFunction(50d), new Fields("change", "threshold"));
		log.debug("filteredStream.....");
		Stream filteredStream = thresholdStream.each(new Fields("change"), new BooleanFilter());

		// filteredStream.each(filteredStream.getOutputFields(), new
		// XMPPFunction(new NotifyMessageMapper()),
		// new Fields());
//		thresholdStream.each(thresholdStream.getOutputFields(), new PrintFunction(new NotifyMessageMapper()),new Fields());
		log.debug("unkown.....");
		Stream unkown = filteredStream.each(filteredStream.getOutputFields(), new PrintFunction(new NotifyMessageMapper()),new Fields());
		log.debug("return...");
		return topology.build();
	}
}
