package com.kafka_log_analysis;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
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
import storm.trident.tuple.TridentTuple;

public class AnalysisTopology {
	public static final String brokerHost = "192.168.199.210";
	public static final String token = "|@@|";
	public static final String tokenEscap = "\\|@@\\|";
	
	public static final DateFormat df = new SimpleDateFormat("yyyyMMdd");

	private static LogStatistic logStatistic = new LogStatistic();
	// public static String broker="192.168.72.128";
	public static void main(String[] args) {
		Config conf = new Config();
		TridentTopology topology = new TridentTopology();
		GlobalPartitionInformation info = new GlobalPartitionInformation();
		info.addPartition(0, new Broker(brokerHost, 9092));
		StaticHosts kafkaHosts = new StaticHosts(info);

		TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, "log");
		spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
		spoutConf.forceFromStart = true;
		spoutConf.startOffsetTime = -2;// -1最后一条记录开始 -2.从新开始
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		Stream spoutStream = topology.newStream("kafka-stream", spout);
		spoutStream.each(new Fields("str"), new BaseFunction() {

			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				String str = tuple.getString(0);
				System.out.println(str);
				if (str.indexOf(token) > 0) {
					String[] arr = str.split(tokenEscap);
					String ip = arr[0];
					String method = arr[3];
					String date = arr[1].substring(0, 10).replaceAll("-", "");
					String hour = arr[1].substring(11, 13);
					Values values = new Values();
					values.add(ip);
					values.add(method);
					values.add(date);
					values.add(hour);
					collector.emit(values);
				}
			}
		}, new Fields("ip","method","date","hour"))
		.each( new Fields("ip","method","date","hour"), new  BaseFunction() {
			
			@Override
			public void execute(TridentTuple tuple, TridentCollector collector) {
				String ip = tuple.getString(0);
				String method = tuple.getString(1);
				String date = tuple.getString(2);
				String hour = tuple.getString(3);
				logStatistic.add(ip,method,date,hour);
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
