package com.book1.idongri_log_analysis;

import java.util.Arrays;

import org.apache.kafka.common.metrics.stats.Avg;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t02_trident.operator.OutbreakDetector;
import com.book1.t02_trident.state.OutbreakTrendFactory;
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
import storm.trident.operation.builtin.Count;

public class KafkaTopology {
	private static final Logger log = LoggerFactory.getLogger(KafkaTopology.class);

	public static void main(String[] args) {
		buildTopology();
		Config conf = new Config();
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

		GlobalPartitionInformation info = new GlobalPartitionInformation();
		info.addPartition(0, new Broker("192.168.199.210", 9092));
		// info.addPartition(0, new Broker("10.1.110.21", 9092));
		StaticHosts kafkaHosts = new StaticHosts(info);

		TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, "storm2");
		spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new StringKeyValueScheme());
		spoutConf.forceFromStart = true;
		spoutConf.startOffsetTime = -2;// -1最后一条记录开始 -2.从新开始

		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

		Stream spoutStream = topology.newStream("kafka-stream", spout);
		Fields f1 = new Fields("method", "time");
		spoutStream.each(new Fields("str"), new SplitFunction(),f1)
//		.each(f1, new AvgFunction(),new Fields("avg"))
		.groupBy(new Fields("method"))
		.persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
		.newValuesStream()
        .each(new Fields("method","count"), new CheckFunction(), new Fields("alert"));
	
		return topology.build();
	}
}
