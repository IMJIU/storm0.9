package com.book1.t04;

import java.util.Arrays;

import com.book1.t04.EWMA.Time;
import com.book1.t04.filter.BooleanFilter;
import com.book1.t04.func.JsonProjectFunction;
import com.book1.t04.func.MovingAverageFunction;
import com.book1.t04.func.ThresholdFilterFunction;
import com.book1.t04.func.XMPPFunction;
import com.book1.t04.msg.NotifyMessageMapper;

import backtype.storm.tuple.Fields;
import kafka.server.KafkaConfig;
import storm.kafka.StaticHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TSpout {
	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		StaticHosts kafkaHosts =  KafkaConfig.StaticHosts.fromHostString(Arrays.asList(new String[]{"localhost"}));
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, "log-analysis");
		spoutConf.scheme = new StringSchema();
		spoutConf.forceStartOffsetTime(-1);
		
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
