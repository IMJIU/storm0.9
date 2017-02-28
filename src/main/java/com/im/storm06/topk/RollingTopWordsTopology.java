package com.im.storm06.topk;

import java.io.Serializable;
import java.util.Map;

import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.tools.SlotBasedCounter;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RollingTopWordsTopology {

	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

	private static final int TOP_N = 5;

	private final TopologyBuilder builder;

	private final String topologyName;

	private final Config topologyConfig;

	private final int runtimeInSeconds;

	public RollingTopWordsTopology() throws Exception {
		builder = new TopologyBuilder();
		topologyName = "slidingWindowCounts";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}

	private void wireTopology() {
		String spoutId = "wordGenerator";
		String counterId = "counter";
		String intermediateRankerId = "intermediaRanker";
		String totalRankerId = "finalRanker";
		builder.setSpout(spoutId, new TestWordSpout(), 5);
		builder.setBolt(counterId, new RollingCountBolt(9, 3), 4).fieldsGrouping(spoutId, new Fields("word"));
		builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(counterId, new Fields("obj"));
		builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
	}

	public static void main(String[] args) throws Exception {
		new RollingTopWordsTopology().run();
	}

	private void run() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);

	}
}
