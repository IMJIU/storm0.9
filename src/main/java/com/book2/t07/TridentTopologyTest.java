package com.book2.t07;

import java.net.InetSocketAddress;

import org.apache.tinkerpop.shaded.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t02_trident.operator.DispatchAlert;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TridentTopologyTest {
	private static Logger log = LoggerFactory.getLogger(TridentTopologyTest.class);

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("track"), 2, new Values("101 item.com/item/200001 fav"),
				new Values("102 xxx.com/item/200002 addcart"), new Values("102 xxx.com/item/200002 addcart"));
		spout.setCycle(true);

		StormTopology t = getTopology2(spout);
		Config conf = new Config();
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("t01", conf, t);
		} else {
			conf.setNumAckers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, t);
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
		Utils.sleep(200000);
	}

	private static StormTopology getTopology(FixedBatchSpout spout) {
		TridentTopology topology = new TridentTopology();
		StateFactory memState = new MemoryMapState.Factory();
		// StateFactory dbstate = RedisState.transactional(new
		// InetSocketAddress("192.168.xxx",6379));
		topology.newStream("userstat", spout).shuffle()
				.each(new Fields("track"), new TrackSplit(), new Fields("userId","url","btnPosition"))
				.groupBy(new Fields("userId"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("agg_users"))
				.parallelismHint(2).newValuesStream().each(new Fields("userId", "agg_users"), new BaseFunction() {
					public void execute(TridentTuple tuple, TridentCollector collector) {
						log.info("{}-{}", tuple.get(0).toString(), tuple.get(1));
					}
				}, new Fields());

		StormTopology t = topology.build();
		return t;
	}
	
	private static StormTopology getTopology2(FixedBatchSpout spout) {
		TridentTopology topology = new TridentTopology();
		StateFactory memState = new MemoryMapState.Factory();
		// StateFactory dbstate = RedisState.transactional(new
		// InetSocketAddress("192.168.xxx",6379));
		topology.newStream("userstat", spout).shuffle()
				.each(new Fields("track"), new TrackSplit(), new Fields("userId","url","btnPosition"))
				.groupBy(new Fields("userId"))
				.aggregate(new One(), new Fields("one"))
				.aggregate(new Fields("one"), new StateCount(),new Fields("reach"))
				.each(new Fields( "reach"), new BaseFunction() {
					public void execute(TridentTuple tuple, TridentCollector collector) {
						log.info("{}", tuple.get(0));
					}
				}, new Fields());

		StormTopology t = topology.build();
		return t;
	}
}

class TrackSplit extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Log.debug("tuple:{}", tuple.getString(0));
		String sentence = (String) tuple.getValue(0);
		if (sentence != null) {
			String[] items = (sentence + "\n").split(" ");
			String userId = items[0];
			String url = items[1];
			String buttonPosition = items[2];
			collector.emit(new Values(userId, url, buttonPosition));
		}
	}

}