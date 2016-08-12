package com.book2.t07;

import org.apache.tinkerpop.shaded.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
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
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;

public class DRPCTopologyTest {
	private static Logger log = LoggerFactory.getLogger(DRPCTopologyTest.class);

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("track"), 2, 
				new Values("101 item.com/item/200001 fav"),
				new Values("102 xxx.com/item/200002 addcart"), 
				new Values("102 xxx.com/item/200002 addcart"));
		spout.setCycle(true);

		StormTopology t = getTopology(spout);
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
		StateFactory dbState = new MemoryMapState.Factory();
		// StateFactory dbstate = RedisState.transactional(new
		// InetSocketAddress("192.168.xxx",6379));
		TridentState countState = topology.newStream("userstat", spout).shuffle()
				.each(new Fields("track"), new TrackSplit(), new Fields("userId","url","btnPosition"))
				.groupBy(new Fields("userId"))
				.aggregate(new Fields("userId","url","btnPosition"), new StateCount(), new Fields("count"))
				.partitionPersist(dbState, new Fields("userId","count"),null,new Fields("count2"));
		
		LocalDRPC drpc = new LocalDRPC();
		topology.newDRPCStream("reach",drpc)
		.each(new Fields("args"), new BaseFunction() {
			public void execute(TridentTuple tuple, TridentCollector collector) {
				log.info(tuple.getString(0));
			}
		},new Fields("userId"))
		.stateQuery(countState, new Fields("userId"), new MapGet(),new Fields("count"));

		StormTopology t = topology.build();
		return t;
	}
}

class TrackSplit extends BaseFunction {
	private static Logger log = LoggerFactory.getLogger(DRPCTopologyTest.class);
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		log.debug("tuple:{}", tuple.getString(0));
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