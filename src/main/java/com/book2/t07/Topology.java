package com.book2.t07;

import java.net.InetSocketAddress;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class Topology {
	
	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("track"), 2
				,new Values("101 item.com/item/200001 fav")
				,new Values("102 xxx.com/item/200002 addcart")
				,new Values("102 xxx.com/item/200002 addcart"));
		spout.setCycle(true);
		
		TridentTopology topology = new TridentTopology();
		StateFactory memState = new MemoryMapState.Factory();
//		StateFactory dbstate = RedisState.transactional(new InetSocketAddress("192.168.xxx",6379));
		TridentState actions = topology.newStream("userstat", spout).shuffle()
				.each(new Fields("track"), new TrackSplit(),new Fields("userId"))
				.groupBy(new Fields("userId"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("agg_users"))
				.parallelismHint(2);
		
		topology.build();
	}

}
class TrackSplit extends BaseFunction{

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = (String)tuple.getValue(0);
		if(sentence != null){
			String[] items = (sentence +"\n").split(" ");
			String userId = items[0];
			String url = items[1];
			String buttonPosition = items[2];
			collector.emit(new Values(userId,url,buttonPosition));
		}
	}
	
}