package com.im.storm06.drpc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReachTopology {

	public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {

		{
			put("foo.com/blog/1", Arrays.asList("a", "b", "c", "d", "e"));
			put("engineering.twitter.com/blog/5", Arrays.asList("a", "b", "c", "d"));
			put("tech.backtype.com/blog/123", Arrays.asList("c", "d", "e"));
		}
	};

	public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {

		{
			put("a", Arrays.asList("a", "c", "d", "e", "f", "g"));
			put("b", Arrays.asList("c", "d", "e", "f", "a"));
			put("c", Arrays.asList("d"));
			put("d", Arrays.asList("e", "f", "a", "b"));
			put("e", Arrays.asList("c", "d", "g"));
			put("f", Arrays.asList("e", "a", "b"));
			put("g", Arrays.asList("c", "d", "e"));
		}
	};

	public static class GetTweeters extends BaseBasicBolt {

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			Object id = tuple.getValue(0);
			String url = tuple.getString(1);
			System.out.println("GetTweeters arg0:"+id+"-arg1:"+url);
			List<String> tweeters = TWEETERS_DB.get(url);
			if (tweeters != null) {
				for (String tweeter : tweeters) {
					System.out.println("GetTweeters emit() id:"+id+" tweeter:"+tweeter);
					collector.emit(new Values(id, tweeter));
				}
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "tweeter"));

		}
	}

	public static class GetFollowers extends BaseBasicBolt {

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			Object id = tuple.getValue(0);
			String tweeter = tuple.getString(1);
			System.out.println("GetFollowers arg0:"+id+"-arg1:"+tweeter);
			List<String> followers = FOLLOWERS_DB.get(tweeter);
			if (followers != null) {
				for (String follower : followers) {
					System.out.println("GetFollowers emit() id:"+id+" tweeter:"+tweeter+" follower:"+follower);
					collector.emit(new Values(id, follower));
				}
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "follower"));
		}
	}

	public static class PartialUniquer extends BaseBatchBolt {

		BatchOutputCollector _collector;

		Object _id;

		Set<String> _followers = new HashSet<String>();

		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
		}

		@Override
		public void execute(Tuple tuple) {
			System.out.println("PartialUniquer execute:"+tuple.getLong(0)+"-"+tuple.getString(1));
			_followers.add(tuple.getString(1));
		}

		@Override
		public void finishBatch() {
			System.out.println("PartialUniquer finishBatch: id-"+_id+" xx-"+_followers+" size:"+_followers.size());
			_collector.emit(new Values(_id, _followers.size()));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "partial-count"));
		}
	}

	public static class CountAggregator extends BaseBatchBolt {

		BatchOutputCollector _collector;

		Object _id;

		int _count = 0;

		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
		}

		@Override
		public void execute(Tuple tuple) {
			System.out.println("CountAggregator execute:"+tuple.getLong(0)+"-"+tuple.getInteger(1));
			_count += tuple.getInteger(1);
		}

		@Override
		public void finishBatch() {
			System.out.println("CountAggregator finishBatch: id-"+_id+" count:"+_count);
			_collector.emit(new Values(_id, _count));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "reach"));
		}
	}

	public static LinearDRPCTopologyBuilder construct() {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
		builder.addBolt(new GetTweeters(), 4);
		builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
		builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
		builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));
		return builder;
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		LinearDRPCTopologyBuilder builder = construct();

		Config conf = new Config();

		if (args == null || args.length < 3) {
			conf.setMaxTaskParallelism(3);
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("reach-drpc", conf, builder.createLocalTopology(drpc));

			String[] urlsToTry = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "tech.backtype.com/blog/123" };

			for (String url : urlsToTry) {
				System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
			}

			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumAckers(6);
			StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
		}
	}
}
