package com.book3.t02;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.book3.t01.LogWriter;

import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class Topology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("log-reader", new LogReader(), 1);

		builder.setBolt("log-stat", new LogStat(), 2)
		.fieldsGrouping("log-reader", "log", new Fields("user"))
		.allGrouping("log-reader", "stop");

		builder.setBolt("log-writer", new LogWriter(), 1)
		.shuffleGrouping("log-stat");

		Config conf = new Config();
		conf.setNumAckers(5);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("t01", conf, builder.createTopology());
		} else {
			conf.setNumAckers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
		Utils.sleep(200000);
	}

}

class LogReader extends BaseRichSpout {
	private SpoutOutputCollector _collector;
	private Random _rand = new Random();
	private int _count = 100;
	private String[] _users = { "a", "b", "c", "d", "e" };
	private String[] _urls = { "url1", "url2", "url3", "url4", "url5" };

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			Thread.sleep(1000);
			while (_count-- > 0) {
				if (_count == 0) {
					_collector.emit("stop", new Values(""));
				} else {
					_collector.emit("log",new Values(System.currentTimeMillis(), _users[_rand.nextInt(5)], _urls[_rand.nextInt(5)]));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("log",new Fields("time", "user", "url"));
		declarer.declareStream("stop",new Fields(""));
	}

}

class LogStat extends BaseRichBolt {
	private OutputCollector _collector;
	private Map<String,Integer> _pvMap = new HashMap<>();
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String streamId = input.getSourceStreamId();
		
		if(streamId.equals("log")){
			String user = input.getStringByField("user");
			
			if(_pvMap.containsKey(user)){
				_pvMap.put(user, _pvMap.get(user)+1);
			}else{
				_pvMap.put(user, 1);
			}
		}
		if(streamId.equals("stop")){
			Iterator<Entry<String,Integer>>it = _pvMap.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String,Integer>entry = it.next();
				_collector.emit(new Values(entry.getKey(),entry.getValue()));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user","pv"));
	}

}