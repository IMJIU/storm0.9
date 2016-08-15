package com.book3.t03;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

class BSpout extends BaseRichSpout {
	private SpoutOutputCollector _collector;
	private String[] _users = { "a", "b", "c", "d", "e" };
	private String[] _pays = { "100", "200", "300", "400", "200" };
	private int _count = 5;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			for (int i = 0; i < _count; i++) {
				Thread.sleep(1000);
				_collector.emit("business", new Values(System.currentTimeMillis(), _users[i], _pays[i]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("business", new Fields("time", "user", "pay"));
	}

}