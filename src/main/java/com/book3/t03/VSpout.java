package com.book3.t03;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

class VSpout extends BaseRichSpout {
	private SpoutOutputCollector _collector;
	private String[] _users = { "a", "b", "c", "d", "e" };
	private String[] _srcids = { "s1", "s2", "s3", "s4", "s5" };
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
				_collector.emit("visit", new Values(System.currentTimeMillis(), _users[i], _srcids[i]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("visit", new Fields("time", "user", "srcid"));
	}

}