package com.book1.t02_ack;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private ConcurrentHashMap<UUID, Values> pending;

	private String[] sentences = { "i love you", "i love you too", "do you love me?", "have you love me", "no thanks" };
	private int index = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		pending = new ConcurrentHashMap<>();
	}

	@Override
	public void nextTuple() {
		Values values = new Values(sentences[index]);
		UUID msgId = UUID.randomUUID();
		this.pending.put(msgId, values);
		this.collector.emit(values,msgId);
		index++;
		if (index >= sentences.length)
			index = 0;
		Utils.sleep(100);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	@Override
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId),msgId);
	}

}
