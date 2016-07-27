package com.book1.t01;

import java.util.*;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ReportBolt extends BaseRichBolt {
	private HashMap<String, Long> counts = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.counts.put(word, count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
	
	@Override
	public void cleanup() {
		System.out.println("------------ end -------------");
		List<String>keys = new ArrayList<>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key+":"+this.counts.get(key));
		}
		System.out.println("------------------------");
	}
}
