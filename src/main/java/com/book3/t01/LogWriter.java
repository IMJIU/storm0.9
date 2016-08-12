package com.book3.t01;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogWriter extends BaseRichBolt {
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		System.out.println(String.format("%s:%d", input.getStringByField("user"),input.getIntegerByField("pv")));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
