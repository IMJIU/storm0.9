package com.book3.t03;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class LogStatBolt extends BaseRichBolt {
	private OutputCollector _collector;
	private Map<String, Long> srcpay;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		if (srcpay == null) {
			srcpay = new HashMap<>();
		}
	}

	@Override
	public void execute(Tuple input) {
		String pay = input.getStringByField("pay");
		String srcid = input.getStringByField("srcid");

		if (srcpay.containsKey(srcid)) {
			srcpay.put(srcid, Long.parseLong(pay) + srcpay.get(srcid));
		} else {
			srcpay.put(srcid, Long.parseLong(pay));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}