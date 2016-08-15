package com.book3.t03;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LogMergeBolt extends BaseRichBolt {
	private OutputCollector _collector;
	private Map<String, String> srcmap;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		if (srcmap == null) {
			srcmap = new HashMap<>();
		}
	}

	@Override
	public void execute(Tuple input) {
		String streamId = input.getSourceStreamId();

		String user = input.getStringByField("user");
		if (streamId.equals("visit")) {
			String srcid = input.getStringByField("srcid");
			srcmap.put(user, srcid);
		}else if (streamId.equals("business")) {
			String pay = input.getStringByField("pay");
			String srcid = srcmap.get(user);
			if(srcid != null){
				_collector.emit(new Values(user,pay,srcid));
				srcmap.remove(user);
			}else{
				//成交日志快于流量日志才发生
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("user", "pay","srcid"));
	}

}