package com.book3.t02;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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
		}else if(streamId.equals("stop")){
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