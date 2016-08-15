package com.book2.t07;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TrackSplit extends BaseFunction {
	private static Logger log = LoggerFactory.getLogger(DRPCTopologyTest.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		log.debug("tuple:{}", tuple.getString(0));
		String sentence = (String) tuple.getValue(0);
		if (sentence != null) {
			String[] items = (sentence + "\n").split(" ");
			String userId = items[0];
			String url = items[1];
			String buttonPosition = items[2];
			collector.emit(new Values(userId, url, buttonPosition));
		}
	}

}