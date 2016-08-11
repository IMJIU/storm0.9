package com.book2.t07;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class StateCount implements CombinerAggregator<Integer> {
	private Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public Integer init(TridentTuple tuple) {
		return 1;
	}

	@Override
	public Integer combine(Integer val1, Integer val2) {
		//log.info("v1:{}-v2:{}", val1, val2);
		return val1 + val2;
	}

	@Override
	public Integer zero() {
		return 0;
	}

}