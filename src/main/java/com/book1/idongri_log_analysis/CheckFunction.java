package com.book1.idongri_log_analysis;

import java.util.*;

import org.json.simple.JSONValue;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CheckFunction extends BaseFunction {
	private static final long serialVersionUID = 1L;
	public static final int THRESHOLD = 3;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String key = (String) tuple.getValue(0);
		Long count = (Long) tuple.getValue(1);
		if (count > THRESHOLD) {
			List<Object> values = new ArrayList<Object>();
			values.add("========warning!! [" + key + ":"+count+"]");
			System.out.println(values);
			collector.emit(values);
		}
	}

}
