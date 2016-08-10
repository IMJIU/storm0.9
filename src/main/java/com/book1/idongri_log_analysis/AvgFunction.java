package com.book1.idongri_log_analysis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.json.simple.JSONValue;

import com.book1.t04_log_analysis.EWMA;
import com.book1.t04_log_analysis.EWMA.Time;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class AvgFunction extends BaseFunction {
	private static final long serialVersionUID = 1L;
	private Map<String, EWMA> map = new ConcurrentHashMap<>();

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		String key = (String) tuple.getValue(0);
		String count = (String) tuple.getValue(1);
		EWMA ewma = map.getOrDefault(key, new EWMA().sliding(1.0, Time.MINUTES).withAlpha(EWMA.ONE_MINUTE_ALPHA));
		ewma.mark(Long.parseLong(count));
		map.put(key, ewma);
		Values val = new Values();
		val.add(ewma.getAverage());
		System.out.print("key:" + key + " " + ewma.getAverageRatePer(Time.MINUTES));
		System.out.println(" " + ewma.getAverage());
		collector.emit(val);
	}

}
