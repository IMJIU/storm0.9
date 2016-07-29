package com.book1.t02_trident.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.book1.t02_trident.model.DiagnosisEvent;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class HourAssignment extends BaseFunction {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = Logger.getLogger(HourAssignment.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		String city = (String) tuple.getValue(1);
		long timestamp = diagnosis.time;
		long hourSinceEpoch = timestamp / 1000 / 60 / 60;
		LOG.debug("Key =  [" + city + ":" + diagnosis.diagnosisCode  +":" +hourSinceEpoch+ "]");
		String key = city + ":" + diagnosis.diagnosisCode + ":" + hourSinceEpoch;

		List<Object> values = new ArrayList<Object>();
		values.add(hourSinceEpoch);
		values.add(key);
		collector.emit(values);
	}
}
