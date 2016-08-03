package com.book1.t04_log_analysis.func;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t04_log_analysis.EWMA.Time;

import backtype.storm.tuple.Values;
import kafka.utils.threadsafe;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class ThresholdFilterFunction extends BaseFunction {
//	private static final Logger log = Logger.getLogger(ThresholdFilterFunction.class);
	private static final Logger log = LoggerFactory.getLogger(ThresholdFilterFunction.class);
	private static enum State {
		BELOW, ABOVE;
	}

	private State last = State.BELOW;
	private double threshold;

	public ThresholdFilterFunction(double threshold) {
		this.threshold = threshold;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		double val = tuple.getDouble(0);
		State newState = val < this.threshold ? State.BELOW : State.ABOVE;
		boolean stateChange = this.last != newState;
		collector.emit(new Values(stateChange,threshold));
		this.last = newState;
		log.debug("State change ? -->" + stateChange);
	}

}
