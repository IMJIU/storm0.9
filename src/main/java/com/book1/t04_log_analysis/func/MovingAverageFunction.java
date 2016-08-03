package com.book1.t04_log_analysis.func;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t04_log_analysis.EWMA;
import com.book1.t04_log_analysis.EWMA.Time;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class MovingAverageFunction extends BaseFunction{
//	private static final Logger log = Logger.getLogger(MovingAverageFunction.class);
	private static final Logger log = LoggerFactory.getLogger(MovingAverageFunction.class);
	private EWMA ewma;
	private Time emitRatePer;
	
	public  MovingAverageFunction(EWMA ewma,Time emitRatePer) {
		this.ewma = ewma;
		this.emitRatePer = emitRatePer;
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		ewma.mark(tuple.getLong(0));
		log.debug("Rate:"+ewma.getAverageRatePer(emitRatePer));
		collector.emit(new Values(this.ewma.getAverageRatePer(emitRatePer)));
	}

}
