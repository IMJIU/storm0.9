package com.book1.t02_trident.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DispatchAlert extends BaseFunction {
	private Logger log = LoggerFactory.getLogger(DispatchAlert.class);
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String alert = (String) tuple.getValue(0);
        log.error("ALERT RECEIVED [" + alert + "]");
        log.error("Dispatch the national guard!");
        System.exit(0);
    }
}
