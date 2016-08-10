package com.book1.t02_trident.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class OutbreakTrendFactory implements StateFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
//    	System.out.println(conf);
//    	System.out.println(metrics);
//    	System.out.println(partitionIndex);
//    	System.out.println(numPartitions);
        return new OutbreakTrendState(new OutbreakTrendBackingMap());
    }
}
