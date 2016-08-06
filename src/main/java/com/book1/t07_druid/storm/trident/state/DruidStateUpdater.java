package com.book1.t07_druid.storm.trident.state;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t07_druid.storm.model.FixMessageDto;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

public class DruidStateUpdater implements StateUpdater<DruidState> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DruidStateUpdater.class);

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void updateState(DruidState state, List<TridentTuple> tuples, TridentCollector collector) {
        //LOG.info("Updating [" + state + "]");
        for (TridentTuple tuple : tuples) {
            FixMessageDto message = (FixMessageDto) tuple.getValue(0);
            state.aggregateMessage(message);
        }
    }
}
