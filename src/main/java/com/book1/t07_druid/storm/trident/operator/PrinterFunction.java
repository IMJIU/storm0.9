package com.book1.t07_druid.storm.trident.operator;

import java.util.ArrayList;
import java.util.List;

import com.book1.t07_druid.storm.model.FixMessageDto;
import com.esotericsoftware.minlog.Log;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class PrinterFunction extends BaseFunction {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        FixMessageDto message = (FixMessageDto) tuple.getValue(0);
        Log.error("MESSAGE RECEIVED [" + message + "]");
        List<Object> values = new ArrayList<Object>();
        values.add(message);
        collector.emit(values);
    }
}
