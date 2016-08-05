package com.book1.t07_druid.storm.trident.operator;

import com.book1.t07_druid.storm.model.FixMessageDto;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class MessageTypeFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = LoggerFactory.getLogger(MessageTypeFilter.class);

    @Override
    public boolean isKeep(TridentTuple tuple) {
        FixMessageDto message = (FixMessageDto) tuple.getValue(0);
        if (message.msgType.equals("8") && message.price > 0) {
            return true;
        }
        return false;
    }
}