package com.book1.t04_log_analysis.func;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t04_log_analysis.msg.MessageMapper;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class PrintFunction extends BaseFunction{
//	private static final Logger log = Logger.getLogger(PrintFunction.class);
	private static final Logger log = LoggerFactory.getLogger(PrintFunction.class);

	private MessageMapper mapper;
	
	public  PrintFunction(MessageMapper m) {
		mapper = m;
	}
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		System.out.println("why...................");
		System.out.println(tuple.getValues());
		log.warn(mapper.toMessageBody(tuple));
	}

}
