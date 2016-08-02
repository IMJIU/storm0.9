package com.book1.t04.msg;

import java.util.Date;

import storm.trident.tuple.TridentTuple;


public class NotifyMessageMapper  implements MessageMapper{
	@Override
	public String toMessageBody(TridentTuple tuple) {
		StringBuilder sb = new StringBuilder();
		sb.append(new Date(tuple.getLongByField("timestamp")))
		.append(" application：").append(tuple.getStringByField("logger"))
		.append(" changed state：").append(tuple.getDoubleByField("threshold\n"))
		.append(" last value：").append(tuple.getDoubleByField("avarage\n"))
		.append(" last message：").append(tuple.getStringByField("message"));
		return sb.toString();
	}

}
