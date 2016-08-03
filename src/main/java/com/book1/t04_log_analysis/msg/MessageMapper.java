package com.book1.t04_log_analysis.msg;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

public interface MessageMapper extends Serializable{
	public String toMessageBody(TridentTuple tuple);
}
