package com.book1.idongri_log_analysis;

import java.util.*;

import org.json.simple.JSONValue;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitFunction extends BaseFunction {
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String string = tuple.getString(0);
//		System.out.println("str:" + string);
		String[] ss = string.split(" ");
		Values values = new Values();
//		for (int i = 0; i < ss.length; i++) {
//			System.out.println(ss[i]);
////			values.add(ss[i]);
//		}
		String method = ss[6];
		String time = ss[7].replaceAll("ms", "");
		int len1 = method.indexOf(")")+1;
		values.add(method.substring(0,len1));
		values.add(time);
//		System.out.println(values);
		collector.emit(values);
	}

}
