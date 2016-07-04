package com.im.storm05_reliable.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageSpout implements IRichSpout {

	private static final long serialVersionUID = -4664068313075450186L;

	private int index = 0;
	
	private String[] lines;
	
	private SpoutOutputCollector collector;
	
	public MessageSpout(){
		lines = new String[]{
				"0,zero",
				"1,one",
				"2,two",
				"3,three",
				"4,four",
				"5,five",
				"6,six",
				"7,seven",
				"8,eight",
				"9,nine"
		};
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		if(index < lines.length){
			String l = lines[index];
			collector.emit(new Values(l), index);
			index++;
		}
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("message sends successfully (msgId = " + msgId +")");
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("error : message sends unsuccessfully (msgId = " + msgId +")");
		System.out.println("resending...");
		collector.emit(new Values(lines[(Integer) msgId]), msgId);
		System.out.println("resend successfully");
	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
