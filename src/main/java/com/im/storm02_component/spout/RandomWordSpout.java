package com.im.storm02_component.spout;

import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
/**
 * �����String���鵱�ж�ȡһ�����ʷ��͸���һ��bolt
 * @author Administrator
 *
 */
public class RandomWordSpout extends BaseRichSpout {

	private static final long serialVersionUID = -4287209449750623371L;
	
	private SpoutOutputCollector collector;
	
	private String[] words = new String[]{"storm", "hadoop", "hive", "flume"};
	
	private Random random = new Random();
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("str"));
	}

	@Override
	public void nextTuple() {
		Utils.sleep(500);
		String str = words[random.nextInt(words.length)];
		collector.emit(new Values(str));
	}

}
