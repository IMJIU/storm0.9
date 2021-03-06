package com.im.storm03_life.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.im.storm03_life.topology.TopoMain;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * �����յ��ĵ���д�뵽һ���ļ�����
 * 
 * @author Administrator
 *
 */
public class WriterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6586283337287975719L;

	private static final Logger log = LoggerFactory.getLogger(WriterBolt.class);

	private FileWriter writer = null;

	public WriterBolt() {
		log.warn("&&&&&&&&&&&&&&&&& WriterBolt constructor method invoked");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		log.warn("################# WriterBolt prepare() method invoked");
		try {
			writer = new FileWriter("/home/" + this);
		} catch (IOException e) {
			log.error("error", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		log.warn("################# WriterBolt declareOutputFields() method invoked");
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		log.warn("################# WriterBolt execute() method invoked");
		String s = input.getString(0);
		try {
			writer.write(s);
			writer.write("\n");
			writer.flush();
		} catch (IOException e) {
			log.error("error",e);
			throw new RuntimeException(e);
		}
	}

}
