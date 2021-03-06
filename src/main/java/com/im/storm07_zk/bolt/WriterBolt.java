package com.im.storm07_zk.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WriterBolt extends BaseBasicBolt {

	private static final Log log = LogFactory.getLog(WriterBolt.class);
	private HashMap<String, FileWriter> writerMap = new HashMap<String, FileWriter>();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	private String filePath = "/home/cloud/logs/";
	private String lineSeparator = System.getProperty("line.separator");
	private static final long serialVersionUID = -8235524993337289148L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String partition = input.getStringByField("partition");
		String line = input.getStringByField("line");
		lock.readLock().lock();
		FileWriter fileWriter = writerMap.get(partition);
		if (fileWriter == null) {
			lock.readLock().unlock();
			lock.writeLock().lock();
			try {
				if (writerMap.get(partition) == null) {
					fileWriter = new FileWriter(filePath + partition, true);
					writerMap.put(partition, fileWriter);
				}
			} catch (IOException e) {
				log.error(e);
			} finally {
				lock.writeLock().unlock();
			}
			lock.readLock().lock();
		}
		try {
			fileWriter.write(line);
			fileWriter.write(lineSeparator);
			fileWriter.flush();
		} catch (IOException e) {
			log.error(e);
		} finally{
			lock.readLock().unlock();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
