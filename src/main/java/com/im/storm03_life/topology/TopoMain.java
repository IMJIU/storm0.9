package com.im.storm03_life.topology;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t06_compute_xox.topology.ScoringTopology;
import com.im.storm03_life.bolt.TransferBolt;
import com.im.storm03_life.bolt.WriterBolt;
import com.im.storm03_life.spout.RandomWordSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TopoMain {
	private static final Logger log = LoggerFactory.getLogger(TopoMain.class);
//	private static final Log log = LogFactory.getLog(TopoMain.class);
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("random", new RandomWordSpout(), 2);
		builder.setBolt("transfer", new TransferBolt(), 4).shuffleGrouping("random");
		builder.setBolt("writer", new WriterBolt(), 4).fieldsGrouping("transfer", new Fields("word"));
		Config conf = new Config();
		conf.setNumWorkers(2);
		conf.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("123", conf, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("123");
		cluster.shutdown();
	
		log.warn("$$$$$$$$$$$ submitting topology...");
//		StormSubmitter.submitTopology("life-cycle", conf, builder.createTopology());
		log.warn("$$$$$$$4$$$ topology submitted !");
	}

}
