package com.im.storm01.topology;

import com.im.storm01.bolt.WordCounter;
import com.im.storm01.bolt.WordSpliter;
import com.im.storm01.spout.WordReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopo {

	/**
	 * Storm word count demo
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
//		if (args.length != 2) {
//			System.err.println("Usage: inputPaht timeOffset");
//			System.err.println("such as : java -jar  WordCount.jar D://input/ 2");
//			System.exit(2);
//		}
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter()).shuffleGrouping("word-spilter");
		String inputPaht = "d://log/debug.log";
		String timeOffset = "0";
		Config conf = new Config();
		conf.put("INPUT_PATH", inputPaht);
		conf.put("TIME_OFFSET", timeOffset);
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCount", conf, builder.createTopology());

	}

}
