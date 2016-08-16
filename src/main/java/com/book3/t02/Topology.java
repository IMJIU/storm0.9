package com.book3.t02;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.book3.t01.LogWriter;

import java.util.Random;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class Topology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("log-reader", new LogReader(), 1);

		builder.setBolt("log-stat", new LogStat(), 2)
		.fieldsGrouping("log-reader", "log", new Fields("user"))
		.allGrouping("log-reader", "stop");

		builder.setBolt("log-writer", new LogWriter(), 1)
		.shuffleGrouping("log-stat");

		Config conf = new Config();
		conf.setNumAckers(5);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("t01", conf, builder.createTopology());
		} else {
			conf.setNumAckers(3);
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
		Utils.sleep(200000);
	}

}

