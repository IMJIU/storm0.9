package com.book3.t01;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class Topology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("log-reader", new LogReader(),1);
		
		builder.setBolt("log-stat", new LogStat(),1)
		.fieldsGrouping("log-reader",new Fields("user"));

		
		builder.setBolt("log-writer", new LogWriter(),1)
		.shuffleGrouping("log-stat");
		
		Config conf = new Config();
//		conf.setNumAckers(5);
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
