package com.book3.t03;

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

		builder.setSpout("log-vspout", new VSpout(), 1);
		builder.setSpout("log-bspout", new BSpout(), 1);

		builder.setBolt("log-merge", new LogMergeBolt(), 2)
		.fieldsGrouping("log-vspout", "visit", new Fields("user"))
		.fieldsGrouping("log-bspout", "business",new Fields("user"));

		builder.setBolt("log-stat", new LogStatBolt(), 2)
		.fieldsGrouping("log-merge", new Fields("srcid"));

		Config conf = new Config();
		// 实时计算不需要可靠消息，故关闭acker节省通信资源
//		conf.setNumAckers(0);
		// 一般设置为spout和bolt总task数量相等或更多
		// 以免多task集中在一个jvm里运行
//		conf.setNumWorkers(7);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("t03", conf, builder.createTopology());
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


