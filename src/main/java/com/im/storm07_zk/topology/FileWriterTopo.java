package com.im.storm07_zk.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.im.storm07_zk.bolt.FieldsGroupingBolt;
import com.im.storm07_zk.bolt.WriterBolt;
import com.im.storm07_zk.spout.MetaSpout;
import com.im.storm07_zk.spout.StringScheme;
import com.im.storm07_zk.utils.PropertyUtil;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class FileWriterTopo {

	/**
	 * 数据写入文本的Topo
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: topic group");
			System.exit(2);
		}
		String topic = args[0];
		String group = args[1];
		
//		zkConnect=master:2181
//		zkSessionTimeoutMs=30000
//		zkConnectionTimeoutMs=30000
//		zkSyncTimeMs=5000
//
//		scheme=date,id,content
//		separator=,
//		target=date
		
		// 设置ZK配置信息
		final ZKConfig zkConfig = new ZKConfig();
		zkConfig.zkConnect = PropertyUtil.getProperty("zkConnect");
		zkConfig.zkSessionTimeoutMs = Integer.parseInt(PropertyUtil.getProperty("zkSessionTimeoutMs"));
		zkConfig.zkConnectionTimeoutMs = Integer.parseInt(PropertyUtil.getProperty("zkConnectionTimeoutMs"));
		zkConfig.zkSyncTimeMs = Integer.parseInt(PropertyUtil.getProperty("zkSyncTimeMs"));

		// 设置MetaQ
		final MetaClientConfig metaClientConfig = new MetaClientConfig();
		metaClientConfig.setZkConfig(zkConfig);
		ConsumerConfig consumerConfig = new ConsumerConfig(group);
		
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();
		conf.setNumWorkers(2);
		builder.setSpout("meta-spout", new MetaSpout(metaClientConfig, topic, consumerConfig, new StringScheme()));
		builder.setBolt("field-grouping-bolt", new FieldsGroupingBolt(), 2).shuffleGrouping("meta-spout");
		builder.setBolt("file-writer-bolt", new WriterBolt(), 4).fieldsGrouping("field-grouping-bolt", new Fields("partition"));
		StormSubmitter.submitTopology("fileWriter", conf, builder.createTopology());
		

	}

}
