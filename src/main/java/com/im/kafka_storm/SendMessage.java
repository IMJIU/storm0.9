package com.im.kafka_storm;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class SendMessage {

	public static void main(String[] args) {
		System.out.println(StringEncoder.class.getName());
		Properties p = new Properties();
		p.put("zookeeper.connect", "192.168.199.210:2181");
		p.put("serializer.class", StringEncoder.class.getName());
		p.put("producer.type", "async");
		p.put("compression.codec", "1");
		p.put("metadata.broker.list", "192.168.199.210:9092");

		ProducerConfig config = new ProducerConfig(p);
		Producer<String, String> producer = new Producer<String, String>(config);
		Random r = new Random();
		for (int i = 0; i < 100; i++) {
			int id = r.nextInt(10000000);
			int memberid = r.nextInt(10000);
			int totalPrice = r.nextInt(100) + 10;
			int youhui = r.nextInt(100);
			int sendpay = r.nextInt(3);

			StringBuilder data = new StringBuilder();
			data.append(String.valueOf(id)).append("\t").append(String.valueOf(memberid)).append("\t").append(String.valueOf(totalPrice)).append("\t").append(String.valueOf(youhui)).append("\t")
			        .append(String.valueOf(sendpay)).append("\t").append("2016-05-06");
			System.out.println(data.toString());
			producer.send(new KeyedMessage<String, String>("order", data.toString()));
		}
		producer.close();
		System.out.println("send over....");

	}

}
