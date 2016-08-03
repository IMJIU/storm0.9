package com.book1.t04_log_analysis.log_kafka;

import java.util.Properties;

import com.book1.t04_log_analysis.formatter.Formatter;
import com.book1.t04_log_analysis.formatter.MessageFormatter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaAppender extends AppenderBase<ILoggingEvent>{
	private String topic;
	private String zookeeperHost;
	private Producer<String,String>producer;
	private Formatter formatter;
	
	@Override
	public void addWarn(String msg) {
		// TODO Auto-generated method stub
		super.addWarn(msg);
	}
	@Override
	public void start() {
		System.out.println("start......");
		if(this.formatter == null){
			this.formatter = new MessageFormatter();
		}
		super.start();
		Properties props = new Properties();
		props.put("zk.connect", this.zookeeperHost);
		System.out.println("zookeeper:"+zookeeperHost);
		 props.put("metadata.broker.list", "192.168.199.210:9092");
        //配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<>(config);
	}
	
	@Override
	public void append(ILoggingEvent event) {
		String payload = this.formatter.format(event);
//		System.out.println("topic:"+topic+" pay:"+payload);
		this.producer.send(new KeyedMessage<String, String>(topic, payload));
	}
	@Override
	public void stop() {
		System.out.println("stop......");
		super.stop();
		this.producer.close();
	}
	
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getZookeeperHost() {
		return zookeeperHost;
	}

	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}

	public Producer<String, String> getProducer() {
		return producer;
	}

	public void setProducer(Producer<String, String> producer) {
		this.producer = producer;
	}

	public Formatter getFormatter() {
		return formatter;
	}

	public void setFormatter(Formatter formatter) {
		this.formatter = formatter;
	}

}
