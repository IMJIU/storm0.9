package com.book1.t04;

import java.util.Properties;

import com.book1.t04.formatter.Formatter;
import com.book1.t04.formatter.MessageFormatter;

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
	public void start() {
		if(this.formatter == null){
			this.formatter = new MessageFormatter();
		}
		super.start();
		Properties props = new Properties();
		props.put("zk.connect", this.zookeeperHost);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		this.producer = new Producer<>(config);
	}
	@Override
	protected void append(ILoggingEvent event) {
		String payload = this.formatter.format(event);
		this.producer.send(new KeyedMessage<String, String>(topic, payload));
	}
	@Override
	public void stop() {
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
