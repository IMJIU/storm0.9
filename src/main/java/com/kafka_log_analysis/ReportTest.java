package com.kafka_log_analysis;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class ReportTest {
	 
	public static final String brokerHost = "192.168.199.210";
	public static final DateFormat df = new SimpleDateFormat("yyyyMMdd");

	private static final Jedis jedis = new Jedis(brokerHost, 6379);
	
	public static void main(String[] args) {
		String dt = df.format(new Date());
		List<String> list = jedis.lrange(TaleConstants.IP_LOG+dt, 0, jedis.llen(TaleConstants.IP_LOG+dt));
		Map<String,Integer> ipMap = new HashMap<>();
		Map<String,Integer> methodMap = new HashMap<>();
		for (int i = 0; i < list.size(); i++) {
			String key = list.get(i);
			List<String> record = jedis.hmget(key, "count","ip","method","date","hour");
			String count = record.get(0);
			String ip = record.get(1);
			String method = record.get(2);
			String date = record.get(3);
			String hour = record.get(4);
			ipMap.put(ip, ipMap.getOrDefault(ip, 0));
			methodMap.put(method, methodMap.getOrDefault(method, 0));
		}
	}
}
