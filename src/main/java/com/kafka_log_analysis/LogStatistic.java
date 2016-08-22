package com.kafka_log_analysis;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class LogStatistic {
	public static final String brokerHost = "192.168.199.210";
	private static final Jedis jedis = new Jedis(brokerHost, 6379);


	public void add(String ip, String method, String date, String hour) {
		String key = TaleConstants.IP_LOG + ip + "-" + date + "-" + hour;
		String count = jedis.hget(key, TaleConstants.COUNT);
		if (count == null) {
			Map<String, String> map = new HashMap<>();
			map.put(TaleConstants.COUNT, "1");
			map.put("date", date);
			map.put("hour", hour);
			map.put("ip", ip);
			map.put("method", method);
			jedis.hmset(key, map);
		} else {
			jedis.hset(key, TaleConstants.COUNT, String.valueOf(Integer.parseInt(count) + 1));
		}
		jedis.lpush(TaleConstants.IP_LOG + date, key);
		lpushIfNotExist(TaleConstants.IP_LOG_DATE_ID, TaleConstants.IP_LOG_DATE + date);

	}


	private void lpushIfNotExist(String key, String value) {
		String obj = jedis.get(key);
		if(obj == null){
			jedis.lpush(key, value);
		}
	}
}
