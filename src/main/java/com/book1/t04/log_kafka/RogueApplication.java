package com.book1.t04.log_kafka;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

public class RogueApplication {
	private static final Logger log = LoggerFactory.getLogger(RogueApplication.class);
//	private static final Logger log = Logger.getLogger(RogueApplication.class);
	
	public static void main(String[]args)throws Exception{
		int slowCount = 6;
		int fastCount = 6;
		for (int i = 0; i < slowCount; i++) {
			log.debug("this is a warning (slow state).");
			System.out.println("this is a warning (slow state).");
			Utils.sleep(3000);
		}
		for (int i = 0; i < fastCount; i++) {
			log.warn("this is a warning (rapid state).");
			System.out.println("this is a warning (rapid state).");
			Utils.sleep(1000);
		}
		for (int i = 0; i < 100; i++) {
			log.warn("this is a warning (slow state).");
			Utils.sleep(5000);
		}
	}
}
