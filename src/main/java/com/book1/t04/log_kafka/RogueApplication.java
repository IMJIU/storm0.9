package com.book1.t04;

import org.apache.log4j.Logger;

import backtype.storm.utils.Utils;

public class RogueApplication {
//	private static final Logger log = Logger.getLogger(RogueApplication.class);
	private static final Logger log = Logger.getLogger(RogueApplication.class);
	
	public static void main(String[]args)throws Exception{
		int slowCount = 6;
		int fastCount = 15;
		for (int i = 0; i < slowCount; i++) {
			log.warn("this is a warning (slow state).");
			Utils.sleep(5000);
		}
		for (int i = 0; i < fastCount; i++) {
			log.warn("this is a warning (rapid state).");
			Utils.sleep(5000);
		}
		for (int i = 0; i < slowCount; i++) {
			log.warn("this is a warning (slow state).");
			Utils.sleep(5000);
		}
	}
}
