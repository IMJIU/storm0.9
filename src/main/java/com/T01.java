package com;

import backtype.storm.utils.Utils;
import storm.starter.tools.NthLastModifiedTimeTracker;

public class T01 {
	public static void main(String[] args) {
		NthLastModifiedTimeTracker t = new NthLastModifiedTimeTracker(3);
		int cur = t.secondsSinceOldestModification();
		System.out.println("cur:"+cur);
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
		t.markAsModified();
		Utils.sleep(2000);
		System.out.println("cur:"+t.secondsSinceOldestModification());
	}
}
