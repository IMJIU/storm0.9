package com.book1.t06_compute_xox.model;

public class Player {
	public static String next(String current) {
		if (current.equals("X"))
			return "O";
		else
			return "X";
	}
}
