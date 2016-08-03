package com.book1.t05_score.model;

public class Player {
	public static String next(String current) {
		if (current.equals("X"))
			return "O";
		else
			return "X";
	}
}
