package com.book1.t05_score.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Board implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String EMPTY = " ";
	public String[][] board = { { EMPTY, EMPTY, EMPTY }, { EMPTY, EMPTY, EMPTY }, { EMPTY, EMPTY, EMPTY } };

	public Board() {
	}
	public static void main(String[] args) {
		Board b = new Board("   XXX   ");
		List<Board>list = b.nextBoards("X");
		for (Board board : list) {
			System.out.println(board);
		}
	}
	public Board(String key) {
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				this.board[i][j] = "" + key.charAt(i * 3 + j);
			}
		}
	}

	public List<Board> nextBoards(String player) {
		List<Board> bordList = new ArrayList<Board>();
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				if (board[i][j].equals(EMPTY)) {
					Board newBoard = this.clone();
					newBoard.board[i][j] = player;
					bordList.add(newBoard);
				}
			}
		}
		return bordList;
	}
	/**
	 * 是否结束
	 * 下满 或 胜利
	 * @return
	 */
	public boolean isEndState() {
		return (nextBoards("X").size() == 0 || Math.abs(score("X")) > 1000);
	}

	public int score(String player) {
		return scoreLines(player) - scoreLines(Player.next(player));
	}
	public String getWin(){
		int score = 0;
		String player = "X";
		score += scoreLine(board[0][0], board[1][0], board[2][0], player);
		score += scoreLine(board[0][1], board[1][1], board[2][1], player);
		score += scoreLine(board[0][2], board[1][2], board[2][2], player);
		if(score>1000){
			return "3列X";
		}
		score += scoreLine(board[0][0], board[0][1], board[0][2], player);
		score += scoreLine(board[1][0], board[1][1], board[1][2], player);
		score += scoreLine(board[2][0], board[2][1], board[2][2], player);
		if(score>1000){
			return "3行X";
		}
		score += scoreLine(board[0][0], board[1][1], board[2][2], player);
		score += scoreLine(board[2][0], board[1][1], board[0][2], player);
		if(score>1000){
			return "对角X";
		}
		 player = "O";
		score += scoreLine(board[0][0], board[1][0], board[2][0], player);
		score += scoreLine(board[0][1], board[1][1], board[2][1], player);
		score += scoreLine(board[0][2], board[1][2], board[2][2], player);
		if(score>1000){
			return "3列O";
		}
		score += scoreLine(board[0][0], board[0][1], board[0][2], player);
		score += scoreLine(board[1][0], board[1][1], board[1][2], player);
		score += scoreLine(board[2][0], board[2][1], board[2][2], player);
		if(score>1000){
			return "3行O";
		}
		score += scoreLine(board[0][0], board[1][1], board[2][2], player);
		score += scoreLine(board[2][0], board[1][1], board[0][2], player);
		if(score>1000){
			return "对角O";
		}
		return "mei you";
	}
	public int scoreLines(String player) {
		int score = 0;
		// Columns  3列
		score += scoreLine(board[0][0], board[1][0], board[2][0], player);
		score += scoreLine(board[0][1], board[1][1], board[2][1], player);
		score += scoreLine(board[0][2], board[1][2], board[2][2], player);

		// Rows     3行
		score += scoreLine(board[0][0], board[0][1], board[0][2], player);
		score += scoreLine(board[1][0], board[1][1], board[1][2], player);
		score += scoreLine(board[2][0], board[2][1], board[2][2], player);

		// Diagonals 2对角线
		score += scoreLine(board[0][0], board[1][1], board[2][2], player);
		score += scoreLine(board[2][0], board[1][1], board[0][2], player);
		return score;
	}

	public int scoreLine(String pos1, String pos2, String pos3, String player) {
		int score = 0;
		// xxx=1000分
		if (pos1.equals(player) && pos2.equals(player) && pos3.equals(player)) {
			score = 10000;
		// xxo | oxx 100分	
		} else if (pos1.equals(player) && pos2.equals(player) && pos3.equals(EMPTY) 
				|| pos1.equals(EMPTY) && pos2.equals(player) && pos3.equals(player)) {
			score = 100;
		// xoo|oxo|oox 1分
		} else {
			if (pos1.equals(player) && pos2.equals(EMPTY) && pos3.equals(EMPTY) 
			|| pos1.equals(EMPTY) && pos2.equals(player) && pos3.equals(EMPTY)
			|| pos1.equals(EMPTY) && pos2.equals(EMPTY) && pos3.equals(player)) {
				score = 10;
			}
		}
		return score;
	}

	public Board clone() {
		Board clone = new Board();
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				clone.board[i][j] = this.board[i][j];
			}
		}
		return clone;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("\n---------\n");
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				sb.append("|").append(board[i][j]).append("|");
			}
			sb.append("\n---------\n");
		}
		return sb.toString();
	}

	public String toKey() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				sb.append(board[i][j]);
			}
		}
		return sb.toString();
	}
}