package com.book1.t05_score.operators;

import com.book1.t05_score.model.Board;
import com.book1.t05_score.model.GameState;
import com.book1.t05_score.model.Player;
import com.esotericsoftware.minlog.Log;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class GenerateBoards extends BaseFunction {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        GameState gameState = (GameState) tuple.get(0);
        Board currentBoard = gameState.getBoard();
        List<Board> history = new ArrayList<Board>();
        history.addAll(gameState.getHistory());
        history.add(currentBoard);

        if (!currentBoard.isEndState()) {
            String nextPlayer = Player.next(gameState.getPlayer());
            List<Board> boards = gameState.getBoard().nextBoards(nextPlayer);
            Log.info("Generated [" + boards.size() + "] children boards for [" + gameState.toString() + "]");
            for (Board b : boards) {
                GameState newGameState = new GameState(b, history, nextPlayer);
                List<Object> values = new ArrayList<Object>();
                values.add(newGameState);
                collector.emit(values);
            }
        } else {
            Log.info("End game found! [" + currentBoard + "]");
        }
    }
}