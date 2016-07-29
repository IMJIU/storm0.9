package com.book1.t05_score.operators;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

import com.book1.t05_score.model.Board;
import com.book1.t05_score.model.GameState;
import com.book1.t05_score.model.Player;

public class ScoreFunction extends BaseFunction {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        GameState gameState = (GameState) tuple.get(0);
        String player = gameState.getPlayer();
        int score = gameState.score();

        List<Object> values = new ArrayList<Object>();
        values.add(gameState.getBoard());
        values.add(score);
        values.add(player);
        collector.emit(values);

        for (Board b : gameState.getHistory()) {
            player = Player.next(player);
            values = new ArrayList<Object>();
            values.add(b);
            values.add(score);
            values.add(player);
            collector.emit(values);
        }
    }
}