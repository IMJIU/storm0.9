package com.book1.t05_score.operators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t05_score.model.GameState;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class isEndGame extends BaseFilter {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(isEndGame.class);

    @Override
    public boolean isKeep(TridentTuple tuple) {
        GameState gameState = (GameState) tuple.get(0);
        boolean keep = (gameState.getBoard().isEndState());
        if (keep) {
            LOG.info("END GAME [" + gameState + "]");
        }
        return keep;
    }
}