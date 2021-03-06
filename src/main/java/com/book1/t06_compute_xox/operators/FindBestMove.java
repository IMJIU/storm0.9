package com.book1.t06_compute_xox.operators;

import backtype.storm.tuple.Values;

import com.book1.t06_compute_xox.model.BestMove;
import com.book1.t06_compute_xox.model.Board;
import com.esotericsoftware.minlog.Log;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class FindBestMove extends BaseAggregator<BestMove> {
    private static final long serialVersionUID = 1L;

    @Override
    public BestMove init(Object batchId, TridentCollector collector) {
        Log.info("Batch Id = [" + batchId + "]");
        return new BestMove();
    }

    @Override
    public void aggregate(BestMove currentBestMove, TridentTuple tuple, TridentCollector collector) {
        Board board = (Board) tuple.get(0);
        Integer score = tuple.getInteger(1);
        if (score > currentBestMove.score) {
            currentBestMove.score = score;
            currentBestMove.bestMove = board;
        }
    }

    @Override
    public void complete(BestMove bestMove, TridentCollector collector) {
        collector.emit(new Values(bestMove));
    }

}