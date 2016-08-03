package com.book1.t06_compute_xox.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t06_compute_xox.model.Board;
import com.book1.t06_compute_xox.operators.ArgsFunction;
import com.book1.t06_compute_xox.operators.FindBestMove;
import com.book1.t06_compute_xox.operators.GenerateBoards;
import com.book1.t06_compute_xox.operators.ScoreFunction;

import storm.trident.TridentTopology;

public class DrpcTopology {
    private static final Logger LOG = LoggerFactory.getLogger(DrpcTopology.class);

    public static void main(String[] args) throws Exception {
        final LocalCluster cluster = new LocalCluster();
        final Config conf = new Config();

        LocalDRPC client = new LocalDRPC();
        TridentTopology drpcTopology = new TridentTopology();

        drpcTopology.newDRPCStream("drpc", client)
                .each(new Fields("args"), new ArgsFunction(), new Fields("gamestate"))
                .each(new Fields("gamestate"), new GenerateBoards(), new Fields("children"))
                .each(new Fields("children"), new ScoreFunction(), new Fields("board", "score", "player"))
                .groupBy(new Fields("gamestate"))
                .aggregate(new Fields("board", "score"), new FindBestMove(), new Fields("bestMove"))
                .project(new Fields("bestMove"));

        cluster.submitTopology("drpcTopology", conf, drpcTopology.build());

        Board board = new Board();
        board.board[1][1] = "O";
        board.board[2][2] = "X";
        board.board[0][1] = "O";
        board.board[0][0] = "X";
        LOG.info("Determing best move for O on:" + board.toString());
        LOG.info("RECEIVED RESPONSE [" + client.execute("drpc", board.toKey()) + "]");
    }
}
