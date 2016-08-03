package com.book1.t06_compute_xox.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t06_compute_xox.model.Board;
import com.book1.t06_compute_xox.model.GameState;
import com.book1.t06_compute_xox.operators.ScoreFunction;
import com.book1.t06_compute_xox.operators.ScoreUpdater;
import com.book1.t06_compute_xox.operators.isEndGame;
import com.book1.t06_compute_xox.trident.spout.LocalQueueEmitter;
import com.book1.t06_compute_xox.trident.spout.LocalQueueSpout;

import storm.trident.Stream;
import storm.trident.TridentTopology;

public class ScoringTopology {
	private static final Logger LOG = LoggerFactory.getLogger(ScoringTopology.class);

	public static StormTopology buildTopology() {
		LOG.info("Building topology.");
		TridentTopology topology = new TridentTopology();

		GameState exampleRecursiveState = GameState.playAtRandom(new Board(), "X");
		LOG.info("SIMULATED LEAF NODE : [" + exampleRecursiveState.getBoard() + "] w/ state [" + exampleRecursiveState + "]");

		// Scoring Queue / Spout
		final LocalQueueEmitter<GameState> scoringSpoutEmitter = new LocalQueueEmitter<GameState>("ScoringQueue");
		scoringSpoutEmitter.enqueue(exampleRecursiveState);
		LocalQueueSpout<GameState> scoringSpout = new LocalQueueSpout<GameState>(scoringSpoutEmitter);
		createBoard(scoringSpoutEmitter);
		Stream inputStream = topology.newStream("scoring", scoringSpout);

		inputStream.each(new Fields("gamestate"), new isEndGame())
				.each(new Fields("gamestate"), new ScoreFunction(), new Fields("board", "score", "player"))
				.each(new Fields("board", "score", "player"), new ScoreUpdater(), new Fields());
		
		return topology.build();
	}

	private static void createBoard(final LocalQueueEmitter<GameState> scoringSpoutEmitter) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while(true){
					
					Utils.sleep(5000);
					scoringSpoutEmitter.enqueue(GameState.playAtRandom(new Board(), "X"));
				}
			}
		}).start();
	}

	public static void main(String[] args) throws Exception {
		final Config conf = new Config();
		final LocalCluster cluster = new LocalCluster();

		LOG.info("Submitting topology.");
		cluster.submitTopology("scoringTopology", conf, ScoringTopology.buildTopology());
		LOG.info("Topology submitted.");
		Thread.sleep(600000);
	}
}
