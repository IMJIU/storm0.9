package com.book1.t08.storm.trident.state;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.book1.t08.druid.firehose.StormFirehose;
import com.book1.t08.druid.firehose.StormFirehoseFactory;

//import com.packtpub.druid.firehose.StormFirehose;
//import com.packtpub.druid.firehose.StormFirehoseFactory;

import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

public class DruidState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(DruidState.class);
    private List<TridentTuple> batch = new ArrayList<TridentTuple>();

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txId) {
        LOG.info("Committing [" + txId + "]");
        try {
            if (StormFirehose.STATUS.isCompleted(txId)) {
                LOG.warn("Encountered previously completed txId [" + txId + "]");
                return;
            } else if (StormFirehose.STATUS.isInLimbo(txId)) {
                LOG.error("Encountered txId in limbo [" + txId + "] : NOTIFY THE AUTHORITIES!");
                return;
            } else if (StormFirehose.STATUS.isInProgress(txId)) {
                LOG.error("Encountered txId in processing [" + txId + "] : NOTIFY THE AUTHORITIES!");
                return;
            }
            StormFirehose.STATUS.putInProgress(txId);
            StormFirehoseFactory.getFirehose().sendBatch(txId, batch);
        } catch (Exception e) {
            LOG.error("Could not start firehose [" + txId + "]", e);
        }
    }

    public void aggregateTuples(List<TridentTuple> tuples) {
        batch.addAll(tuples);
    }
}
