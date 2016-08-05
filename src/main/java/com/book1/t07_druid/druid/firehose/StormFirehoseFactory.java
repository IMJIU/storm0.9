package com.book1.t07_druid.druid.firehose;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
//import com.metamx.druid.realtime.firehose.Firehose;
//import com.metamx.druid.realtime.firehose.FirehoseFactory;


@JsonTypeName("storm")
public class StormFirehoseFactory implements FirehoseFactory {
    private static final StormFirehose FIREHOSE = new StormFirehose();

    @JsonCreator
    public StormFirehoseFactory() {
    }

    public static StormFirehose getFirehose() {
        return FIREHOSE;
    }

	@Override
	public Firehose connect() throws IOException {
		// TODO Auto-generated method stub
		return FIREHOSE;
	}
}
