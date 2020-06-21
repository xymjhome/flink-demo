package com.analysis.project.market;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class ChannelBehaviorKeySelector implements KeySelector<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Integer> value)
        throws Exception {
        return value.f0;
    }
}
