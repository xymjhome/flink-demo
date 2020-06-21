package com.analysis.project.market;


import com.analysis.project.pojo.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ChannelBehaviorMapFunction implements MapFunction<MarketingUserBehavior, Tuple2<Tuple2<String, String>, Integer>> {

    @Override
    public Tuple2<Tuple2<String, String>, Integer> map(MarketingUserBehavior value)
        throws Exception {
        return new Tuple2<>(new Tuple2<>(value.getChannel(), value.getBehavior()), 1);
    }
}
