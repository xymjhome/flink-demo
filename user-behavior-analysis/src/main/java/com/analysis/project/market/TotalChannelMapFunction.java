package com.analysis.project.market;

import com.analysis.project.pojo.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class TotalChannelMapFunction implements MapFunction<MarketingUserBehavior, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
        return new Tuple2<>("dummyKey", 1);
    }
}
