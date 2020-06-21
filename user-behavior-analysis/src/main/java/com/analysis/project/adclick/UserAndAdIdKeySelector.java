package com.analysis.project.adclick;


import com.analysis.project.pojo.AdClickLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class UserAndAdIdKeySelector implements KeySelector<AdClickLog, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
        return new Tuple2<>(value.getUserId(), value.getAdId());
    }
}
