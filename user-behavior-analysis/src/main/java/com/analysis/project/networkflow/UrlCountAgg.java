package com.analysis.project.networkflow;


import com.analysis.project.pojo.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UrlCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {


    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
