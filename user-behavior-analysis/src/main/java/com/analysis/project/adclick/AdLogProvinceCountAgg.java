package com.analysis.project.adclick;


import com.analysis.project.pojo.AdClickLog;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AdLogProvinceCountAgg implements AggregateFunction<AdClickLog, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickLog value, Long accumulator) {
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
