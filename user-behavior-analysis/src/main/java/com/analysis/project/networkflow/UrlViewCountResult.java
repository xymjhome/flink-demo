package com.analysis.project.networkflow;


import com.analysis.project.pojo.UrlViewCount;
import com.analysis.project.pojo.UrlViewCount.UrlViewCountBuilder;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UrlViewCountResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<Long> input,
        Collector<UrlViewCount> out) throws Exception {
        UrlViewCountBuilder viewCountBuilder = UrlViewCount.builder().url(key)
            .count(input.iterator().next()).windowEnd(window.getEnd());

        out.collect(viewCountBuilder.build());
    }
}
