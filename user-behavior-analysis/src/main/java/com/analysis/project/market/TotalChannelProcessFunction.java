package com.analysis.project.market;


import com.google.common.collect.Lists;
import com.analysis.project.pojo.MarketingCountView;
import com.analysis.project.pojo.MarketingCountView.MarketingCountViewBuilder;
import java.sql.Timestamp;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TotalChannelProcessFunction extends
    ProcessWindowFunction<Tuple2<String, Integer>, MarketingCountView, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements,
        Collector<MarketingCountView> out) throws Exception {

        MarketingCountViewBuilder countViewBuilder = MarketingCountView.builder()
            .channel("total")
            .behavior("total")
            .count(Lists.newArrayList(elements).size())
            .windowStart(new Timestamp(context.window().getStart()))
            .windowEnd(new Timestamp(context.window().getEnd()));

        out.collect(countViewBuilder.build());

    }
}
