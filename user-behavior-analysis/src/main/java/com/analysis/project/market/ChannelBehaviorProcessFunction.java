package com.analysis.project.market;


import com.google.common.collect.Lists;
import com.analysis.project.pojo.MarketingCountView;
import com.analysis.project.pojo.MarketingCountView.MarketingCountViewBuilder;
import java.sql.Timestamp;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ChannelBehaviorProcessFunction extends
    ProcessWindowFunction<Tuple2<Tuple2<String, String>, Integer>, MarketingCountView, Tuple2<String, String>, TimeWindow> {

    @Override
    public void process(Tuple2<String, String> key, Context context,
        Iterable<Tuple2<Tuple2<String, String>, Integer>> elements,
        Collector<MarketingCountView> out) throws Exception {

        MarketingCountViewBuilder countViewBuilder = MarketingCountView.builder()
            .channel(key.f0)
            .behavior(key.f1)
            .count(Lists.newArrayList(elements).size())
            .windowStart(new Timestamp(context.window().getStart()))
            .windowEnd(new Timestamp(context.window().getEnd()));

        out.collect(countViewBuilder.build());
    }
}
