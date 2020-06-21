package com.analysis.project.market;


import com.analysis.project.pojo.MarketingCountView;
import com.analysis.project.pojo.MarketingUserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

//不区分分渠道和行为的市场推广统计，即一个小时内所有记录
public class AppMarketingStatistics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<MarketingUserBehavior> dataSource = env
            .addSource(new MarketingUserBehaviorSimulatedSource())
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                }).filter(record -> !"UNINSTALL".equals(record.getBehavior()));

        SingleOutputStreamOperator<MarketingCountView> result = dataSource
            .map(new TotalChannelMapFunction())
            .keyBy(record -> record.f0)
            .timeWindow(Time.hours(1), Time.seconds(10))
            .process(new TotalChannelProcessFunction());

        result.printToErr();

        env.execute("total count job");


    }
}
