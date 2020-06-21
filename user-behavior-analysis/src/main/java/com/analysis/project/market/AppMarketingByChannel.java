package com.analysis.project.market;


import com.analysis.project.pojo.MarketingCountView;
import com.analysis.project.pojo.MarketingUserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

//分渠道和行为的市场推广统计
public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<MarketingUserBehavior> source = env
            .addSource(new MarketingUserBehaviorSimulatedSource());

        SingleOutputStreamOperator<Tuple2<Tuple2<String, String>, Integer>> dataStream = source
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp();
                    }
                }).filter(behavior -> !"UNINSTALL".equals(behavior.getBehavior()))
            .map(new ChannelBehaviorMapFunction());

        SingleOutputStreamOperator<MarketingCountView> result = dataStream.keyBy(new ChannelBehaviorKeySelector())
            .timeWindow(Time.minutes(30), Time.seconds(10))
            .process(new ChannelBehaviorProcessFunction());

        result.printToErr();

        env.execute("channel behavior count job");
    }

}
