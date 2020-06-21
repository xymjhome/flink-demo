package com.analysis.project.uniquevisitor;


import com.analysis.project.hotitems.kafkasource.UserBehaviorMapFunction;
import com.analysis.project.pojo.UserBehavior;
import com.analysis.project.pojo.UserUVCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UvWithBloomFilterCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String path = UniqueVisitorCount.class.getClassLoader().getResource("UserBehavior.csv")
            .getPath();

        SingleOutputStreamOperator<UserBehavior> dataSource = env.readTextFile(path)
            .map(new UserBehaviorMapFunction()).filter(behaior -> null != behaior)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior element) {
                    return element.getTimestamp() * 1000;
                }
            }).filter(behavior -> "pv".equals(behavior.getBehavior()));
        //dataSource.printToErr();
        KeyedStream<Tuple2<String, Long>, String> keyedStream = dataSource
            .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                    return new Tuple2<>("allUserIdKey", value.getUserId());
                }
            }).keyBy(data -> data.f0);
        //keyedStream.printToErr();

        SingleOutputStreamOperator<UserUVCount> result = keyedStream
            .timeWindow(Time.hours(1))
            .trigger(new WindowTrigger())
            .process(new UvCountWithBloom());

        result.printToErr();
        System.out.println("end end end");

        env.execute("unique visitor bloom count");
    }
}
