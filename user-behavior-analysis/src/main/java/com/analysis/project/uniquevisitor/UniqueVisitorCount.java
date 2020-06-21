package com.analysis.project.uniquevisitor;


import com.analysis.project.hotitems.kafkasource.UserBehaviorMapFunction;
import com.analysis.project.pojo.UserBehavior;
import com.analysis.project.pojo.UserUVCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UniqueVisitorCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String path = UniqueVisitorCount.class.getClassLoader().getResource("UserBehavior.csv")
            .getPath();

        SingleOutputStreamOperator<UserUVCount> uVCount = env.readTextFile(path)
            .map(new UserBehaviorMapFunction())
            .filter(behavior -> null != behavior)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior element) {
                    return element.getTimestamp() * 1000;
                }
            }).filter(behavior -> "pv".equals(behavior.getBehavior()))
            .timeWindowAll(Time.hours(1))
            .apply(new UserUVAllWindowFunction());

        uVCount.printToErr();

        env.execute("unique visitor count");


    }
}
