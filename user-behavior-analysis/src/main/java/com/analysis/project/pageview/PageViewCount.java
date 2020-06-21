package com.analysis.project.pageview;

import com.analysis.project.hotitems.kafkasource.UserBehaviorMapFunction;
import com.analysis.project.pojo.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

//实时统计每小时内的网站 PV
public class PageViewCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String path = PageViewCount.class.getClassLoader().getResource("UserBehavior.csv")
            .getPath();

        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = env.readTextFile(path)
            .map(new UserBehaviorMapFunction())
            //此watermark添加的Timestamp会贯穿整个流的处理逻辑
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior element) {
                    return element.getTimestamp() * 1000;
                }
            })
            .filter(behavior -> null != behavior)
            .filter(behavior -> "pv".equals(behavior.getBehavior()))
            .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                    return new Tuple2<>("pv", 1);//即使重新封装的元素不包含时间戳概念，但是元素处理是StreamRecord，包含转换的Tuple的值，和对应的timestamp
                }
            })
            .keyBy(data -> data.f0)
            .timeWindow(Time.hours(1))
            .sum(1);

        pv.printToErr();

        env.execute("pv total job");
    }
}
