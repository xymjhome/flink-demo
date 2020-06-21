package com.analysis.project.networkflow;


import com.analysis.project.pojo.ApacheLogEvent;
import com.analysis.project.pojo.UrlViewCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

//每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL。
public class NetworkFlow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String path = NetworkFlow.class.getClassLoader().getResource("apache.txt").getPath();
        SingleOutputStreamOperator<ApacheLogEvent> apacheLogSource = env
            .readTextFile(path)
            .map(new ApacheLogParse())
            .filter(data -> null != data)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getEventTime();
                    }
                });

        SingleOutputStreamOperator<UrlViewCount> urlViewCountDataStream = apacheLogSource
            .keyBy(item -> item.getUrl())
            .timeWindow(Time.seconds(10), Time.milliseconds(5))
            .aggregate(new UrlCountAgg(), new UrlViewCountResult());

        //urlViewCountDataStream.filter(item -> null == item).printToErr();

        SingleOutputStreamOperator<String> result = urlViewCountDataStream
            .keyBy(item -> item.getWindowEnd())
            .process(new TopNHotUrls(5));

        result.printToErr();

        env.execute("network flow job");

    }
}
