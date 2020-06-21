package com.analysis.project.hotitems.kafkasource;


import com.analysis.project.pojo.ItemViewCount;
import com.analysis.project.pojo.UserBehavior;
import com.analysis.project.hotitems.filesource.CountAggregateFunction;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaSourceHotGoodsCompute {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "hot-items-test-group0");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "hot-items-test-client0");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("user_behavior",
            new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        DataStreamSource<String> source = env.addSource(consumer);
//        SingleOutputStreamOperator<String> map = source.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                return value;
//            }
//        });
//        map.printToErr();


        SingleOutputStreamOperator<UserBehavior> behaviorDataSource = source.map(new UserBehaviorMapFunction())
            .filter(behavior -> null != behavior)
            .filter(behavior -> "pv".equals(behavior.getBehavior()))
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior element) {
                    return element.getTimestamp() * 1000;
                }
            });
        //behaviorDataSource.printToErr();

        SingleOutputStreamOperator<ItemViewCount> itemViewCountDataStream = behaviorDataSource
            .keyBy(behavior -> behavior.getItemId())
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAggregateFunction(), new UserBehaviorWinResultFunction());

        SingleOutputStreamOperator<String> result = itemViewCountDataStream
            .keyBy(item -> item.getWindowEnd())
            .process(new UserBehaviorTopItemsProcessFunction(3));

        result.printToErr();

        env.execute("hot items kafka job");

    }
}
