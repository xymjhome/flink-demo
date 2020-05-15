package com.project.realtimehotgoods;


import com.project.pojo.ItemViewCount;
import com.project.pojo.UserBehavior;
import java.io.File;
import java.net.URL;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class HotGoogsCompute {

    public static void main(String[] args) throws Exception {
        URL resource = HotGoogsCompute.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(resource.toURI()));
        PojoTypeInfo<UserBehavior> typeInfo = (PojoTypeInfo)TypeExtractor.createTypeInfo(UserBehavior.class);
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInputFormat = new PojoCsvInputFormat<>(
            filePath, typeInfo, fieldOrder);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        //告诉 Flink 我们现在按照 EventTime 模式进行处理，Flink 默认使用 ProcessingTime 处理
        //如果不设置，会导致timeWindow出问题，不会根据文件中的timestamp做聚合，具体原因得再深究
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<UserBehavior> dataStreamSource = env.createInput(csvInputFormat, typeInfo);
        //dataStreamSource.printToErr();

        SingleOutputStreamOperator<UserBehavior> timeData = dataStreamSource
            .assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000;
                    }
                });
        //timeData.printToErr();

        SingleOutputStreamOperator<UserBehavior> pvData = timeData
            .filter(new FilterFunction<UserBehavior>() {
                @Override
                public boolean filter(UserBehavior value) throws Exception {
                    return "pv".equals(value.getBehavior());
                }
            });
        //pvData.printToErr();

//        SingleOutputStreamOperator<UserBehavior> windowedData = pvData.keyBy("itemId")
//            .timeWindow(Time.minutes(60), Time.minutes(5))
//            .reduce(new ReduceFunction<UserBehavior>() {
//                @Override
//                public UserBehavior reduce(UserBehavior value1, UserBehavior value2)
//                    throws Exception {
//                    value1.setBehavior(value1.getBehavior() + " --> " + value2.getBehavior());
//                    return value1;
//                }
//            });
        SingleOutputStreamOperator<ItemViewCount> windowedData = pvData.keyBy("itemId")
            .timeWindow(Time.minutes(60), Time.minutes(5))
            .aggregate(new CountAggregateFunction(), new WindowResultFunction());

        //windowedData.printToErr();

        SingleOutputStreamOperator<String> topNItemsData = windowedData.keyBy("windowEnd")
            .process(new TopNItemsProcessFunction(3));

        topNItemsData.printToErr();

        env.execute("hot topN items job");

    }
}
