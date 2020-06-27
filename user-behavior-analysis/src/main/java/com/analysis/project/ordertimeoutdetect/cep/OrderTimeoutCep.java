package com.analysis.project.ordertimeoutdetect.cep;


import com.analysis.project.loginfaildetect.state.LoginFailWithState;
import com.analysis.project.ordertimeoutdetect.OrderEventParse;
import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class OrderTimeoutCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String path = LoginFailWithState.class.getClassLoader().getResource("OrderLog.csv")
            .getPath();
        SingleOutputStreamOperator<OrderEvent> dataSource = env.readTextFile(path)
            .map(new OrderEventParse()).filter(event -> null != event)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                @Override
                public long extractAscendingTimestamp(OrderEvent element) {
                    return element.getEventTime() * 1000;
                }
            });

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("begin")
            .where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "create".equals(value.getEventType());
                }
            }).followedBy("follow").where(new SimpleCondition<OrderEvent>() {
                @Override
                public boolean filter(OrderEvent value) throws Exception {
                    return "pay".equals(value.getEventType());
                }
            }).within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP
            .pattern(dataSource.keyBy(order -> order.getOrderId()), pattern);

        OutputTag<OrderResult> orderTimeoutOutput = new OutputTag<OrderResult>("orderTimeout") {
        };
        SingleOutputStreamOperator<OrderResult> result = patternStream
            .select(orderTimeoutOutput, new OrderPatternTimeoutFunction(),
                new OrderPatternSelectFunction());

        result.getSideOutput(orderTimeoutOutput).printToErr();
        result.printToErr();

        env.execute("order time detect job");

    }
}
