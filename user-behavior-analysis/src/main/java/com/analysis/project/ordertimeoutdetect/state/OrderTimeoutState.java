package com.analysis.project.ordertimeoutdetect.state;


import com.analysis.project.loginfaildetect.state.LoginFailWithState;
import com.analysis.project.ordertimeoutdetect.OrderEventParse;
import com.analysis.project.ordertimeoutdetect.cep.OrderPatternSelectFunction;
import com.analysis.project.ordertimeoutdetect.cep.OrderPatternTimeoutFunction;
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

public class OrderTimeoutState {

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

        //dataSource.printToErr();

        SingleOutputStreamOperator<OrderResult> timeoutResult = dataSource
            .keyBy(order -> order.getOrderId())
            .process(new OrderTimeoutAlert());

        timeoutResult.printToErr();
//756  747
        env.execute("order time detect job");

    }
}
