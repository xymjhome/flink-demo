package com.analysis.project.txmatch;


import com.analysis.project.loginfaildetect.state.LoginFailWithState;
import com.analysis.project.ordertimeoutdetect.OrderEventParse;
import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.ReceiptEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.OutputTag;

public class TxMatch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String path = LoginFailWithState.class.getClassLoader().getResource("OrderLog.csv")
            .getPath();
        KeyedStream<OrderEvent, String> orderEventStream = env.readTextFile(path)
            .map(new OrderEventParse()).filter(event -> null != event)
            .filter(event -> StringUtils.isNotBlank(event.getTxId()))
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                @Override
                public long extractAscendingTimestamp(OrderEvent element) {
                    return element.getEventTime() * 1000;
                }
            }).keyBy(event -> event.getTxId());


        String receiptPath = LoginFailWithState.class.getClassLoader().getResource("ReceiptLog.csv")
            .getPath();
        KeyedStream<ReceiptEvent, String> receiptEventStream = env.readTextFile(receiptPath)
            .map(new ReceiptEventParse()).filter(event -> null != event)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                @Override
                public long extractAscendingTimestamp(ReceiptEvent element) {
                    return element.getEventTime() * 1000;
                }
            }).keyBy(event -> event.getTxId());

        OutputTag<OrderEvent> orderEventOutputTag = new OutputTag<OrderEvent>("unmatched-pays") {
        };
        OutputTag<ReceiptEvent> receiptEventOutputTag = new OutputTag<ReceiptEvent>("unmatched-receipts") {
        };
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventStream
            .connect(receiptEventStream)
            .process(new TxMatchProcessFunction(orderEventOutputTag, receiptEventOutputTag));

        result.getSideOutput(orderEventOutputTag).printToErr("unmatched pays");
        result.getSideOutput(receiptEventOutputTag).printToErr("unmatched receipts");

        result.printToErr("all process");

        env.execute("txmatch job");
    }
}
