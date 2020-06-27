package com.analysis.project.txmatch;

import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TxMatchProcessFunction extends
    CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

    private OutputTag<OrderEvent> unOrderEventOutputTag;
    private OutputTag<ReceiptEvent> unReceiptEventOutputTag;

    private ValueState<OrderEvent> orderEventValueState;
    private ValueState<ReceiptEvent> receiptEventValueState;

    public TxMatchProcessFunction(OutputTag<OrderEvent> unOrderEventOutputTag,
        OutputTag<ReceiptEvent> unReceiptEventOutputTag) {
        this.unOrderEventOutputTag = unOrderEventOutputTag;
        this.unReceiptEventOutputTag = unReceiptEventOutputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        orderEventValueState = getRuntimeContext()
            .getState(new ValueStateDescriptor<OrderEvent>("order-event-state", OrderEvent.class));
        receiptEventValueState = getRuntimeContext()
            .getState(new ValueStateDescriptor<ReceiptEvent>("receipt-event-state", ReceiptEvent.class));
    }

    @Override
    public void processElement1(OrderEvent value, Context ctx,
        Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        ReceiptEvent receiptEvent = receiptEventValueState.value();
        if (null != receiptEvent) {
            receiptEventValueState.clear();
            out.collect(new Tuple2<>(value, receiptEvent));
        } else {
            orderEventValueState.update(value);

            ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000);
        }
    }

    @Override
    public void processElement2(ReceiptEvent value, Context ctx,
        Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        OrderEvent orderEvent = orderEventValueState.value();

        if (null != orderEvent) {
            orderEventValueState.clear();
            out.collect(new Tuple2<>(orderEvent, value));
        } else {
            receiptEventValueState.update(value);

            ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000);
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
        Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
        if (orderEventValueState.value() != null) {
            ctx.output(unOrderEventOutputTag, orderEventValueState.value());
        }

        if (receiptEventValueState.value() != null) {
            ctx.output(unReceiptEventOutputTag, receiptEventValueState.value());
        }

        orderEventValueState.clear();
        receiptEventValueState.clear();
    }
}
