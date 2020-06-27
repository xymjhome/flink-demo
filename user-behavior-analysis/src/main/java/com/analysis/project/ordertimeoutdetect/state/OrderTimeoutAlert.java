package com.analysis.project.ordertimeoutdetect.state;


import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.OrderResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class OrderTimeoutAlert extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

    private ValueState<Boolean> isPayed;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isPayed = getRuntimeContext()
            .getState(new ValueStateDescriptor<Boolean>("payed", Boolean.class));
    }

    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out)
        throws Exception {
        boolean isPayedValue = false;
        if (null != isPayed.value()) {
            isPayedValue = isPayed.value();
        }

        if (ctx.getCurrentKey() == 34756) {
            log.error("34756 isPayedValue:" + isPayedValue + " : " + value.getEventTime());
        }

        if ("create".equals(value.getEventType()) && !isPayedValue) {
            if (ctx.getCurrentKey() == 34756) {
                log.error("34756 isPayedValue create:" + isPayedValue + " : " + value.getEventTime());
            }
            ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000 + 1 );
        } else if ("pay".equals(value.getEventType())) {
            if (ctx.getCurrentKey() == 34756) {
                log.error("34756 isPayedValue update true:" + isPayedValue + " : " + value.getEventTime());
            }
            isPayed.update(true);
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out)
        throws Exception {
        boolean isPayedValue = false;
        if (null != isPayed.value()) {
            isPayedValue = isPayed.value();
        }

        if (ctx.getCurrentKey() == 34756) {
            log.error("timestamp:" + timestamp);
            log.error("ontimer 34756 isPayedValue:" + isPayedValue);
        }

        if (!isPayedValue) {
            out.collect(new OrderResult(ctx.getCurrentKey(), String.valueOf(timestamp), "order timeout"));
        }

        //out.collect(new OrderResult(ctx.getCurrentKey(), String.valueOf(timestamp), "order timeout"));
        isPayed.clear();
    }
}
