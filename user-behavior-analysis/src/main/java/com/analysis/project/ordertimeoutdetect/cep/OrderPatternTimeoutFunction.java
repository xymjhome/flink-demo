package com.analysis.project.ordertimeoutdetect.cep;


import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.OrderResult;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.PatternTimeoutFunction;

public class OrderPatternTimeoutFunction implements
    PatternTimeoutFunction<OrderEvent, OrderResult> {

    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp)
        throws Exception {
        List<OrderEvent> begin = pattern.get("begin");
        OrderEvent createOrder = begin.get(0);
        return new OrderResult(createOrder.getOrderId(), createOrder.getEventType(), "timeout");
    }
}
