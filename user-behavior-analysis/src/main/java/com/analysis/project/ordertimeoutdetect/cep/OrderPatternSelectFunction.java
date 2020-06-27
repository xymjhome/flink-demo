package com.analysis.project.ordertimeoutdetect.cep;


import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.OrderResult;
import java.util.List;
import java.util.Map;
import org.apache.flink.cep.PatternSelectFunction;

public class OrderPatternSelectFunction implements PatternSelectFunction<OrderEvent, OrderResult> {

    @Override
    public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
        List<OrderEvent> follow = pattern.get("follow");
        OrderEvent payOrder = follow.get(0);
        return new OrderResult(payOrder.getOrderId(), payOrder.getEventType(), "sucess");
    }
}
