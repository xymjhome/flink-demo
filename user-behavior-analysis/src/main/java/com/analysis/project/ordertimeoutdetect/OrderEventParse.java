package com.analysis.project.ordertimeoutdetect;


import com.analysis.project.pojo.LoginEvent;
import com.analysis.project.pojo.LoginEvent.LoginEventBuilder;
import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.OrderEvent.OrderEventBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class OrderEventParse implements MapFunction<String, OrderEvent> {

    @Override
    public OrderEvent map(String value) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        String[] datas = value.split(",");
        if (datas.length != 4) {
            return null;
        }

        OrderEventBuilder orderEventBuilder = OrderEvent.builder().orderId(Long.parseLong(datas[0]))
            .eventType(datas[1]).txId(datas[2]).eventTime(Long.parseLong(datas[3]));

        return orderEventBuilder.build();
    }
}
