package com.analysis.project.txmatch;


import com.analysis.project.pojo.OrderEvent;
import com.analysis.project.pojo.OrderEvent.OrderEventBuilder;
import com.analysis.project.pojo.ReceiptEvent;
import com.analysis.project.pojo.ReceiptEvent.ReceiptEventBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class ReceiptEventParse implements MapFunction<String, ReceiptEvent> {

    @Override
    public ReceiptEvent map(String value) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        String[] datas = value.split(",");
        if (datas.length != 3) {
            return null;
        }
        ReceiptEventBuilder eventBuilder = ReceiptEvent.builder().txId(datas[0])
            .payChannel(datas[1]).eventTime(Long.parseLong(datas[2]));
        return eventBuilder.build();
    }
}
