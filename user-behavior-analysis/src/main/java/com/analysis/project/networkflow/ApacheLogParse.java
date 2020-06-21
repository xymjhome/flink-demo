package com.analysis.project.networkflow;


import com.analysis.project.pojo.ApacheLogEvent;
import com.analysis.project.pojo.ApacheLogEvent.ApacheLogEventBuilder;
import java.text.SimpleDateFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class ApacheLogParse implements MapFunction<String, ApacheLogEvent> {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
    @Override
    public ApacheLogEvent map(String value) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }

        String[] datas = value.split(" ");
        if (datas.length != 7) {
            return null;
        }

        ApacheLogEventBuilder eventBuilder = ApacheLogEvent.builder().ip(datas[0].trim())
            .userId(datas[1].trim())
            .eventTime(sdf.parse(datas[3].trim()).getTime())
            .method(datas[5].trim())
            .url(datas[6].trim());

        return eventBuilder.build();
    }
}
