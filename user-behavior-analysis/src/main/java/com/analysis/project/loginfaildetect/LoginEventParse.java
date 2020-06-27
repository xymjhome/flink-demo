package com.analysis.project.loginfaildetect;


import com.analysis.project.pojo.LoginEvent;
import com.analysis.project.pojo.LoginEvent.LoginEventBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class LoginEventParse implements MapFunction<String, LoginEvent> {

    @Override
    public LoginEvent map(String value) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        String[] datas = value.split(",");
        if (datas.length != 4) {
            return null;
        }

        LoginEventBuilder loginEventBuilder = LoginEvent.builder()
            .userId(Long.parseLong(datas[0]))
            .ip(datas[1])
            .eventType(datas[2])
            .eventTime(Long.parseLong(datas[3]));
        return loginEventBuilder.build();
    }
}
