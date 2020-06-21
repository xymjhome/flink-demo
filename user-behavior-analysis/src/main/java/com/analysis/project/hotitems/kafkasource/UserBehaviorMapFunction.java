package com.analysis.project.hotitems.kafkasource;


import com.analysis.project.pojo.UserBehavior;
import com.analysis.project.pojo.UserBehavior.UserBehaviorBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class UserBehaviorMapFunction implements MapFunction<String, UserBehavior> {

    @Override
    public UserBehavior map(String value) throws Exception {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        String[] datas = value.split(",");
        if (datas.length != 5) {
            return null;
        }
        UserBehaviorBuilder behaviorBuilder = UserBehavior.builder()
            .userId(Long.parseLong(datas[0].trim()))
            .itemId(Long.parseLong(datas[1].trim()))
            .categoryId(Integer.parseInt(datas[2].trim()))
            .behavior(datas[3].trim())
            .timestamp(Long.parseLong(datas[4].trim()));
        return behaviorBuilder.build();
    }
}