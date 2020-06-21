package com.analysis.project.adclick;


import com.analysis.project.pojo.AdClickLog;
import com.analysis.project.pojo.AdClickLog.AdClickLogBuilder;
import org.apache.flink.api.common.functions.MapFunction;

public class AdClickLogMapFunction implements
    MapFunction<String, AdClickLog> {

    @Override
    public AdClickLog map(String value) throws Exception {
        String[] datas = value.split(",");
        if (datas.length != 5) {
            return null;
        }
        AdClickLogBuilder clickLogBuilder = AdClickLog.builder()
            .userId(Long.parseLong(datas[0].trim()))
            .adId(Long.parseLong(datas[1].trim()))
            .province(datas[2].trim())
            .city(datas[3].trim())
            .timestamp(Long.parseLong(datas[4].trim()));
        return clickLogBuilder.build();
    }
}
