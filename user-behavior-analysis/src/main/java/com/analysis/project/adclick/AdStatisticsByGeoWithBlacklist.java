package com.analysis.project.adclick;


import com.analysis.project.pojo.AdClickLog;
import com.analysis.project.pojo.BlackListWarning;
import com.analysis.project.pojo.CountByProvince;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class AdStatisticsByGeoWithBlacklist {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String path = AdStatisticsByGeo.class.getClassLoader().getResource("AdClickLog.csv")
            .getPath();

        SingleOutputStreamOperator<AdClickLog> dataSource = env
            .readTextFile(path).filter(data -> StringUtils.isNotBlank(data))
            .map(new AdClickLogMapFunction()).filter(adlog -> null != adlog)
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickLog>() {
                @Override
                public long extractAscendingTimestamp(AdClickLog element) {
                    return element.getTimestamp() * 1000;
                }
            });
        //dataSource.printToErr();

        OutputTag<BlackListWarning> blacklist = new OutputTag<BlackListWarning>("blacklist"){};
        SingleOutputStreamOperator<AdClickLog> blacklistFilter = dataSource
            .keyBy(new UserAndAdIdKeySelector())
            .process(new UserBlacklistKeyedProcessFunc(100, blacklist));
        blacklistFilter.getSideOutput(blacklist).printToErr("blacklist");

        SingleOutputStreamOperator<CountByProvince> result = blacklistFilter
            .keyBy(adlog -> adlog.getProvince())
            .timeWindow(Time.hours(1), Time.seconds(5))
            .aggregate(new AdLogProvinceCountAgg(), new AdLogResultWindowFucntion());

        result.print();

        env.execute("ad log statistic with blacklist job");
    }
}


