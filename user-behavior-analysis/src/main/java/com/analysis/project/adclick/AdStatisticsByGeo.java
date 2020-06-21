package com.analysis.project.adclick;


import com.analysis.project.pojo.AdClickLog;
import com.analysis.project.pojo.CountByProvince;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

//进行行页面广告按照省份划分的点击量的统计。
//开一小时的时间窗口，滑动距离为 5 秒，统计窗口内的点击事件数量
public class AdStatisticsByGeo {

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

        SingleOutputStreamOperator<CountByProvince> result = dataSource
            .keyBy(adlog -> adlog.getProvince())
            .timeWindow(Time.hours(1), Time.seconds(5))
            .aggregate(new AdLogProvinceCountAgg(), new AdLogResultWindowFucntion());

        result.printToErr();

        env.execute("ad log statistic job");

    }
}
