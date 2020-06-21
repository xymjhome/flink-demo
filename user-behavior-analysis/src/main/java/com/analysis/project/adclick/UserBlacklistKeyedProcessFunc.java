package com.analysis.project.adclick;

import com.analysis.project.pojo.AdClickLog;
import com.analysis.project.pojo.BlackListWarning;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class UserBlacklistKeyedProcessFunc extends
    KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {

    private int maxClkTimes;
    private OutputTag<BlackListWarning> blacklist;

    private ValueState<Integer> curClkTimes;
    private ValueState<Boolean> firstRecord;
    private ValueState<Long> registTime;

    public UserBlacklistKeyedProcessFunc(int maxClkTimes, OutputTag<BlackListWarning> blacklist) {
        this.maxClkTimes = maxClkTimes;
        this.blacklist = blacklist;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        curClkTimes = getRuntimeContext()
            .getState(new ValueStateDescriptor<Integer>("cur-clk-times", Integer.class));
        //curClkTimes.update(0);

        firstRecord = getRuntimeContext()
            .getState(new ValueStateDescriptor<Boolean>("first-record", Boolean.class));
        //firstRecord.update(false);


        registTime= getRuntimeContext()
            .getState(new ValueStateDescriptor<Long>("regist-time", Long.class));
        //registTime.update(0L);
    }



    @Override
    public void processElement(AdClickLog value, Context ctx, Collector<AdClickLog> out)
        throws Exception {
        Integer curClkCount = curClkTimes.value();
        if (curClkCount == null) {
            curClkCount = 0;
        }

        log.error("userid:" + value.getUserId() + " adId:" + value.getAdId() + " counts:" + curClkCount);

        if (curClkCount == 0) {
            long onedayMillis = 24 * 60 * 60 * 1000;
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            long registTs = (currentProcessingTime / onedayMillis + 1) * onedayMillis
                + (currentProcessingTime % onedayMillis);
            registTime.update(registTs);
            ctx.timerService().registerProcessingTimeTimer(registTs);
        }

        if (curClkCount >= maxClkTimes) {
            Boolean record = firstRecord.value();
            if (record == null) {
                record = false;
            }
            if (!record) {
                firstRecord.update(true);
                ctx.output(blacklist, BlackListWarning.builder()
                    .userId(value.getUserId())
                    .adId(value.getAdId())
                    .msg("Click over " + maxClkTimes + " times today.")
                    .build());
            }

            return;
        }

        int counts = curClkCount + 1;
        curClkTimes.update(counts);
        out.collect(value);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickLog> out)
        throws Exception {
        if (timestamp == registTime.value()) {
            registTime.clear();
            curClkTimes.clear();
            firstRecord.clear();
        }
    }

}
