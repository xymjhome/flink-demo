package com.analysis.project.networkflow;


import com.google.common.collect.Lists;
import com.analysis.project.pojo.UrlViewCount;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class TopNHotUrls extends KeyedProcessFunction<Long, UrlViewCount, String> {

    private int topN;
    private ListState<UrlViewCount> urlViewCountListState;

    public TopNHotUrls(int topN) {
        this.topN = topN;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        urlViewCountListState = getRuntimeContext()
            .getListState(new ListStateDescriptor<UrlViewCount>("url_view_count", UrlViewCount.class));
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out)
        throws Exception {
        urlViewCountListState.add(value);

        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
        throws Exception {
        List<UrlViewCount> topUrls = Lists.newArrayList(urlViewCountListState.get())
            .stream().sorted(Comparator.comparingLong(UrlViewCount::getCount).reversed())
            .limit(topN).collect(Collectors.toList());

        urlViewCountListState.clear();

        StringBuilder result = new StringBuilder();
        result.append(StringUtils.repeat("*", 50)).append("\n");
        result.append(" Time:" + new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i <topUrls.size(); i++) {
            result.append(" No:" + (i + 1));
            result.append(" Count:" + topUrls.get(i).getCount());
            result.append(" Url:" + topUrls.get(i).getUrl()).append("\n");
        }
        result.append(StringUtils.repeat("*", 50)).append("\n");

        out.collect(result.toString());

    }
}
