package com.analysis.project.hotitems.kafkasource;

import com.analysis.project.pojo.ItemViewCount;
import com.google.common.collect.Lists;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UserBehaviorTopItemsProcessFunction extends
    KeyedProcessFunction<Long, ItemViewCount, String> {

    private int topN;
    private ListState<ItemViewCount> itemViewCountListState = null;

    public UserBehaviorTopItemsProcessFunction(int topN) {
        this.topN = topN;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        itemViewCountListState = getRuntimeContext().getListState(
            new ListStateDescriptor<ItemViewCount>("item_view_count", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out)
        throws Exception {
        itemViewCountListState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
        throws Exception {
        List<ItemViewCount> topItems = Lists.newArrayList(itemViewCountListState.get()).stream()
            .sorted(Comparator.comparingLong(ItemViewCount::getViewCount).reversed())
            .limit(topN).collect(Collectors.toList());

        itemViewCountListState.clear();

        StringBuilder result = new StringBuilder();
        result.append(StringUtils.repeat("*", 50)).append("\n");
        result.append("Time:").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < topItems.size(); i++) {
            result.append("No:" + (i + 1)).append("-->");
            result.append("ItemId:" + topItems.get(i).getItemId()).append("-->");
            result.append("Count:" + topItems.get(i).getViewCount()).append("\n");
        }
        result.append(StringUtils.repeat("*", 50)).append("\n");

        out.collect(result.toString());
    }
}
