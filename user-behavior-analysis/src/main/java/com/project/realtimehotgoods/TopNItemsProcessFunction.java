package com.project.realtimehotgoods;


import com.google.common.collect.Lists;
import com.project.pojo.ItemViewCount;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
public class TopNItemsProcessFunction extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private int topN;

    // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<ItemViewCount> itemViewCountListState;

    public TopNItemsProcessFunction(int topN) {
        this.topN = topN;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemListState = new ListStateDescriptor<>("ItemState",
            ItemViewCount.class);
        itemViewCountListState = getRuntimeContext().getListState(itemListState);
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out)
        throws Exception {

        // 每条数据都保存到状态中
        itemViewCountListState.add(value);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
        throws Exception {
        List<ItemViewCount> itemViewCounts = Lists.newArrayList();
        for (ItemViewCount itemViewCount : itemViewCountListState.get()) {
            itemViewCounts.add(itemViewCount);
        }

        List<ItemViewCount> countList = itemViewCounts.stream()
            .sorted(Comparator.comparingLong(ItemViewCount::getViewCount).reversed())
            .limit(topN).collect(Collectors.toList());


        StringBuilder result = new StringBuilder();
        final int[] i = {1};
        result.append(StringUtils.repeat("*", 50)).append("\n");
        result.append("Time:").append(new Timestamp(timestamp - 1)).append("\n");
        countList.forEach(itemViewCount -> {
            result.append("No:" + i[0]).append("-->");
            result.append("ItemId:" + itemViewCount.getItemId()).append("-->");
            result.append("Count:" + itemViewCount.getViewCount()).append("\n");
            i[0]++;
        });
        result.append(StringUtils.repeat("*", 50)).append("\n");

        out.collect(result.toString());
    }
}
