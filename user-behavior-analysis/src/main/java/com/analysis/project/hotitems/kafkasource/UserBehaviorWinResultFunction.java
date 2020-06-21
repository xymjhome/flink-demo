package com.analysis.project.hotitems.kafkasource;


import com.analysis.project.pojo.ItemViewCount;
import com.analysis.project.pojo.ItemViewCount.ItemViewCountBuilder;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserBehaviorWinResultFunction  implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    @Override
    public void apply(Long key, TimeWindow window, Iterable<Long> input,
        Collector<ItemViewCount> out) throws Exception {
        ItemViewCountBuilder viewCountBuilder = ItemViewCount.builder()
            .itemId(key)
            .viewCount(input.iterator().next())
            .windowEnd(window.getEnd());

        out.collect(viewCountBuilder.build());
    }
}
