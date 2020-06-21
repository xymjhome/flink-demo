package com.analysis.project.hotitems.filesource;


import com.analysis.project.pojo.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out)
        throws Exception {
        Long itemId = (Long) ((Tuple1)key).f0; //key.getField(0)
        Long count = input.iterator().next();
        long windowEnd = window.getEnd();
        out.collect(ItemViewCount.builder().itemId(itemId).viewCount(count).windowEnd(windowEnd).build());
    }
}
