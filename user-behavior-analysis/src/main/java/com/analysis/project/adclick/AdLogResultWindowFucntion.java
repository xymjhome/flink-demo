package com.analysis.project.adclick;


import com.analysis.project.pojo.CountByProvince;
import com.analysis.project.pojo.CountByProvince.CountByProvinceBuilder;
import java.text.SimpleDateFormat;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AdLogResultWindowFucntion implements
    WindowFunction<Long, CountByProvince, String, TimeWindow> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Override
    public void apply(String key, TimeWindow window, Iterable<Long> input,
        Collector<CountByProvince> out) throws Exception {
        CountByProvinceBuilder provinceBuilder = CountByProvince.builder()
            .province(key)
            .count(input.iterator().next())
            .windowEnd(dateFormat.format(window.getEnd()));

        out.collect(provinceBuilder.build());
    }
}
