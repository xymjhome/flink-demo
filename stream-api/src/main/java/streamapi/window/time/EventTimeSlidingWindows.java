package streamapi.window.time;


import com.google.common.collect.Lists;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streamapi.util.ParseSourceDataUtil;

@Slf4j
public class EventTimeSlidingWindows {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //全局设置并行度为1，便于查看生成的watermark和window
        env.setParallelism(1);


        SingleOutputStreamOperator<Tuple2<String, Long>> sourceData = ParseSourceDataUtil.getTuple2SocketSource(env);

        env.getConfig().setAutoWatermarkInterval(5000);
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = sourceData
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(
                    Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element) {
                        Watermark currentWatermark = super.getCurrentWatermark();
                        log.error("timestamp:" + element.f0 + "  "
                            + element.f1 + "[" + sdf.format(element.f1) + "]"
                            + "  " + currentWatermark.toString()
                            + "[" + sdf.format(currentWatermark.getTimestamp()) + "]");
                        return element.f1;
                    }
                });

        /**
         * //1
         * 001,1590732662000
         * 001,1590732666000
         * 001,1590732672000
         * 001,1590732673000
         * 001,1590732674000
         * 001,1590732674000
         *
         * //2
         * 001,1590732675000
         * 001,1590732676000
         * 001,1590732677000
         *
         * //3
         * 001,1590732678000
         * 001,1590732679000
         * 001,1590732680000
         *
         * //4
         * 001,1590732681000
         * 001,1590732682000
         * 001,1590732683000
         *
         * //5
         * 001,1590732671000
         * 001,1590732673000
         * 001,1590732683000
         * 001,1590732672000
         * 001,1590732783000
         *
         * 窗口的触发机制同TumblingEventTimeWindows
         *
         * window配合watermark
         * 触发window执行计算的时机：
         *     watermark时间 >= window_end_time，并且在[window_start_time,window_end_time)中有数据存在
         */
        SingleOutputStreamOperator<Tuple7<String, Integer, String, String, String, String, String>> window = watermarks
            .keyBy(0)
            //初始化的窗口个数是10/3 = 3个，这三个初始化的窗口以第一个时机戳为基准进行计算，计算逻辑见EventTimeSlidingWindowsTest
            //后面的窗口会根据初始化的窗口时机戳进行滑动
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(3)))
            .apply(
                new WindowFunction<Tuple2<String, Long>, Tuple7<String, Integer, String,
                    String, String, String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window,
                        Iterable<Tuple2<String, Long>> input,
                        Collector<Tuple7<String, Integer, String, String, String, String, String>> out)
                        throws Exception {
                        ArrayList<Tuple2<String, Long>> windowDatas = Lists.newArrayList(input);
                        String key = tuple.getField(0);
                        String ids = windowDatas.stream().map(data -> data.f0)
                            .collect(Collectors.joining("_"));
                        int dataSize = windowDatas.size();
                        long firstTime = windowDatas.get(0).f1;
                        long lastTime = windowDatas.get(dataSize - 1).f1;
                        long windowStart = window.getStart();
                        long windowEnd = window.getEnd();
                        out.collect(new Tuple7<>(key,
                            dataSize,
                            ids,
                            firstTime + "[" + sdf.format(firstTime) + "]",
                            lastTime + "[" + sdf.format(lastTime) + "]",
                            windowStart + "[" + sdf.format(windowStart) + "]",
                            windowEnd + "[" + sdf.format(windowEnd) + "]"));
                    }
                });

        window.printToErr();

        env.execute("event time sliding windows");
    }




}
