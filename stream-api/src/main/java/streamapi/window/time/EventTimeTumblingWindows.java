package streamapi.window.time;


import com.google.common.collect.Lists;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streamapi.util.ParseSourceDataUtil;

@Slf4j
public class EventTimeTumblingWindows {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //全局设置并行度为1，便于查看生成的watermark和window
        env.setParallelism(1);

        /**
         * //1
         * 001,1590732662000
         * 001,1590732666000
         * 001,1590732672000
         * 001,1590732673000
         *
         * //2
         * 001,1590732674000
         * 001,1590732676000
         * 001,1590732677000
         * 001,1590732679000
         *
         * //3
         * 001,1590732671000
         * 001,1590732673000
         * 001,1590732683000
         *
         * //4
         * 001,1590732672000
         * 001,1590732783000
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> sourceData = ParseSourceDataUtil.getTuple2SocketSource(env);

        env.getConfig().setAutoWatermarkInterval(10000);

        SingleOutputStreamOperator<Tuple2<String, Long>> sourceWatermark = sourceData
            .assignTimestampsAndWatermarks(new BoundedAssigner(Time.seconds(10)));
        //sourceWatermark.printToErr();

        /**
         *  window配合watermark
         *  触发window执行计算的时机：
         *      watermark时间 >= window_end_time，并且在[window_start_time,window_end_time)中有数据存在
         *
         *
         * window的触发机制，是先按照自然时间将window划分，如果window大小是3秒，那么1分钟内会把window划分为如下的形式:
         * [00:00:00,00:00:03)
         * [00:00:03,00:00:06)
         * ...
         * [00:00:57,00:01:00)
         *
         * 如果window大小是10秒，则window会被分为如下的形式：
         * [00:00:00,00:00:10)
         * [00:00:10,00:00:20)
         * ...
         * [00:00:50,00:01:00)
         * window的设定无关数据本身，而是系统定义好了的。
         */
        SingleOutputStreamOperator<Tuple6<String, Integer,
            String, String, String, String>> window = sourceWatermark
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(3)))
            .apply(
                new WindowFunction<Tuple2<String, Long>, Tuple6<String, Integer, String, String, String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window,
                        Iterable<Tuple2<String, Long>> input,
                        Collector<Tuple6<String, Integer, String, String, String, String>> out)
                        throws Exception {
                        ArrayList<Tuple2<String, Long>> windowDatas = Lists.newArrayList(input);
                        String key = tuple.getField(0);
                        int dataSize = windowDatas.size();
                        long firstTime = windowDatas.get(0).f1;
                        long lastTime = windowDatas.get(dataSize - 1).f1;
                        long windowStart = window.getStart();
                        long windowEnd = window.getEnd();
                        out.collect(new Tuple6<>(key,
                            dataSize,
                            firstTime + "[" + sdf.format(firstTime) + "]",
                            lastTime + "[" + sdf.format(lastTime) + "]",
                            windowStart + "[" + sdf.format(windowStart) + "]",
                            windowEnd + "[" + sdf.format(windowEnd) + "]"));
                    }
                });

        window.printToErr();

        env.execute("event time tumbling windows");

    }

    public static class BoundedAssigner extends
        BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>> {

        public BoundedAssigner(
            Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element) {
            Watermark currentWatermark = super.getCurrentWatermark();
            log.error("timestamp:" + element.f0 + "  "
                + element.f1 + "[" + sdf.format(element.f1) + "]"
                + "  " + currentWatermark.toString()
                + "[" + sdf.format(currentWatermark.getTimestamp()) + "]");
            return element.f1;
        }
    }
}
