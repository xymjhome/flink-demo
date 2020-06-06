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
public class EventTimeSessionWindows {

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
         * 001,1590732674000
         * 001,1590732677000
         *
         * //2
         * 001,1590732678000
         *
         * //3
         * 001,1590732679000
         * 001,1590732680000
         * 001,1590732681000
         * 001,1590732682000
         * 001,1590732683000
         * 001,1590732684000
         * 001,1590732685000
         * 001,1590732686000
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
         *  相邻两次数据的 EventTime 的时间差超过指定的时间间隔就会触发执行。
         *  如果加入 Watermark， 会在符合窗口触发的情况下进行延迟。到达延迟水位再进行窗口触发。
         *
         *  session window 生成窗口策略:
         *      window_start_time: event_time
         *      window_end_time: 最后一个不在下一个会话周期内的event_time + gap
         */
        SingleOutputStreamOperator<Tuple6<String, Integer,
            String, String, String, String>> window = sourceWatermark
            .keyBy(0)
            .window(org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows.withGap(Time.milliseconds(2000)))
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

        env.execute("event time session windows");

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
