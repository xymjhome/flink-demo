package streamapi.window.time;


import java.text.SimpleDateFormat;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import streamapi.pojo.Sensor;
import streamapi.util.ParseSourceDataUtil;

@Slf4j
public class AssignWatermarks {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        //这么设置会使所有的operator、data source和data sink以2个并发数来运行，导致每个并行内部都有一个watermark
        env.setParallelism(2);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Sensor> source = ParseSourceDataUtil
            .getSensorSocketSource(env);

        //每隔5秒产生一个watermark,默认200ms,与PeriodicAssigner配合验证
        //env.getConfig().setAutoWatermarkInterval(5000);

        SingleOutputStreamOperator<Sensor> watermarks = source
            //.assignTimestampsAndWatermarks(new PeriodicAssigner());
            //.assignTimestampsAndWatermarks(new PunctuatedAssigner());
            //.assignTimestampsAndWatermarks(new AscendingAssigner());
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessAssigner(Time.seconds(5)));

        watermarks.printToErr();

        env.execute("assign watermarks");


    }

    /**
     * 周期性生成watermark 自定义时间戳抽取
     *
     * env.getConfig().setAutoWatermarkInterval(5000);产生 watermark 的逻辑：每隔 5 秒钟，Flink 会调用
     * AssignerWithPeriodicWatermarks 的 getCurrentWatermark()方法。如果方法返回一个 时间戳大于之前水位的时间戳，新的 watermark
     * 会被插入到流中。这个检查保证了 水位线是单调递增的。如果方法返回的时间戳小于等于之前水位的时间戳，则不会 产生新的 watermark。
     *
     * 如果不设置setAutoWatermarkInterval,默认周期是 200 毫秒。可以使用 ExecutionConfig.setAutoWatermarkInterval()方法进行设置。
     * 不设置看log可以发现其实对于每一个元素来后(200ms很短)，会根据之前元素的timestamp和延迟时间生成watermark s_1, 1590659331000, 10 s_1,
     * 1590659332000, 10 s_1, 1590659333000, 10 s_1, 1590659334000, 10
     */
    private static class PeriodicAssigner implements AssignerWithPeriodicWatermarks<Sensor> {

        private long bound = 1000 * 10; //延迟10s
        private long maxTs = Long.MIN_VALUE;
        private Watermark currentWatermark;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //log.error("currentWatermark maxTs:" + maxTs);
            currentWatermark = new Watermark(maxTs - bound);  //根据上一次的element.getTimestamp()生成watermark
            log.error("currentWatermark:" + currentWatermark.toString());
            return currentWatermark;
        }

        @Override
        public long extractTimestamp(Sensor element, long previousElementTimestamp) {
            getElementTimestamp(element, maxTs, currentWatermark);
            return element.getTimestamp();
        }
    }

    /**
     * 间断式地生成 watermark。 s_1, 1590659331000, 10 s_1, 1590659332000, 10 sensor_1, 1590659333000, 1
     * sensor_1, 1590659334000, 1 sensor_1, 1590659335000, 1 s_1, 1590659336000, 10 s_1,
     * 1590659337000, 10 s_1, 1590659338000, 10
     */
    private static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Sensor> {

        private long bound = 1000 * 10;
        private Watermark currentWatermark;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Sensor lastElement, long extractedTimestamp) {
            log.error("sensor id:" + lastElement.getId());
            if ("sensor_1".equals(lastElement.getId())) {
                currentWatermark = new Watermark(extractedTimestamp - bound);
                return currentWatermark;
            }
            return null;
        }

        @Override
        public long extractTimestamp(Sensor element, long previousElementTimestamp) {
            getElementTimestamp(element, previousElementTimestamp, currentWatermark);
            return element.getTimestamp();
        }
    }


    /**
     * 周期性生成watermark
     * 一种简单的特殊情况是，如果我们事先得知数据流的时间戳是单调递增的， 也就是说没有乱序，那我们可以使用AscendingTimestampExtractor，
     * 会直接使用数据的时间戳生成 watermark
     */
    public static class AscendingAssigner extends AscendingTimestampExtractor<Sensor> {

        @Override
        public long extractAscendingTimestamp(Sensor element) {
            getElementTimestamp(element, 0, super.getCurrentWatermark());
            return element.getTimestamp();
        }
    }

    /**
     * 周期性生成watermark
     * 对于乱序数据流，如果能大致估算出数据流中的事件的最大延迟时间，可以直接简单使用BoundedOutOfOrdernessTimestampExtractor
     * 会直接使用数据的时间戳生成 watermark, 而且可设置延迟时间
     * 最常用生成watermark Assigner.
     */
    public static class BoundedOutOfOrdernessAssigner extends BoundedOutOfOrdernessTimestampExtractor<Sensor> {

        public BoundedOutOfOrdernessAssigner(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Sensor element) {
            getElementTimestamp(element, 0, super.getCurrentWatermark());
            return element.getTimestamp();
        }
    }


    public static void getElementTimestamp(Sensor element, long maxTs, Watermark currentWatermark) {
        maxTs = Math.max(element.getTimestamp(), maxTs);
        log.error("current maxTs    :" + sdf.format(maxTs) + " [" + maxTs + "]");
        if (null != currentWatermark) {
            log.error("current watermark:" + sdf.format(currentWatermark.getTimestamp()) + " ["
                + currentWatermark.getTimestamp() + "]" + currentWatermark.toString());
        } else {
            log.error("current watermark is null");
        }
        log.error("element timestamp:" + sdf.format(element.getTimestamp()) + " [" + element
            .getTimestamp() + "]");
    }
}
