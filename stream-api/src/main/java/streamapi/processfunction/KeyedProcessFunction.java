package streamapi.processfunction;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import streamapi.pojo.Sensor;
import streamapi.util.ParseSourceDataUtil;

public class KeyedProcessFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);

        SingleOutputStreamOperator<Sensor> source = ParseSourceDataUtil
            .getSensorSocketSource(env);

        SingleOutputStreamOperator<Sensor> watermarks = source
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Sensor>(
                Time.milliseconds(10)) {
                @Override
                public long extractTimestamp(Sensor element) {
                    return element.getTimestamp();
                }
            });

        SingleOutputStreamOperator<String> process = watermarks.keyBy("id")
            .process(new TempIncreaseAlertFunction());

        process.printToErr();
        env.execute("keyed process function");
    }

    private static class TempIncreaseAlertFunction extends
        org.apache.flink.streaming.api.functions.KeyedProcessFunction<Tuple, Sensor, String> {

        //只能在open方法内才能获取到runtimeContext,直接在类初始化成员变量的位置获取，会报错：The runtime context has not been initialized.
        private ValueState<Double> lastTemperature;
        //= getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemperature", Types.DOUBLE));

        private ValueState<Long> currentTimer;
        //getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Types.LONG));

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemperature = getRuntimeContext()
                .getState(new ValueStateDescriptor<Double>("lastTemperature", Types.DOUBLE));

            currentTimer = getRuntimeContext()
                .getState(new ValueStateDescriptor<Long>("currentTimer", Types.LONG));
        }


        @Override
        public void processElement(Sensor value, Context ctx, Collector<String> out)
            throws Exception {
            Double preTemperature = lastTemperature.value();
            double currentTemperature = value.getTemperature();
            lastTemperature.update(currentTemperature);

            if (preTemperature == null) {
                return;
            }


            Long currentTimerTimestamp = currentTimer.value();

            if (currentTemperature <= preTemperature && currentTimerTimestamp != null) {
                ctx.timerService().deleteEventTimeTimer(currentTimerTimestamp);
                currentTimer.clear();
            } else if (currentTemperature > preTemperature && currentTimerTimestamp == null) {
                //Registers a timer to be fired when the event time watermark passes the given time.
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 1000);//触发此注册timer的是watermark时间戳
                currentTimer.update(value.getTimestamp() + 1000);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
            out.collect("id:" + ctx.getCurrentKey() + "-->" + lastTemperature.value()
                + " Rose for 1s in a row！！！");

            currentTimer.clear();
        }
    }
}
