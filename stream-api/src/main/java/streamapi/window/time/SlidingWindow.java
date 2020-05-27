package streamapi.window.time;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import streamapi.pojo.Sensor;
import streamapi.util.ParseSourceDataUtil;

public class SlidingWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        SingleOutputStreamOperator<Sensor> socketSource = ParseSourceDataUtil
            .getSensorSocketSource(env);

        SingleOutputStreamOperator<Tuple3<String, Double, Long>> minTempPerWindow = socketSource
            .map(new MapFunction<Sensor, Tuple3<String, Double, Long>>() {
                @Override
                public Tuple3<String, Double, Long> map(Sensor value) throws Exception {
                    return new Tuple3<>(value.getId(), value.getTemperature(), System.currentTimeMillis());
                }
            }).keyBy(0)
            .timeWindow(Time.seconds(15), Time.seconds(5))
            .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                @Override
                public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1,
                    Tuple3<String, Double, Long> value2) throws Exception {
                    Tuple3<String, Double, Long> min =
                        value1.f1 > value2.f1 ? value2 : value1;
                    return  new Tuple3<>(min.f0, min.f1, min.f2);
                }
            });

        minTempPerWindow.printToErr();
        env.execute("sliding window");
    }
}
