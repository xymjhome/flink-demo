package streamapi.window.count;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.Sensor;
import streamapi.util.ParseSourceDataUtil;

public class CountSlidingWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        SingleOutputStreamOperator<Sensor> source = ParseSourceDataUtil
            .getSensorSocketSource(env);

        SingleOutputStreamOperator<Tuple2<String, Double>> sum = source
            .map(new MapFunction<Sensor, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(Sensor value) throws Exception {
                    return new Tuple2<>(value.getId(), value.getTemperature());
                }
            }).keyBy(0).countWindow(5, 2).sum(1);

        sum.printToErr();
        env.execute("count sliding window");
    }
}
