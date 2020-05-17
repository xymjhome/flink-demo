package streamapi.operator;

import java.net.URL;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import streamapi.pojo.Sensor;

public class UnionCoMapCoFlatMapDemo {

    public static void main(String[] args) throws Exception {
        URL url = UnionCoMapCoFlatMapDemo.class.getClassLoader().getResource("sensor_data.txt");
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        OutputTag<Sensor> hightTag = new OutputTag<Sensor>("hight") {
        };
        OutputTag<Sensor> lowTag = new OutputTag<Sensor>("low") {
        };
        SingleOutputStreamOperator<Sensor> all = env.readTextFile(url.getPath()).map(line -> {
            String[] values = line.split(",");
            return Sensor.builder().id(values[0])
                .timestamp(Long.parseLong(values[1].trim()))
                .temperature(Double.parseDouble(values[2].trim()))
                .build();
        }).process(new ProcessFunction<Sensor, Sensor>() {
            @Override
            public void processElement(Sensor value, Context ctx, Collector<Sensor> out)
                throws Exception {
                out.collect(value);

                if (value.getTemperature() > 30) {
                    ctx.output(hightTag, value);
                } else {
                    ctx.output(lowTag, value);
                }
            }
        });

        DataStream<Sensor> hightDataStream = all.getSideOutput(hightTag).map(sensor ->
            Sensor.builder().id(sensor.getId() + "-hight")
                .temperature(sensor.getTemperature())
                .timestamp(sensor.getTimestamp()).build());
        DataStream<Sensor> lowDataStream = all.getSideOutput(lowTag).map(sensor ->
            Sensor.builder().id(sensor.getId() + "-low")
                .temperature(sensor.getTemperature())
                .timestamp(sensor.getTimestamp()).build());


        hightDataStream.printToErr("hight").setParallelism(1);
        lowDataStream.printToErr("low").setParallelism(1);
        all.printToErr("all").setParallelism(1);

        /**
         * union算子：对两个或者两个以上的 DataStream 进行union操作，产生一个包含所有 DataStream 元素的新 DataStream
         *
         *
         * Connect 与 Union 区别:
         * 1. Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap 中再去调整成为一样的。
         * 2. Connect 只能操作两个流，Union 可以操作多个。
         */

        SingleOutputStreamOperator<Sensor> operator = hightDataStream.union(all).union(lowDataStream)
            .map(item -> item);

        operator.printToErr("all + hight + low stream").setParallelism(1);

        env.execute("union job");
    }
}
