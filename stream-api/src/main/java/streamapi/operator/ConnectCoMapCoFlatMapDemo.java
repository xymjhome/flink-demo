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

public class ConnectCoMapCoFlatMapDemo {

    public static void main(String[] args) throws Exception {
        URL url = ConnectCoMapCoFlatMapDemo.class.getClassLoader().getResource("sensor_data.txt");
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

        DataStream<Sensor> hightDataStream = all.getSideOutput(hightTag);
        DataStream<Sensor> lowDataStream = all.getSideOutput(lowTag);


        SingleOutputStreamOperator<Tuple2<String, Double>> hightOperate = hightDataStream
            .map(new MapFunction<Sensor, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(Sensor value) throws Exception {
                    return new Tuple2(value.getId(), value.getTemperature());
                }
            });



        /**
         * connect算子：连接两个保持他们类型的数据流，两个数据流被 Connect 之后，
         *             只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化两个流相互独立
         *
         * Connect 与 Union 区别:
         * 1. Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap 中再去调整成为一样的。
         * 2. Connect 只能操作两个流，Union 可以操作多个。
         */
        ConnectedStreams<Tuple2<String, Double>, Sensor> connectedStreams = hightOperate.connect(lowDataStream);

        SingleOutputStreamOperator<Tuple3<String, Double, String>> connectCoMap = connectedStreams
            .map(new CoMapFunction<Tuple2<String, Double>, Sensor,
                Tuple3<String, Double, String>>() {
                @Override
                public Tuple3 map1(Tuple2 value) throws Exception {
                    return new Tuple3<String, Double, String>((String) (value.f0),
                        (double) (value.f1), "warning");
                }

                @Override
                public Tuple3 map2(Sensor value) throws Exception {
                    return new Tuple3<String, Double, String>(value.getId(), value.getTemperature(),
                        "healthy");
                }
            });

        connectCoMap.printToErr("coMap");

        SingleOutputStreamOperator<Tuple2<String, String>> connectCoFlatMap = connectedStreams
            .flatMap(
                new CoFlatMapFunction<Tuple2<String, Double>, Sensor, Tuple2<String, String>>() {
                    @Override
                    public void flatMap1(Tuple2<String, Double> value,
                        Collector<Tuple2<String, String>> out) throws Exception {
                        Tuple2<String, String> hight = new Tuple2<>(value.f0, "hight");
                        out.collect(hight);
                    }

                    @Override
                    public void flatMap2(Sensor value, Collector<Tuple2<String, String>> out)
                        throws Exception {
                        Tuple2<String, String> low = new Tuple2<>(value.getId(), "low");
                        out.collect(low);

                    }
                });

        connectCoFlatMap.printToErr("coFlatMap");

        env.execute("connect job");
    }
}
