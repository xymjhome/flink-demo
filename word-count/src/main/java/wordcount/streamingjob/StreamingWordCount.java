package wordcount.streamingjob;


import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment
            .socketTextStream("127.0.0.1", 8080, "\n");

        SingleOutputStreamOperator<Tuple2<String, Integer>> count = streamSource
            .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String line, Collector<Tuple2<String, Integer>> collector)
                    throws Exception {
                    String[] words = line.toLowerCase().split("\\W+");
                    Arrays.stream(words)
                        .filter(StringUtils::isNotBlank)
                        .forEach(word -> {
                            collector.collect(new Tuple2<String, Integer>(word, 1));
                        });
                }
            }).keyBy(0).timeWindow(Time.seconds(1))
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2,
                    Tuple2<String, Integer> t1) throws Exception {
                    return new Tuple2<String, Integer>(t1.f0, (t1.f1 + t2.f1));
                }
            });

        //count.print().setParallelism(1);
        count.print();

        environment.execute("Socket Word Count");

    }
}
