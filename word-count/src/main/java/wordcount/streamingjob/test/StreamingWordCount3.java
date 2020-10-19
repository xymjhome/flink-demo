package wordcount.streamingjob.test;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import wordcount.util.WordWithCount;

public class StreamingWordCount3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> streamSource = environment
            .socketTextStream("127.0.0.1", 8080, "\n");

        DataStream<WordWithCount> count = streamSource
            .flatMap(new RichFlatMapFunction<String, WordWithCount>() {

                @Override
                public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                    String[] words = value.split("\\s");
                    Arrays.stream(words)
                        .filter(StringUtils::isNotBlank)
                        .forEach(word -> {
                            out.collect(new WordWithCount(word, 1L));
                        });
                }

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    System.out.println("test");
                }
            })
            .keyBy("word").window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .apply(new WindowFunction<WordWithCount, WordWithCount, Tuple, TimeWindow>() {

                @Override
                public void apply(Tuple tuple, TimeWindow window, Iterable<WordWithCount> input,
                    Collector<WordWithCount> out) throws Exception {
                    Iterator<WordWithCount> iterator = input.iterator();
                    while (iterator.hasNext()) {
                        out.collect(iterator.next());
                    }
                    System.out.println("apply function run");
                }
            })
            .map(new MapFunction<WordWithCount, WordWithCount>() {
                @Override
                public WordWithCount map(WordWithCount value) throws Exception {
                    return value;
                }
            });
//            .reduce(new ReduceFunction<WordWithCount>() {
//                @Override
//                public WordWithCount reduce(WordWithCount workWithCount, WordWithCount t1)
//                    throws Exception {
//                    return new WordWithCount(workWithCount.word, workWithCount.count + t1.count);
//                }
//            });


        //count.print().setParallelism(1);
        count.print();

        environment.execute("Socket Word Count");

    }
}
