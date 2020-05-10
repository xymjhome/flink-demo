package wordcount.streamingjob;


import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import wordcount.util.WordWithCount;

public class StreamingWordCount2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment
            .socketTextStream("127.0.0.1", 8080, "\n");

        DataStream<WordWithCount> count = streamSource
            .flatMap(new FlatMapFunction<String, WordWithCount>() {
                @Override
                public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
                    String[] words = line.split("\\s");
                    Arrays.stream(words)
                        .filter(StringUtils::isNotBlank)
                        .forEach(word -> {
                            collector.collect(new WordWithCount(word, 1L));
                        });
                }
            })
            .keyBy("word")
            //.timeWindow(Time.seconds(2), Time.seconds(1)) //不加时间窗口的限制，会记录整个流过程中的数量
            .reduce(new ReduceFunction<WordWithCount>() {
                @Override
                public WordWithCount reduce(WordWithCount workWithCount, WordWithCount t1)
                    throws Exception {
                    return new WordWithCount(workWithCount.word, workWithCount.count + t1.count);
                }
            });


        //count.print().setParallelism(1);
        count.print();

        environment.execute("Socket Word Count");

    }
}
