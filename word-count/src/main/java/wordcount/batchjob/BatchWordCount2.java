package wordcount.batchjob;


import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import wordcount.util.WordData;

public class BatchWordCount2 {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = executionEnvironment.fromElements(WordData.wordData);

        DataSet<Tuple2<String, Integer>> counts = dataSource
            .flatMap(new LineSplitter()).groupBy(0).sum(1);

        counts.print();

        //executionEnvironment.execute("Word Count");
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

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
    }
}
