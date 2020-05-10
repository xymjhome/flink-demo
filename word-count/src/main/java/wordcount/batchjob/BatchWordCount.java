package wordcount.batchjob;


import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import wordcount.util.WordData;

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = executionEnvironment.fromElements(WordData.wordData);

        //The generic type parameters of 'Collector' are missing. In many cases lambda methods don't provide enough information
        // for automatic type extraction when Java generics are involved.
        // An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface.
        // Otherwise the type has to be specified explicitly using type information.
        //at org.apache.flink.api.java.typeutils.TypeExtractionUtils.validateLambdaType(TypeExtractionUtils.java:350)

        //使用lambda methods导致flink内部不能正确识别返回类型，需使用匿名内部类，或者使用实现接口FlatMapFunction的具体实现类
//        AggregateOperator<Tuple2<String, Integer>> counts = dataSource
//            .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
//                String[] words = line.toLowerCase().split("\\W+");
//
////                Arrays.stream(words)
////                    .filter(StringUtils::isNotBlank)
////                    .forEach(word -> {
////                        collector.collect(new Tuple2<String, Integer>(word, 1));
////                    });
//
//                for (int i = 0; i < words.length; i++) {
//                    if (StringUtils.isNotBlank(words[i])) {
//                        collector.collect(new Tuple2<String, Integer>(words[i], 1));
//                    }
//                }
//            }).groupBy(0).sum(1);
        AggregateOperator<Tuple2<String, Integer>> counts = dataSource
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
            }).groupBy(0).sum(1);

        counts.print();

        //executionEnvironment.execute("Word Count");
    }
}
