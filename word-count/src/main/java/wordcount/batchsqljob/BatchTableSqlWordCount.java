package wordcount.batchsqljob;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import wordcount.util.WordData;
import wordcount.util.WordWithCount;
import wordcount.util.WordWithFrequency;

public class BatchTableSqlWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);

        List<WordWithFrequency> wordCountList = Arrays.stream(WordData.wordData).flatMap(line -> {
            return Arrays.stream(line.toLowerCase().split("\\W+"))
                .filter(StringUtils::isNotBlank);
        }).map(word -> new WordWithFrequency(word, 1L)).collect(Collectors.toList());

        DataSource<WordWithFrequency> dataSource = environment
            .fromCollection(wordCountList);

        //转sql, 指定实体类字段名
        Table table = tableEnvironment.fromDataSet(dataSource, "word, frequency");
        table.printSchema();
        //注册为一个表
        tableEnvironment.createTemporaryView("WordCount", table);

        Table sqlQuery = tableEnvironment
            .sqlQuery("select word as word, sum(frequency) as frequency from WordCount group by word");
        sqlQuery.printSchema();

        //将查询后转换的表数据转换为DataSet
        DataSet<WordWithFrequency> wordWithCountDataSet = tableEnvironment
            .toDataSet(sqlQuery, WordWithFrequency.class);

        wordWithCountDataSet.printToErr();

    }
}
