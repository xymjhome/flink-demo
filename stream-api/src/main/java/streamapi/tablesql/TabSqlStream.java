package streamapi.tablesql;


import com.google.common.collect.Lists;
import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import streamapi.pojo.DataItem;
import streamapi.source.MyStreamingSource;

@Slf4j
public class TabSqlStream {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner()
            .inStreamingMode().build();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment
            .create(environment, environmentSettings);

        SingleOutputStreamOperator<DataItem> source = environment.addSource(new MyStreamingSource())
            .map(value -> value);

        SplitStream<DataItem> split = source.split(new OutputSelector<DataItem>() {
            @Override
            public Iterable<String> select(DataItem value) {
                ArrayList<String> result = Lists.newArrayList();
                int id = value.getId();
                if ((id & 1) == 0) {
                    result.add("even");
                } else {
                    result.add("odd");
                }
                log.error("result size:" + result.size());
                return result;
            }
        });

        DataStream<DataItem> even = split.select("even");
        DataStream<DataItem> odd = split.select("odd");

        tableEnvironment.createTemporaryView("evenTable", even, "name, id");
        tableEnvironment.createTemporaryView("oddTable", odd, "name, id");

        Table table = tableEnvironment.sqlQuery("select even.name,even.id, odd.name,odd.id "
            + "from evenTable as even join oddTable as odd on even.name = odd.name");

        table.printSchema();

        tableEnvironment.toRetractStream(table, TypeInformation.of(
            new TypeHint<Tuple4<String, Integer, String, Integer>>(){}
        )).printToErr();

        environment.execute("sql data stream");
    }
}
