package streamapi.tablesql;


import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import streamapi.pojo.DataItem;
import streamapi.source.MyStreamingSource;

public class TableSqlSideOutputs {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        DataStream<DataItem> streamOperator = environment.addSource(new MyStreamingSource())
            .map(value -> value);

        /**
         * 报错org.apache.flink.api.common.functions.InvalidTypesException:
         * Could not determine TypeInformation for the OutputTag type.
         * The most common reason is forgetting to make the OutputTag an anonymous inner class.
         * It is also not possible to use generic type variables with OutputTags, such as 'Tuple2<A, B>'.
         */
        //OutputTag<DataItem> even = new OutputTag<>("even");//没有具体实例化，找不多对应的OutputTag的类型


        OutputTag<DataItem> even = new OutputTag<DataItem>("even") {};//{}是对泛型指定精确类型的实例化，相当于OutputTag的Superclass

        OutputTag<DataItem> odd = new OutputTag<>("odd", TypeInformation.of(DataItem.class));

        SingleOutputStreamOperator<DataItem> mainDataStream = streamOperator
            .process(new ProcessFunction<DataItem, DataItem>() {
                @Override
                public void processElement(DataItem value, Context ctx, Collector<DataItem> out)
                    throws Exception {
                    out.collect(value);

                    int id = value.getId();
                    if ((id & 1) == 0) {
                        ctx.output(even, value);
                    } else {
                        ctx.output(odd, value);
                    }
                }
            });

        DataStream<DataItem> evenSideOutput = mainDataStream.getSideOutput(even);
        DataStream<DataItem> oddSideOutput = mainDataStream.getSideOutput(odd);

        tableEnvironment.createTemporaryView("evenTable", evenSideOutput,"id, name");
        tableEnvironment.createTemporaryView("oddTable", oddSideOutput, "id, name");

        Table table = tableEnvironment
            .sqlQuery("select even.id, even.name, odd.name, odd.id from evenTable as even join "
                + "oddTable as odd on even.name = odd.name");

        table.printSchema();

        tableEnvironment.toRetractStream(table,
            TypeInformation.of(new TypeHint<Tuple4<Integer, String, String, Integer>>() {
        })).printToErr();

        environment.execute("side out put");
    }
}
