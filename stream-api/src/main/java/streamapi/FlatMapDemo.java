package streamapi;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import streamapi.pojo.DataItem;
import streamapi.source.StreamingSource;

public class FlatMapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<DataItem> streamSource = environment
            .addSource(new StreamingSource());

        SingleOutputStreamOperator<String> map = streamSource
            .flatMap(new FlatMapFunction<DataItem, String>() {
                @Override
                public void flatMap(DataItem dataItem, Collector<String> collector)
                    throws Exception {
                    collector.collect(dataItem.getName());
                }
            });
        map.printToErr().setParallelism(1);

        environment.execute("user defined streaming source");
    }
}
