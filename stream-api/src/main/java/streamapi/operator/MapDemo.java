package streamapi.operator;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.DataItem;
import streamapi.source.MyStreamingSource;

public class MapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<DataItem> streamSource = environment
            .addSource(new MyStreamingSource());

        SingleOutputStreamOperator<String> map = streamSource
            .map(new MapFunction<DataItem, String>() {
                @Override
                public String map(DataItem dataItem) throws Exception {
                    return dataItem.getName();
                }
            });
        map.printToErr().setParallelism(1);

        environment.execute("user defined streaming source");
    }
}
