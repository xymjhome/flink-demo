package streamapi.source;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.DataItem;

public class StreamSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<DataItem> streamSource = environment
            .addSource(new MyStreamingSource());

        SingleOutputStreamOperator<DataItem> map = streamSource
            .map((MapFunction<DataItem, DataItem>) value -> value);
        //SingleOutputStreamOperator<DataItem> map = streamSource.map(value -> value);
        map.printToErr().setParallelism(1);

        environment.execute("user defined streaming source");
    }
}
