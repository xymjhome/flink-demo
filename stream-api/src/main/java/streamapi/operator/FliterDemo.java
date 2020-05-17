package streamapi.operator;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.DataItem;
import streamapi.source.MyStreamingSource;

public class FliterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<DataItem> streamSource = environment
            .addSource(new MyStreamingSource());

        SingleOutputStreamOperator<DataItem> filter = streamSource
            .filter(new FilterFunction<DataItem>() {
                @Override
                public boolean filter(DataItem dataItem) throws Exception {
                    return (dataItem.getId() & 1) == 0;
                }
            });

        filter.printToErr().setParallelism(1);

        environment.execute("filter demo");
    }
}
