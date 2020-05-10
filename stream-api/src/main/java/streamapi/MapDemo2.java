package streamapi;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.DataItem;
import streamapi.source.StreamingSource;

public class MapDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<DataItem> streamSource = environment
            .addSource(new StreamingSource());

        SingleOutputStreamOperator<String> map = streamSource
            .map(new MyMapFunction());
        map.printToErr().setParallelism(1);

        environment.execute("user defined streaming source");
    }

    public static class MyMapFunction extends RichMapFunction<DataItem, String> {


        @Override
        public String map(DataItem dataItem) throws Exception {
            return dataItem.getName();
        }
    }
}
