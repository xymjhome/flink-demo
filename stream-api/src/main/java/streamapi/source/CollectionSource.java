package streamapi.source;


import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.Sensor;

public class CollectionSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<Sensor> collectionSource = env.fromCollection(Lists.newArrayList(
            new Sensor("sensor_1", 1547718199, 35.80018327300259),
            new Sensor("sensor_6", 1547718201, 15.402984393403084),
            new Sensor("sensor_7", 1547718202, 6.720945201171228),
            new Sensor("sensor_10", 1547718205, 38.101067604893444)
        ));

        collectionSource.printToErr();

        env.execute("collection source");

    }
}
