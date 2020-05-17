package streamapi.source;

import java.net.URL;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.Sensor;

public class FileSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        URL url = FileSource.class.getClassLoader().getResource("sensor_data.txt");
        DataStreamSource<String> fileSource = env.readTextFile(url.getPath());
        SingleOutputStreamOperator<Sensor> map = fileSource.map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));

        });

        map.printToErr();

        env.execute("file source");

    }
}
