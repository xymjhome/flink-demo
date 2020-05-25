package streamapi.util;


import java.net.URL;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.Sensor;
import streamapi.source.FileSource;

public class ParseFileDataUtil {

    public static DataStreamSource<String> getFileSourcer(
        StreamExecutionEnvironment env, String fileName) {
        URL url = FileSource.class.getClassLoader().getResource(fileName);
        return env.readTextFile(url.getPath());
    }

    public static SingleOutputStreamOperator<Sensor> getSensorFileSourcer(
        StreamExecutionEnvironment env, String fileName) {
        DataStreamSource<String> fileSource = getFileSourcer(env, fileName);
        return fileSource.map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));
        });
    }

}
