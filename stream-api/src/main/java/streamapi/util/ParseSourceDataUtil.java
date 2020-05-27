package streamapi.util;


import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streamapi.pojo.Sensor;
import streamapi.source.FileSource;

public class ParseSourceDataUtil {

    public static DataStreamSource<String> getFileSource(
        StreamExecutionEnvironment env, String fileName) {
        URL url = FileSource.class.getClassLoader().getResource(fileName);
        return env.readTextFile(url.getPath());
    }

    public static SingleOutputStreamOperator<Sensor> getSensorFileSource(
        StreamExecutionEnvironment env, String fileName) {
        DataStreamSource<String> fileSource = getFileSource(env, fileName);
        return fileSource.map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));
        });
    }

    public static SingleOutputStreamOperator<Sensor> getSensorSocketSource(
        StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 8080);
        return source.filter(line -> StringUtils.isNotBlank(line)).map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));
        });
    }

    public static SingleOutputStreamOperator<Tuple2<String, Long>> getTuple2SocketSource(
        StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 8080);
        return source.filter(line -> StringUtils.isNotBlank(line))
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    String[] data = value.split(",");
                    return new Tuple2<>(data[0].trim(), Long.parseLong(data[1].trim()));
                }
            });
    }
}
