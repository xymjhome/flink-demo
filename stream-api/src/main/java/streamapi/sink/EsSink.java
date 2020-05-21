package streamapi.sink;


import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.net.URL;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink.Builder;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import streamapi.pojo.Sensor;
import streamapi.source.FileSource;
import streamapi.util.JsonToMapUtil;

@Slf4j
public class EsSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        URL url = FileSource.class.getClassLoader().getResource("sensor_data.txt");
        DataStreamSource<String> fileSource = env.readTextFile(url.getPath());
        SingleOutputStreamOperator<Sensor> sensor = fileSource.map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));
        });

        sensor.printToErr();

        ElasticsearchSink<Sensor> sink = new Builder<>(
            Lists.newArrayList(new HttpHost("localhost", 9200)),
            new ElasticsearchSinkFunction<Sensor>() {
                @Override
                public void process(Sensor sensor, RuntimeContext runtimeContext,
                    RequestIndexer requestIndexer) {
                    String data = JsonToMapUtil.GSON.toJson(sensor);
                    log.error("save data:" + data);


                    IndexRequest indexRequest = Requests.indexRequest().index("sensor")
                        .source(JsonToMapUtil.parseJsonToMap(data));
                    requestIndexer.add(indexRequest);

                    log.error("sace data sucess");
                }
            }).build();

        sensor.addSink(sink);

        env.execute("es sink");
    }

}
