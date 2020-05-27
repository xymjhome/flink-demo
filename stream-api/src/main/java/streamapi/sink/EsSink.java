package streamapi.sink;


import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
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
import streamapi.util.JsonToMapUtil;
import streamapi.util.ParseSourceDataUtil;

@Slf4j
public class EsSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        SingleOutputStreamOperator<Sensor> sensor =
            ParseSourceDataUtil.getSensorFileSource(env, "sensor_data.txt");

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
