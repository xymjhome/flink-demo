package streamapi.sink;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink.Builder;
import org.apache.http.HttpHost;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import streamapi.pojo.Sensor;
import streamapi.source.FileSource;
import streamapi.util.JsonToMapUtil;

@Slf4j
public class EsSink2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        URL url = FileSource.class.getClassLoader().getResource("time_log.txt");
        DataStreamSource<String> fileSource = env.readTextFile(url.getPath());
        SingleOutputStreamOperator<Map<String, Object>> source = fileSource
            .map(new MapFunction<String, Map<String, Object>>() {
                @Override
                public Map<String, Object> map(String value) throws Exception {
                    return parseLineData(value);
                }
            });

        source.printToErr();


        ElasticsearchSink<Map<String, Object>> elasticsearchSink = new Builder<>(
            Lists.newArrayList(new HttpHost("localhost", 9200)),
            new ElasticsearchSinkFunction<Map<String, Object>>() {
                @Override
                public void process(Map<String, Object> element, RuntimeContext ctx,
                    RequestIndexer indexer) {
                    IndexRequest indexRequest = Requests.indexRequest("timelog-2020-05-23")
                        .source(element);
                    indexer.add(indexRequest);
                }
            }).build();
        source.addSink(elasticsearchSink);

        env.execute("es sink2");
    }

    private static Map<String, Object> parseLineData(String line) {
        if (StringUtils.isBlank(line)) {
            return Maps.newHashMap();
        }
        String[] split = line.split("  - ");
        if (split.length != 2) {
            return Maps.newHashMap();
        }
        String[] preDataArray = split[0].trim().split("\\s+");
        String debugLevel = preDataArray[0];
        //"2020-05-18T13:01:43.791Z"
        String timestamp = preDataArray[1] + "T"
            + preDataArray[2].replace(",", ".") + "Z";
        String clazzName = preDataArray[4];

        HashMap<String, Object> result = Maps.newHashMap();
        result.put("@timestamp", timestamp);
        result.put("debugLevel", debugLevel);
        result.put("className", clazzName);
        Map<String, Object> dataMap = JsonToMapUtil.parseJsonToMap(split[1]);
        if (MapUtils.isNotEmpty(dataMap)) {
            result.putAll(dataMap);
        }

        String url = (String)dataMap.get("url");
        String[] split333 = url.split("\\?");
        String[] split1 = split333[1].split("&");
        Map<String, String> urlMap = Arrays.stream(split1)
            .filter(data -> data.split("=").length > 0)
            .collect(Collectors.toMap(
                data -> "url_" + data.split("=")[0],
                data -> data.split("=").length > 1 ? data.split("=")[1] : " ",
                (oldValue, newValue) -> newValue));
        result.putAll(urlMap);
        return result;
    }

}
