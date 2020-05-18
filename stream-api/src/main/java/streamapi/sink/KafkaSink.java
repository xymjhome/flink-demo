package streamapi.sink;


import java.nio.charset.Charset;
import java.util.Properties;
import javax.annotation.Nullable;
import kafka.utils.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import streamapi.pojo.DataItem;
import streamapi.source.MyStreamingSource;

public class KafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<DataItem> streamSource = environment
            .addSource(new MyStreamingSource());

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        //Id of the producer so that the broker can determine the source of the request.
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constant.CLIENT_ID);

        streamSource.map(item -> item.toString()).addSink(
            //new FlinkKafkaProducer<String>(Constant.TOPIC_NAME, new SimpleStringSchema(), properties)

            //也是String为结果存储到kafka中，需看下ProducerRecord<byte[], byte[]>最后怎么转换存储为String
            new FlinkKafkaProducer<String>(Constant.TOPIC_NAME,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element,
                        @Nullable Long timestamp) {
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            Constant.TOPIC_NAME,
                            element.getBytes(Charset.forName("UTF-8")));
                        return record;
                    }
                },
                properties, Semantic.AT_LEAST_ONCE)
        );

        environment.execute("kafka sink");

    }
}
