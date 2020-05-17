package kafka.producer;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SensorProducerDemo {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        //Id of the producer so that the broker can determine the source of the request.
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constant.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        Producer<String, String> producer= new KafkaProducer<>(properties);

        URL url = SensorProducerDemo.class.getClassLoader().getResource("sensor_data.txt");
        List<String> lines = FileUtils.readLines(new File(url.getPath()), Charset.defaultCharset());

        lines.forEach(line -> {
            ProducerRecord<String, String> record = new ProducerRecord<>(Constant.SENSOR_TOPIC,
                "sensor", line);
            try {
                RecordMetadata metadata = producer.send(record).get();
                log.error(String.format("record send to patition:%s with offset:%s",
                    metadata.partition(), metadata.offset()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });

    }

}
