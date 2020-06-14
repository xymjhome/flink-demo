package kafka.producer;


import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import kafka.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class UserBehaviorProducer {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "user_behavior_client");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        URL resource = UserBehaviorProducer.class.getClassLoader().getResource("UserBehavior.csv");

        List<String> lines = FileUtils
            .readLines(new File(resource.getFile()), Charset.forName("utf-8"));

        lines.forEach(line -> {
            //System.out.println(line);
            ProducerRecord<String, String> record = new ProducerRecord<>("user_behavior",
                "behavior", line);

            RecordMetadata metadata = null;
            try {
                metadata = kafkaProducer.send(record).get();
                log.error(String.format("user behavior record send to patition:%s with offset:%s",
                    metadata.partition(), metadata.offset()));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

        });
    }
}
