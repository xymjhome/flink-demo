package kafka.producer;


import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.partitioner.ProducerPartitioner;
import kafka.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class ProducerDemo {

    public static void main(String[] args) {
        Producer<Long, String> producer  = createProducer();
        log.error("test");

        for (int i = 0; i < Constant.MESSAGE_COUNT; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord(
                Constant.TOPIC_NAME, (long)i, "This is " + i + "  record");
            log.error("send data key:" + i);
            try {
                RecordMetadata metadata = producer.send(record)
                    .get(1000, TimeUnit.MILLISECONDS);

                log.error(String.format("record send with key:%s to patition:%s with offset:%s",
                    i, metadata.partition(), metadata.offset()));
            } catch (Exception e) {
                log.error("send message error:", e);
            }
        }

    }

    private static Producer<Long, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        //Id of the producer so that the broker can determine the source of the request.
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constant.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //设置分partition策略，不设置会按照DefaultPartitioner进行分区划分
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ProducerPartitioner.class);
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);
        return producer;
    }
}
