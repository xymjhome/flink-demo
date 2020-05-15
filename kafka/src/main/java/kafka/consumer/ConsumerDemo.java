package kafka.consumer;


import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import kafka.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerDemo {


    public static void main(String[] args) {

        Consumer<Long, String> consumer = createConsumer();
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1000));
            log.error("count:" + records.count());
            if (records.count() == 0) {
                log.error("miss record");
            }
            records.forEach(record -> {
                log.error(String.format("record key:%s value:%s patition:%s offset:%s",
                    record.key(), record.value(), record.partition(), record.offset()));
            });
        }

    }

    public static Consumer<Long, String> createConsumer() {
        Properties properties = new Properties();
        //The Kafka broker's address.
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        //The consumer group id used to identify to which group this consumer belongs.
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constant.GROUP_ID_CONFIG);
        //The max count of records that the consumer will fetch in one iteration.
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Constant.MAX_POLL_RECORDS);
        //he class name to deserialize the key object.
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        //The class name to deserialize the value object.
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //When the consumer from a group receives a message it must commit the offset of that record. If this configuration is set to be true then,
        //periodically, offsets will be committed, but, for the production level, this should be false and an offset should be committed manually
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //For each consumer group, the last committed offset value is stored
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constant.OFFSET_RESET_EARLIER);

        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties);

        //设置消费指定topic下的指定partition
        consumer.assign(Lists.newArrayList(new TopicPartition(Constant.TOPIC_NAME, 1)));

        //consumer.subscribe(Lists.newArrayList(Constant.TOPIC_NAME));

        return consumer;
    }

}
