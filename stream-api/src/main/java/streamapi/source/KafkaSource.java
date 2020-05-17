package streamapi.source;


import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import kafka.utils.Constant;
import org.apache.kafka.common.serialization.StringDeserializer;
import streamapi.pojo.Sensor;

public class KafkaSource {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constant.GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constant.OFFSET_RESET_LATEST);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            Constant.OFFSET_RESET_EARLIER);//从最开始offset读取数据，没生效

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            Constant.SENSOR_TOPIC, new SimpleStringSchema(),
            properties);

        /**
         * Flink从topic中最初的数据开始消费
        */
        //consumer.setStartFromEarliest();

        /**
         * Flink从topic中指定的offset开始，这个比较复杂，需要手动指定offset
         *
         * Map<KafkaTopicPartition, Long> Long参数指定的offset位置
         * KafkaTopicPartition构造函数有两个参数，第一个为topic名字，第二个为分区数
         * 获取offset信息，可以用过Kafka自带的kafka-consumer-groups.sh脚本获取
         */
        Map<KafkaTopicPartition, Long> offsets = new HashedMap();
        offsets.put(new KafkaTopicPartition(Constant.SENSOR_TOPIC, 0), 14L);
        //consumer.setStartFromSpecificOffsets(offsets);

        /**
         * Flink从topic中指定的时间点开始消费，指定时间点之前的数据忽略
         */
        //consumer.setStartFromTimestamp(1589632256000L);


        /**
         * Flink从topic中最新的数据开始消费
         *
         * 消费者启动之后，生产者产生数据才会正常接收并消费
         */
        //consumer.setStartFromLatest();

        /**
         * Flink从topic中指定的group上次消费的位置开始消费，所以必须配置group.id参数
         */
        consumer.setStartFromGroupOffsets();


        DataStreamSource<String> kafkaSource = env.addSource(consumer);

        kafkaSource.printToErr();

        SingleOutputStreamOperator<Sensor> sensor = kafkaSource.map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));
        });
        sensor.printToErr();

        env.execute("kafka source");
    }
}
