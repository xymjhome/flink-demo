package com.doris.sink;

import com.doris.pojo.UserBehavior;
import java.nio.charset.Charset;
import java.util.Properties;
import javax.annotation.Nullable;
import kafka.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ExtractTransformLoadUserBehaviorData {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "user_behavior_group0");
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "user_behavior_client0");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("user_behavior",
            new SimpleStringSchema(), consumerProperties);
        consumer.setStartFromLatest();

        SingleOutputStreamOperator<UserBehavior> source = env.addSource(consumer)
            .map(new MapFunction<String, UserBehavior>() {
                @Override
                public UserBehavior map(String value) throws Exception {
                    //log.error(value);
                    String[] datas = value.split(",");
                    if (datas.length != 5) {
                        return null;
                    }
                    UserBehavior userBehavior = UserBehavior.builder()
                        .userId(Long.parseLong(datas[0].trim()))
                        .itemId(Long.parseLong(datas[1].trim()))
                        .categoryId(Integer.parseInt(datas[2].trim()))
                        .behavior(datas[3].trim())
                        .timestamp(Long.parseLong(datas[4].trim())).build();
                    String behavior = userBehavior.getBehavior();
                    switch (behavior) {
                        case "pv":
                            userBehavior.setPvFlag(1);
                            break;
                        case "buy":
                            userBehavior.setBuyFlag(1);
                            break;
                        case "cart":
                            userBehavior.setCartFlag(1);
                            break;
                        case "fav":
                            userBehavior.setFavFlag(1);
                            break;
                        default:
                            break;
                    }
                    return userBehavior;
                }
            }).filter(item -> null != item);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "p_user_behavior_client0");
        source.addSink(
            new FlinkKafkaProducer<UserBehavior>("etl_user_behavior",
                new KafkaSerializationSchema<UserBehavior>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(UserBehavior element,
                        @Nullable Long timestamp) {
                        log.error(element.toString());
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            "etl_user_behavior",
                            element.toString().getBytes(Charset.forName("utf-8")));
                        return record;
                    }
                }, producerProperties, Semantic.EXACTLY_ONCE));


        env.execute("transform user behavior data job");

    }
}
