package streamapi.sink;


import kafka.utils.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig.Builder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import streamapi.pojo.Sensor;
import streamapi.util.KafkaPropertiesUtil;

public class RedisSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            Constant.SENSOR_TOPIC, new SimpleStringSchema(),
            KafkaPropertiesUtil.getKafkaProperties(Constant.KAFKA_BROKERS));
        consumer.setStartFromEarliest();

        SingleOutputStreamOperator<Sensor> source = env.addSource(consumer).map(line -> {
            String[] split = line.split(",");
            return new Sensor(split[0],
                Long.parseLong(split[1].trim()),
                Double.parseDouble(split[2].trim()));
        });

        FlinkJedisPoolConfig config = new Builder().setHost("localhost").setPort(6379).build();

        source.addSink(
            new org.apache.flink.streaming.connectors.redis.RedisSink<>(config, new MyRedisMapper()));


        env.execute("redis sink");

    }

    public static class MyRedisMapper implements RedisMapper<Sensor> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor");
        }

        @Override
        public String getKeyFromData(Sensor o) {
            return o.getId();
        }

        @Override
        public String getValueFromData(Sensor o) {
            return o.toString();
        }
    }
}
