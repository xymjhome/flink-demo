package streamapi.util;


import java.util.Properties;
import kafka.utils.Constant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaPropertiesUtil {
    public static Properties getKafkaProperties(String brokers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constant.GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constant.OFFSET_RESET_LATEST);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            Constant.OFFSET_RESET_EARLIER);//从最开始offset读取数据，此配置设置到flink中的kafka consumer不生效，需根据具体函数设置{@link KafkaSource}

        return properties;
    }
}
