package connect.consumer;

import cn.hutool.setting.dialect.Props;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import constant.ApplicationConstant;
import constant.StaticConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

/**
 * 读取Kafka数据源
 */
@Slf4j
public class KafkaReadString {

    public static DataStream<String> readStringStream(StreamExecutionEnvironment env, Props props, String mark) throws Exception {
        // 入参判断
        if (props == null) {
            return null;
        }

        if (StringUtils.isEmpty(mark)) {
            mark = StaticConstant.ENVIRONMENT_MARK_LOCAL;
        }

        if (StaticConstant.ENVIRONMENT_MARK_LOCAL.equals(mark)) {
            // 从本地读取数据源
            return env.readTextFile("D:\\chenjiaxuan13\\Desktop\\gospace\\2024-5-30\\kafkaLocal\\服务器带内测试数据\\log\\cloudbee-log-1alarm.txt");
        } else {
            Properties properties = new Properties();

            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getStr(ApplicationConstant.CFG_KAFKA_CONSUMER_SERVER));
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, props.getStr(ApplicationConstant.CFG_KAFKA_CONSUMER_GROUP_ID));
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            // 初始化Kafka消费者
            // 创建 FlinkKafkaConsumer
            FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                    props.getStr(ApplicationConstant.CFG_KAFKA_CONSUMER_TOPIC), // Kafka 主题名称
                    new SimpleStringSchema(), // 序列化方式
                    properties);

            kafkaConsumer.setStartFromLatest();

            return env.addSource(kafkaConsumer).name("kafka source");
        }
    }
}
