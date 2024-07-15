package connect.producer;

import cn.hutool.json.JSONUtil;
import cn.hutool.setting.dialect.Props;
import constant.ApplicationConstant;

import lombok.extern.slf4j.Slf4j;
import model.event.Event;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


/**
 * 输出到Kafka
 */
@Slf4j
public class KafkaSink {

    // 事件写入Kafka
    public static void writeEventStream(Props props, SingleOutputStreamOperator<Event> eventStream) throws Exception {
        String bootstrapServers = props.getProperty(ApplicationConstant.CFG_KAFKA_EVENT_SERVER);
        String topic = props.getProperty(ApplicationConstant.CFG_KAFKA_EVENT_TOPIC);

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                bootstrapServers,          // Kafka broker list
                topic,                     // Target topic
                new SimpleStringSchema()        // Serialization schema
        );
        eventStream.map(msg -> JSONUtil.toJsonStr(msg)).addSink(kafkaProducer);
    }
}
