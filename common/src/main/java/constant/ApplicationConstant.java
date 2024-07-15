package constant;

import java.util.HashMap;
import java.util.Map;

/**
 * 应用类型常量
 */
public class ApplicationConstant {
    // 应用类型
    public static final String APPLICATION_COMMON = "common";
    public static final String APPLICATION_ALARM = "alarm";

    public static final String  METRIC_TEMPERATURE = "temperature";
    public static final String  METRIC_HUMIDITY = "humidity";
    public static final String  METRIC_PRESSURE_LEVEL = "pressure_level";
    public static final String  METRIC_ILLUMINATION = "illumination";

    public static final String  METRIC_ElEC_LATHE = "lathe";
    public static final String  METRIC_ElEC_MILLING = "milling";
    public static final String  METRIC_ElEC_POWER = "power";
    public static final String  METRIC_ElEC_TRANSFORMER = "transformer";
    public static final String  METRIC_ElEC_UPS = "ups";
    public static final String  METRIC_TANK_WATER_LEVEl = "tank_water_level";
    public static final String  METRIC_IN_WATER_TEMP = "in_water_temp";
    public static final String  METRIC_OUT_WATER_TEMP = "out_water_temp";

    // Kafka Topic标记
    public static final String KAFKA_LOGS_TOPIC = "kafka_logs_topic";
    public static final String KAFKA_ALARM_TOPIC = "kafka_alarm_topic";

    public static final String CFG_KAFKA_CONSUMER_TOPIC = "kafka.consumer.topic";
    public static final String CFG_KAFKA_CONSUMER_SERVER = "kafka.consumer.server";
    public static final String CFG_KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.groupId";

    public static final long ALARM_INTERVAL = 60 * 1000;
    public static final String CFG_KAFKA_EVENT_TOPIC = "event.analysis.kafka.alarm.topic";
    public static final String CFG_KAFKA_EVENT_SERVER = "event.analysis.kafka.alarm.server";
}
