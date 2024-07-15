package service;

import constant.ApplicationConstant;
import model.check.MetricMessage;
import model.event.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlarmMatchFunc extends RichFlatMapFunction<MetricMessage, Event> {
    private static final Logger log = LoggerFactory.getLogger(AlarmMatchFunc.class);
    // 定义状态，保存上一次告警的时间
    private transient ValueState<Long> temperatureAlertTime;
    private transient ValueState<Long> humidityAlertTime;
    private transient ValueState<Long> pressureLevelAlertTime;
    private transient ValueState<Long> illuminationAlertTime;
    private transient ValueState<Long> latheTimeAlertTime;
    private transient ValueState<Long> millingAlertTime;
    private transient ValueState<Long> powerAlertTime;
    private transient ValueState<Long> transformerAlertTime;
    private transient ValueState<Long> upsAlertTime;
    private transient ValueState<Long> tank_water_levelAlertTime;
    private transient ValueState<Long> in_water_tempAlertTime;
    private transient ValueState<Long> out_water_tempAlertTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 温度
        ValueStateDescriptor<Long> temperatureAlertTimeDescriptor = new ValueStateDescriptor<>(
                "temperatureAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        temperatureAlertTime = getRuntimeContext().getState(temperatureAlertTimeDescriptor);
        // 湿度
        ValueStateDescriptor<Long> humidityAlertTimeDescriptor = new ValueStateDescriptor<>(
                "humidityAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        humidityAlertTime = getRuntimeContext().getState(humidityAlertTimeDescriptor);
        // 压力
        ValueStateDescriptor<Long> pressureLevelAlertTimeDescriptor = new ValueStateDescriptor<>(
                "pressureLevelAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        pressureLevelAlertTime = getRuntimeContext().getState(pressureLevelAlertTimeDescriptor);
        // 光照
        ValueStateDescriptor<Long> illuminationAlertTimeDescriptor = new ValueStateDescriptor<>(
                "illuminationAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        illuminationAlertTime = getRuntimeContext().getState(illuminationAlertTimeDescriptor);

        // 车床
        ValueStateDescriptor<Long> latheTimeAlertTimeDescriptor = new ValueStateDescriptor<>(
                "latheTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        latheTimeAlertTime = getRuntimeContext().getState(latheTimeAlertTimeDescriptor);
        // 铣床
        ValueStateDescriptor<Long> millingAlertTimeDescriptor = new ValueStateDescriptor<>(
                "millingAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        millingAlertTime = getRuntimeContext().getState(millingAlertTimeDescriptor);
        // 市电
        ValueStateDescriptor<Long> powerAlertTimeDescriptor = new ValueStateDescriptor<>(
                "powerAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        powerAlertTime = getRuntimeContext().getState(powerAlertTimeDescriptor);
        // 变压器
        ValueStateDescriptor<Long> transformerAlertTimeDescriptor = new ValueStateDescriptor<>(
                "transformerAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        transformerAlertTime = getRuntimeContext().getState(transformerAlertTimeDescriptor);
        // ups
        ValueStateDescriptor<Long> upsAlertTimeDescriptor = new ValueStateDescriptor<>(
                "upsAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        upsAlertTime = getRuntimeContext().getState(upsAlertTimeDescriptor);
        // tank_water_level
        ValueStateDescriptor<Long> tank_water_levelDescriptor = new ValueStateDescriptor<>(
                "tank_water_levelAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        tank_water_levelAlertTime = getRuntimeContext().getState(tank_water_levelDescriptor);
        // in_water_temp
        ValueStateDescriptor<Long> in_water_tempAlertTimeDescriptor = new ValueStateDescriptor<>(
                "in_water_tempAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        in_water_tempAlertTime = getRuntimeContext().getState(in_water_tempAlertTimeDescriptor);
        // out_water_temp
        ValueStateDescriptor<Long> out_water_tempAlertTimeDescriptor = new ValueStateDescriptor<>(
                "out_water_tempAlertTime", // 状态的名字
                Types.LONG); // 状态存储的数据类型
        out_water_tempAlertTime = getRuntimeContext().getState(out_water_tempAlertTimeDescriptor);
    }

    @Override
    public void flatMap(MetricMessage message, Collector<Event> out) throws Exception {
        // 获取数据时间
        long currentTime =  System.currentTimeMillis();

        // 获取上一次告警的时间
        ValueState<Long> alertTime = null;
        switch (message.getMetric()) {
            case ApplicationConstant.METRIC_TEMPERATURE:
                alertTime = temperatureAlertTime;
                break;
            case ApplicationConstant.METRIC_HUMIDITY:
                alertTime = humidityAlertTime;
                break;
            case ApplicationConstant.METRIC_ILLUMINATION:
                alertTime = illuminationAlertTime;
                break;
            case ApplicationConstant.METRIC_PRESSURE_LEVEL:
                alertTime = pressureLevelAlertTime;
                break;
            case ApplicationConstant.METRIC_ElEC_LATHE:
                alertTime = latheTimeAlertTime;
                break;
            case ApplicationConstant.METRIC_ElEC_MILLING:
                alertTime = millingAlertTime;
                break;
            case ApplicationConstant.METRIC_ElEC_POWER:
                alertTime = powerAlertTime;
                break;
            case ApplicationConstant.METRIC_ElEC_TRANSFORMER:
                alertTime = transformerAlertTime;
                break;
            case ApplicationConstant.METRIC_ElEC_UPS:
                alertTime = upsAlertTime;
                break;
            case ApplicationConstant.METRIC_TANK_WATER_LEVEl:
                alertTime = tank_water_levelAlertTime;
                break;
            case ApplicationConstant.METRIC_IN_WATER_TEMP:
                alertTime = in_water_tempAlertTime;
                break;
            case ApplicationConstant.METRIC_OUT_WATER_TEMP:
                alertTime = out_water_tempAlertTime;
                break;
        }

        Long prevAlertTime = 0L;
        try {
            prevAlertTime = alertTime.value();
        } catch (Exception e) {
            log.error("get prevAlertTime error: " + e);
            return;
        }
        if (prevAlertTime == null) {
            prevAlertTime = 0L;
        }

        // 检查温度是否低于20度，并且距离上次告警已经超过30分钟
        if (message.getValue() < message.getThresholdDown() && (currentTime - prevAlertTime) > ApplicationConstant.ALARM_INTERVAL) {
            Event event = Event.builder().metric(message.getMetric()).timestamp(currentTime).description("Metric " + message.getMetric() + " Warning:" + " value: " + message.getValue() + " < " + message.getThresholdDown() + "!").build();
            // 发出告警信息
            out.collect(event);
            // 更新上次告警时间
            alertTime.update(currentTime);
        } else if (message.getValue() > message.getThresholdUp() && (currentTime - prevAlertTime) > ApplicationConstant.ALARM_INTERVAL) {
            Event event = Event.builder().metric(message.getMetric()).timestamp(currentTime).description("Metric " + message.getMetric() + " Warning:" + " value: " + message.getValue() + " > " + message.getThresholdDown() + "!").build();
            // 发出告警信息
            out.collect(event);
            // 更新上次告警时间
            alertTime.update(currentTime);
        }
    }
}
