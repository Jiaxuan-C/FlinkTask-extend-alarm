import cn.hutool.core.util.StrUtil;

import cn.hutool.setting.dialect.Props;

import connect.consumer.KafkaReadString;

import connect.producer.KafkaSink;
import constant.ApplicationConstant;
import constant.StaticConstant;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import model.check.MetricMessage;
import model.event.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import service.AlarmMatchFunc;
import service.StringToMessageMapFunction;


@Slf4j
@Data
public class AlarmProcess {
    public static void main(String[] args) throws Exception {
        new AlarmProcess(args, ApplicationConstant.APPLICATION_ALARM).run();
    }
    // 通用配置
    private Props commonProps;
    // 应用配置
    private Props applicationProps;
    // 运行环境标识
    private String mark;
    // 运行应用标识
    private String appName;

    public AlarmProcess(String[] args, String appName) {
        // 标识当前环境
        if (args == null || args.length == 0 || StrUtil.isBlank(args[0])) {
            mark = StaticConstant.ENVIRONMENT_MARK_LOCAL;
        } else {
            mark = args[0];
        }

        // 应用标识
        this.appName = appName;

        // 加载配置文件
        try {
            // 按照环境类型加载通用配置文件
            this.commonProps = new Props(ApplicationConstant.APPLICATION_COMMON + StaticConstant.PATH_CONNECTION + mark + StaticConstant.CONFIG_FILE_TYPE);

            // 按照启动参数读取应用配置文件
            this.applicationProps = new Props(appName + StaticConstant.PATH_CONNECTION + mark + StaticConstant.CONFIG_FILE_TYPE);
        } catch (Exception e) {
            log.error("Configure readProperties errors: " + e.getMessage());
            throw e;
        }
    }

    public void run() throws Exception {
        // 构造函数完成 对应的配置文件和运行环境的初始化

        // 设置flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // flink1.12之后StreamTimeCharacteristic默认是EventTime

        // 加载数据源
        DataStream<String> stringDataStream = KafkaReadString.readStringStream(env, this.applicationProps, this.mark).rebalance();

        // 初始处理,string 转 bean
        SingleOutputStreamOperator<MetricMessage> beanStream = stringDataStream.map(new StringToMessageMapFunction(mark));

        // 告警处理
        SingleOutputStreamOperator<Event> alarmStream = beanStream.keyBy((KeySelector<MetricMessage, Object>) MetricMessage::getMetric).flatMap(new AlarmMatchFunc());
        alarmStream.print("alarmStream--");

        // sink event
        KafkaSink.writeEventStream(applicationProps, alarmStream);

        env.execute("AlarmProcessAnalysis");
    }
}
