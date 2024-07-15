package service;

import cn.hutool.json.JSONUtil;
import constant.StaticConstant;
import lombok.extern.slf4j.Slf4j;
import model.check.MetricMessage;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
public class StringToMessageMapFunction implements MapFunction<String, MetricMessage> {

    private final String mark;

    public StringToMessageMapFunction(String mark) {
        this.mark = mark;
    }

    @Override
    public MetricMessage map(String s) {
        try {
            // 从dp拉取的文件需要额外处理一下
            if (this.mark.equals(StaticConstant.ENVIRONMENT_MARK_LOCAL)) {
                s = s.substring(s.indexOf("{"));
            }
            MetricMessage t = JSONUtil.toBean(s, MetricMessage.class);
            t.setRawDataMessage(s);
            return t;
        } catch (Exception e) {
            log.error("StringToMessageMapFunction class: " + MetricMessage.class, e);
        }
        return null;
    }
}
