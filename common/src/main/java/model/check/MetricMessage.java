package model.check;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Slf4j
public class MetricMessage {
    private String metric;

    private Double value;

    private String timestamp;

    private Double thresholdUp;

    private Double thresholdDown;

    private String rawDataMessage;

    private Long timestampUnix;
}
