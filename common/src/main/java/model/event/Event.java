package model.event;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import java.io.Serializable;

/**
 * 事件最终输出的结构，用于将不同的Message统一成一个Model
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Slf4j
@ToString
@Builder
public class Event implements Serializable {
    private String metric;

    private String description;

    private Long timestamp;
}
