package cn.com.shinano.ShinanoMQ.core.dto;

import cn.com.shinano.ShinanoMQ.base.dto.RetryMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lhe.shinano
 * @date 2023/11/22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerRetryMessage {
    private String clientId;
    private String topic;
    private String queue;
    private RetryMessage retryMessage;
}
