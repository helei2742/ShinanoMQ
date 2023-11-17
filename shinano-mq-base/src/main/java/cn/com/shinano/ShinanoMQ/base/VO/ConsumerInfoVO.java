package cn.com.shinano.ShinanoMQ.base.VO;

import cn.com.shinano.ShinanoMQ.base.dto.TopicQueueData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@Data
public class ConsumerInfoVO {
    private Map<String, TopicQueueData> consumerInfo;
    public ConsumerInfoVO(Map<String, TopicQueueData> consumerInfo) {
        this.consumerInfo = consumerInfo;
    }
}
