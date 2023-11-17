package cn.com.shinano.ShinanoMQ.core.manager.client;

import cn.com.shinano.ShinanoMQ.base.VO.ConsumerInfoVO;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueueData;
import javafx.util.Pair;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@Data
public class BrokerConsumerInfo {
    private final ConcurrentMap<String, ConcurrentMap<String, TopicQueueData>> consumerInfoMap = new ConcurrentHashMap<>();


    public void addConsumerInfo(String topic, String queue, String clientId) {
        consumerInfoMap.putIfAbsent(clientId, new ConcurrentHashMap<>());
        ConcurrentMap<String, TopicQueueData> topicMap = consumerInfoMap.get(clientId);
        topicMap.compute(topic, (k, v)->{
            if(v == null) {
                v = new TopicQueueData();
            }
            v.addQueueInfo(new Pair<>(queue, 0L));
            return v;
        });
    }

    public ConsumerInfoVO getConsumerInfo(String clientId) {
        return new ConsumerInfoVO(consumerInfoMap.get(clientId));
    }
}
