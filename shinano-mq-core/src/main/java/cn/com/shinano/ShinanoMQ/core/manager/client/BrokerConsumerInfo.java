package cn.com.shinano.ShinanoMQ.core.manager.client;

import cn.com.shinano.ShinanoMQ.base.VO.ConsumerInfoVO;
import cn.com.shinano.ShinanoMQ.base.dto.Pair;
import cn.com.shinano.ShinanoMQ.base.dto.TopicQueueData;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author lhe.shinano
 * @date 2023/11/17
 */
@Data
public class BrokerConsumerInfo {
    /**
     * Map<clientId, Map<Topic, TopicQueueData(queue, offset)>
     */
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

    public TopicQueueData getQueueData(String clientId, String topic) {
        if(!consumerInfoMap.containsKey(clientId)
                || !consumerInfoMap.get(clientId).containsKey(topic)) {
            return null;
        }
        return consumerInfoMap.get(clientId).get(topic);
    }

    public boolean updateConsumeOffset(String clientId, String topic, String queue, Long minACK) {
        TopicQueueData queueData = getQueueData(clientId, topic);
        if(queueData == null) return false;
        synchronized (queueData.getQueueInfoList()) {
            for (Pair<String, Long> queue_Offset : queueData.getQueueInfoList()) {
                if(queue_Offset.getKey().equals(queue)) {
                    queue_Offset.setValue(minACK);
                    return true;
                }
            }
        }
        return false;
    }
}
