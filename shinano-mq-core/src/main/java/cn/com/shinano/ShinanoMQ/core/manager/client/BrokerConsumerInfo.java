package cn.com.shinano.ShinanoMQ.core.manager.client;

import cn.com.shinano.ShinanoMQ.base.VO.ConsumerInfoVO;
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
            v.addQueueInfo(new TopicQueueData.QueueInfo(queue, 0L, 0));
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

    public boolean updateConsumeOffset(String clientId, String topic, String queue, Long newOffset) {
        TopicQueueData queueData = getQueueData(clientId, topic);
        if(queueData == null) return false;
        synchronized (queueData.getQueueInfoList()) {
            for (TopicQueueData.QueueInfo queueInfo : queueData.getQueueInfoList()) {
                if(queueInfo.getQueue().equals(queue)) {
                    queueInfo.setOffset(newOffset);
//                    queueInfo.addConsumeIndex();
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isConsumerBindQueue(String clientId, String topic, String queue) {
        TopicQueueData queueData = getQueueData(clientId, topic);
        if(queueData == null) return false;
        synchronized (queueData.getQueueInfoList()) {
            for (TopicQueueData.QueueInfo queueInfo : queueData.getQueueInfoList()) {
                if(queueInfo.getQueue().equals(queue)) {
                    return true;
                }
            }
        }
        return false;
    }
}
