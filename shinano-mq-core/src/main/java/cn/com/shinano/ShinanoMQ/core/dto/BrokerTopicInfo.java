package cn.com.shinano.ShinanoMQ.core.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * 存储broker的topic信息
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Data
public class BrokerTopicInfo {

    private ConcurrentMap<String, TopicInfo> activeTopicsMap = new ConcurrentHashMap<>();


    public boolean isTopicExist(String topic) {
        return activeTopicsMap.containsKey(topic);
    }

    public boolean isTopicExist(String topic, String queue) {
        if(!activeTopicsMap.containsKey(topic)) return false;
        return activeTopicsMap.get(topic).getQueueInfo().containsKey(queue);
    }

    public boolean createTopic(String topic) {
        return activeTopicsMap.putIfAbsent(topic, new TopicInfo(topic, new ConcurrentHashMap<>())) == null;
    }

    public boolean addQueue(String topic, List<String> queues) {
        TopicInfo topicInfo = activeTopicsMap.get(topic);

        ConcurrentMap<String, Long> queueInfo = topicInfo.getQueueInfo();

        boolean f = true;
        for (String queue : queues) {
            f &= queueInfo.putIfAbsent(queue, 0L) == null;
        }
        return f;
    }

    public List<String> getTopicList() {
        return new ArrayList<>(activeTopicsMap.keySet());
    }

    public Map<String, Object> getTopicInfo(String topic) {
        Map<String, Object> map = new HashMap<>();
        map.put("info", activeTopicsMap.get(topic));
        return map;
    }

    /**
     * 更新时，不能保证顺序，所以有可能会产生offset错误的情况
     * @param topic
     * @param queue
     * @param offset
     */
    public void updateOffset(String topic, String queue, long offset) {
        TopicInfo topicInfo = activeTopicsMap.get(topic);
        if(topicInfo != null) {
            topicInfo.setOffset(queue, offset);
        }
    }

    public Long queryTopicQueueOffset(String topic, String queue) {
        TopicInfo topicInfo = activeTopicsMap.get(topic);
        if(topicInfo == null) return -1L;
        return topicInfo.getOffset(queue);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopicInfo {
        private String topic;
        private ConcurrentMap<String, Long> queueInfo;
        public Long getOffset(String queue) {
            return queueInfo.getOrDefault(queue, -1L);
        }
        public void setOffset(String queue, long offset) {
            queueInfo.computeIfPresent(queue, (k,v)->{
                return Math.max(v, offset);
            });
        }
    }
}
