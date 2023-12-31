package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.base.constans.ShinanoMQConstants;
import cn.com.shinano.ShinanoMQ.core.dto.OffsetAndCount;
import cn.com.shinano.ShinanoMQ.core.utils.BrokerUtil;
import cn.com.shinano.ShinanoMQ.core.utils.StoreFileUtil;
import lombok.Data;

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

    private ConcurrentMap<String, TopicInfo> closedTopicsMap = new ConcurrentHashMap<>();

    public boolean isTopicExist(String topic) {
        return activeTopicsMap.containsKey(topic);
    }

    public boolean isTopicExist(String topic, String queue) {
        if(!activeTopicsMap.containsKey(topic)) return false;
        return activeTopicsMap.get(topic).getQueueInfo().containsKey(queue);
    }

    public boolean createTopic(String topic) {
        return activeTopicsMap.putIfAbsent(topic, new TopicInfo(topic)) == null;
    }

    public boolean addQueue(String topic, List<String> queues) {
        TopicInfo topicInfo = activeTopicsMap.get(topic);

        Map<String, OffsetAndCount> queueInfo = topicInfo.getQueueInfo();

        boolean f = false;
        for (String queue : queues) {
            if(queueInfo.putIfAbsent(queue, new OffsetAndCount(0L,0)) != null) {
                queueInfo.putIfAbsent(ShinanoMQConstants.RETRY_QUEUE_NAME_PREFIX + queue, new OffsetAndCount(0L, 0));
                queueInfo.putIfAbsent(ShinanoMQConstants.DLQ_BANE_PREFIX + queue, new OffsetAndCount(0L, 0));
                f = true;
            }
        }
        return f;
    }

    public List<String> getTopicList() {
        return new ArrayList<>(activeTopicsMap.keySet());
    }

    public TopicInfo getTopicInfo(String topic) {

        return  activeTopicsMap.get(topic);
    }

    public void updateOffset(String topic, String queue, long offset) {
        TopicInfo topicInfo = activeTopicsMap.get(topic);
        if(topicInfo != null) {
            //FIXME 更新时，不能保证顺序，所以有可能会产生offset错误的情况
            topicInfo.setOffset(queue, offset);
        }
    }

    public Long queryTopicQueueOffset(String topic, String queue) {
        TopicInfo topicInfo = activeTopicsMap.get(topic);
        if(topicInfo == null) return -1L;
        return topicInfo.getOffset(queue);
    }


    public boolean closeTopic(String topic) {
        if(!activeTopicsMap.containsKey(topic) || closedTopicsMap.containsKey(topic)) return false;

        synchronized (activeTopicsMap.get(topic)) {
            if(!activeTopicsMap.containsKey(topic) || closedTopicsMap.containsKey(topic)) return false;

            TopicInfo topicInfo = activeTopicsMap.get(topic);
            closedTopicsMap.put(topic, topicInfo);
            activeTopicsMap.remove(topic);
            return true;
        }
    }

    public List<String>  deleteQueues(String topic, List<String> queues) {
        if(!activeTopicsMap.containsKey(topic)) return null;

        return activeTopicsMap.get(topic).removeQueues(queues);
    }

    public boolean deleteTopic(String topic) {
        if(activeTopicsMap.containsKey(topic) && !closedTopicsMap.containsKey(topic)) return false;

        synchronized (closedTopicsMap.get(topic)) {
            TopicInfo remove = closedTopicsMap.remove(topic);
            if(remove != null) {
                StoreFileUtil.moveTopicData(topic);
            }
            return true;
        }
    }

    public boolean recoverTopic(String topic) {
        if(activeTopicsMap.containsKey(topic) ||!closedTopicsMap.containsKey(topic)) return false;

        synchronized (closedTopicsMap.get(topic)) {
            if(activeTopicsMap.containsKey(topic) ||!closedTopicsMap.containsKey(topic)) return false;

            TopicInfo topicInfo = closedTopicsMap.remove(topic);
            activeTopicsMap.put(topic, topicInfo);
            closedTopicsMap.remove(topic);
            return true;
        }
    }
}
