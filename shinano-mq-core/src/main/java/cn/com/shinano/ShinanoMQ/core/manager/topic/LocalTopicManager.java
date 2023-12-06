package cn.com.shinano.ShinanoMQ.core.manager.topic;

import cn.com.shinano.ShinanoMQ.core.manager.TopicManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lhe.shinano
 * @date 2023/11/8
 */
@Slf4j
@Component
public class LocalTopicManager implements TopicManager {

    @Autowired
    private BrokerTopicInfo brokerTopicInfo;

    @Override
    public boolean isTopicExist(String topic) {
        return brokerTopicInfo.isTopicExist(topic);
    }

    @Override
    public boolean isTopicExist(String topic, String queue) {
        return brokerTopicInfo.isTopicExist(topic, queue);
    }

    @Override
    public boolean createTopic(String topic, List<String> queues) {
        if(!isTopicExist(topic) && !brokerTopicInfo.createTopic(topic)) { //topic不存在,并且创建topic失败
           return false;
        }
        if(queues == null) {
            queues = new ArrayList<>();
        }
        if(queues.size() == 0) {
            queues.add("default");
        }

        return brokerTopicInfo.addQueue(topic, queues);
    }

    @Override
    public List<String> getTopicList() {
        return brokerTopicInfo.getTopicList();
    }

    @Override
    public TopicInfo getTopicInfo(String topic) {
        return brokerTopicInfo.getTopicInfo(topic);
    }

    @Override
    public boolean closeTopic(String topic) {
        return brokerTopicInfo.closeTopic(topic);
    }

    @Override
    public List<String> deleteQueues(String topic, List<String> queues) {

        return brokerTopicInfo.deleteQueues(topic, queues);
    }

    @Override
    public boolean deleteTopic(String topic) {
        return brokerTopicInfo.deleteTopic(topic);
    }

    @Override
    public boolean recoverTopic(String topic) {
        return brokerTopicInfo.recoverTopic(topic);
    }

    @Override
    public Long getOffset(String topic, String queue) {
        TopicInfo topicInfo = brokerTopicInfo.getTopicInfo(topic);
        if(topicInfo == null) return null;
        return topicInfo.getOffset(queue);
    }

    @Override
    public void updateCount(String topic, String queue, int count) {
        TopicInfo topicInfo = brokerTopicInfo.getTopicInfo(topic);
        if(topicInfo == null) return;
        topicInfo.getQueueInfo().get(queue).setCount(count);
    }
}
