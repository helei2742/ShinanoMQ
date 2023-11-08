package cn.com.shinano.ShinanoMQ.core.service.impl;

import cn.com.shinano.ShinanoMQ.core.dto.BrokerTopicInfo;
import cn.com.shinano.ShinanoMQ.core.service.TopicManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    public Map<String, Object> getTopicInfo(String topic) {
        return brokerTopicInfo.getTopicInfo(topic);
    }
}
