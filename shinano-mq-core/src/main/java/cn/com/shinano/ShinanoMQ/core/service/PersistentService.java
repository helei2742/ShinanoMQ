package cn.com.shinano.ShinanoMQ.core.service;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.service.impl.MappedChannelPersistentService;

public interface PersistentService {
    /**
     * 持久化消息
     * @param id 由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    void persistentMessage(String id, String topic, String queue);

    /**
     * 通过topic与queue组成的key查找执行持久化的任务
     * @param topic 消息的topic
     * @param queue 消息的queue
     * @return
     */
    MappedChannelPersistentService.PersistentTask getPersistentTask(String topic, String queue);

    void saveMessageImmediately(Message message);
}
