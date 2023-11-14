package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.dto.BrokerResult;

import java.util.concurrent.CompletableFuture;

public interface PersistentSupport {
    /**
     * 持久化消息
     * @param id 由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    void persistentMessage(String id, String topic, String queue);


    CompletableFuture<BrokerResult> saveMessageImmediately(Message message);
}
