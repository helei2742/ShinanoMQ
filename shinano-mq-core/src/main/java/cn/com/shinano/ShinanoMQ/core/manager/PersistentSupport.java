package cn.com.shinano.ShinanoMQ.core.manager;

import cn.com.shinano.ShinanoMQ.base.dto.Message;
import cn.com.shinano.ShinanoMQ.core.dto.PutMessageResult;

import java.util.concurrent.CompletableFuture;

public interface PersistentSupport {
    /**
     * 持久化消息
     * @param id 由服务器生成的消息id
     * @param topic 消息的topic
     * @param queue 消息的queue
     */
    void persistentMessage(String id, String topic, String queue);


    /**
     * 异步保存消息
     * @param message
     * @return
     */
    CompletableFuture<PutMessageResult> asyncPutMessage(Message message);

    /**
     * 同步保存消息
     * @param message
     * @return
     */
    PutMessageResult syncPutMessage(Message message);


    boolean updateConsumeTimes(String topic, String queue, Long offset, Integer length, Integer retryTimes);
}
